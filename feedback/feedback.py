"""
Pathway-driven online trainer (corrected).
- Reads from NATS JSON stream on subject 'transactions'
- OnlineTrainer instance maintains model state
- Periodic checkpointing in background thread
- Single-process (no distributed replication)
"""

import pathway as pw
import torch
import torch.nn as nn
import torch.optim as optim
import time
import threading
import json
from collections import deque
from datetime import datetime
import argparse
import os

# -----------------------
# Utilities
# -----------------------
def hash_bucket(s, buckets=512):
    return (hash(str(s)) & 0xffffffff) % buckets


# -----------------------
# Model components
# -----------------------
class MessageEncoder(nn.Module):
    def __init__(self, msg_dim=32, buckets=512, emb_dim=8):
        super().__init__()
        self.emb = nn.Embedding(buckets, emb_dim)
        self.fc = nn.Sequential(
            nn.Linear(1 + emb_dim * 3, msg_dim),
            nn.ReLU(),
            nn.Linear(msg_dim, msg_dim)
        )

    def forward(self, amt_1x1, c1, c2, c3):
        e1 = self.emb(c1)
        e2 = self.emb(c2)
        e3 = self.emb(c3)
        x = torch.cat([amt_1x1, e1, e2, e3], dim=1)
        return self.fc(x)


class MemoryModule:
    def __init__(self, mem_dim=64, msg_dim=32):
        self.mem_dim = mem_dim
        self.msg_dim = msg_dim
        self.gru = nn.GRUCell(msg_dim, mem_dim)

    def init_mem(self, n):
        return torch.zeros(n, self.mem_dim)

    def update_inference(self, memory, idx, message):
        with torch.no_grad():
            prev = memory[idx:idx+1]
            new = self.gru(message.detach(), prev)
            memory[idx:idx+1] = new


class EdgeClassifier(nn.Module):
    def __init__(self, mem_dim=64):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(mem_dim*2 + 1, 128),
            nn.ReLU(),
            nn.Linear(128, 1)
        )

    def forward(self, hu, hv, amt_raw):
        return self.net(torch.cat([hu, hv, amt_raw], dim=1))


# -----------------------
# NodeMapper
# -----------------------
class NodeMapper:
    def __init__(self):
        self.id2idx = {}
        self.next = 0

    def get(self, x):
        x = str(x)
        if x not in self.id2idx:
            self.id2idx[x] = self.next
            self.next += 1
        return self.id2idx[x]

    def state(self):
        return self.id2idx.copy()

    def load_state(self, s):
        self.id2idx = s.copy() if s else {}
        self.next = max(s.values()) + 1 if len(s) else 0


# -----------------------
# OnlineTrainer (single-process)
# -----------------------
class OnlineTrainer:
    def __init__(self, initial_nodes=1000, mem_dim=64, msg_dim=32, buckets=512, checkpoint_path="checkpoint.pth"):
        self.mapper = NodeMapper()
        self.mem_dim = mem_dim
        self.msg_dim = msg_dim
        self.buckets = buckets

        self.memory_module = MemoryModule(self.mem_dim, self.msg_dim)
        self.memory_tensor = self.memory_module.init_mem(initial_nodes)

        self.encoder = MessageEncoder(self.msg_dim, buckets)
        self.clf = EdgeClassifier(self.mem_dim)

        self.opt = optim.Adam(
            list(self.encoder.parameters()) + list(self.clf.parameters()),
            lr=1e-3
        )
        self.loss_fn = nn.BCEWithLogitsLoss()

        self.replay = deque(maxlen=2000)
        self.steps = 0
        self.checkpoint_path = checkpoint_path
        self._lock = threading.Lock()

    def _ensure(self, idx):
        if idx >= self.memory_tensor.size(0):
            old = self.memory_tensor.size(0)
            new_sz = max(idx + 1, old * 2)
            print(f"[TRAIN] Growing memory {old} -> {new_sz}")
            new = torch.zeros(new_sz, self.mem_dim)
            new[:old] = self.memory_tensor
            self.memory_tensor = new

    def process(self, src_raw, dst_raw, amount, payment_type, sender_bank_location, receiver_bank_location, label, train=True):
        """Process a single transaction. Returns tuple of (prob, pred, label)."""
        try:
            src_raw = str(src_raw or "unknown")
            dst_raw = str(dst_raw or "unknown")
            amount = float(amount or 0.0)
        except Exception:
            return 0.0, 0, -1

        src = self.mapper.get(src_raw)
        dst = self.mapper.get(dst_raw)
        self._ensure(max(src, dst))

        # Features
        amt_norm = torch.tensor([[amount / 10000.0]], dtype=torch.float32)
        amt_raw = torch.tensor([[amount]], dtype=torch.float32)

        c1 = torch.tensor([hash_bucket(payment_type or "")], dtype=torch.long)
        c2 = torch.tensor([hash_bucket(sender_bank_location or "")], dtype=torch.long)
        c3 = torch.tensor([hash_bucket(receiver_bank_location or "")], dtype=torch.long)

        # Encode message
        msg_vec = self.encoder(amt_norm, c1, c2, c3)

        # Update inference memory (no-grad)
        self.memory_module.update_inference(self.memory_tensor, src, msg_vec)
        self.memory_module.update_inference(self.memory_tensor, dst, msg_vec)

        # Get detached memories
        hu = self.memory_tensor[src:src+1].detach()
        hv = self.memory_tensor[dst:dst+1].detach()

        logit = self.clf(hu, hv, amt_raw)
        prob = torch.sigmoid(logit.detach()).item()

        # Extract label if present
        label_int = None
        if label is not None:
            try:
                label_int = int(label)
            except:
                label_int = None

        # Training step if labeled
        if train and (label_int is not None):
            self.replay.append((hu.clone(), hv.clone(), amt_raw.clone(), float(label_int)))
            self._train_step()

        return prob, 1 if prob >= 0.5 else 0, label_int if label_int is not None else -1

    def _train_step(self):
        if not self.replay:
            return
        hu, hv, amt_raw, lbl = self.replay[-1]
        lbl_t = torch.tensor([[lbl]], dtype=torch.float32)

        self.opt.zero_grad()
        logits = self.clf(hu, hv, amt_raw)
        loss = self.loss_fn(logits, lbl_t)
        loss.backward()
        self.opt.step()

        self.steps += 1
        if self.steps % 50 == 0:
            print(f"[TRAIN] step={self.steps} loss={loss.item():.4f} nodes={self.mapper.next} mem={self.memory_tensor.size(0)}")

    def save_checkpoint(self, path=None):
        if path is None:
            path = self.checkpoint_path
        with self._lock:
            ck = {
                "mapper": self.mapper.state(),
                "memory": self.memory_tensor.clone(),
                "encoder": self.encoder.state_dict(),
                "clf": self.clf.state_dict(),
                "mem_dim": self.mem_dim,
                "msg_dim": self.msg_dim
            }
            torch.save(ck, path)
            print(f"[CHECKPOINT] saved -> {path}")

    def load_checkpoint(self, path):
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        ck = torch.load(path, map_location="cpu")
        self.mapper.load_state(ck.get("mapper", {}))
        self.memory_tensor = ck["memory"].clone()
        self.encoder.load_state_dict(ck["encoder"])
        self.clf.load_state_dict(ck["clf"])
        print(f"[CHECKPOINT] loaded <- {path}")


# -----------------------
# Periodic checkpoint saver
# -----------------------
def start_periodic_checkpoint(trainer: OnlineTrainer, path: str, interval: int = 30):
    def saver():
        while True:
            time.sleep(interval)
            try:
                trainer.save_checkpoint(path)
            except Exception as e:
                print(f"[CHECKPOINT] save failed: {e}")
    t = threading.Thread(target=saver, daemon=True)
    t.start()
    return t


# -----------------------
# Pathway pipeline
# -----------------------
def run_pipeline(nats_url: str, subject: str, checkpoint_path: str, checkpoint_period: int = 30):
    # Define schema for incoming NATS JSON messages
    class TransactionSchema(pw.Schema):
        src: str
        dst: str
        amount: float
        payment_type: str
        sender_bank_location: str
        receiver_bank_location: str
        label: int

    trainer = OnlineTrainer(
        initial_nodes=1000,
        mem_dim=64,
        msg_dim=32,
        buckets=512,
        checkpoint_path=checkpoint_path
    )

    try:
        trainer.load_checkpoint(checkpoint_path)
    except FileNotFoundError:
        print("[TRAIN] No checkpoint found; starting fresh.")
    except Exception as e:
        print(f"[TRAIN] Checkpoint load error: {e}")

    start_periodic_checkpoint(trainer, checkpoint_path, interval=checkpoint_period)

    # Pathway source - NATS with schema
    src = pw.io.nats.read(
        uri=nats_url,
        subject=subject,
        topic="transactions",
        format="json",
        schema=TransactionSchema,
    )

    # Define UDFs for each output column
    def _get_prob(src_val, dst_val, amount_val, payment_type_val, 
                  sender_bank_location_val, receiver_bank_location_val, label_val):
        prob, _, _ = trainer.process(
            src_val, dst_val, amount_val, payment_type_val,
            sender_bank_location_val, receiver_bank_location_val,
            label_val, train=True
        )
        return prob

    def _get_pred(src_val, dst_val, amount_val, payment_type_val,
                  sender_bank_location_val, receiver_bank_location_val, label_val):
        _, pred, _ = trainer.process(
            src_val, dst_val, amount_val, payment_type_val,
            sender_bank_location_val, receiver_bank_location_val,
            label_val, train=True
        )
        return pred

    def _get_label(src_val, dst_val, amount_val, payment_type_val,
                   sender_bank_location_val, receiver_bank_location_val, label_val):
        _, _, lbl = trainer.process(
            src_val, dst_val, amount_val, payment_type_val,
            sender_bank_location_val, receiver_bank_location_val,
            label_val, train=True
        )
        return lbl

    # Apply UDFs individually for each column
    results = src.select(
        src=pw.this.src,
        dst=pw.this.dst,
        amount=pw.this.amount,
        prob=pw.udf(_get_prob, return_type=float)(
            pw.this.src,
            pw.this.dst,
            pw.this.amount,
            pw.this.payment_type,
            pw.this.sender_bank_location,
            pw.this.receiver_bank_location,
            pw.this.label
        ),
        pred=pw.udf(_get_pred, return_type=int)(
            pw.this.src,
            pw.this.dst,
            pw.this.amount,
            pw.this.payment_type,
            pw.this.sender_bank_location,
            pw.this.receiver_bank_location,
            pw.this.label
        ),
        label=pw.udf(_get_label, return_type=int)(
            pw.this.src,
            pw.this.dst,
            pw.this.amount,
            pw.this.payment_type,
            pw.this.sender_bank_location,
            pw.this.receiver_bank_location,
            pw.this.label
        )
    )

    # pw.io.stdout(results)
    pw.io.null.write(results)
    pw.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pathway Online Trainer")
    parser.add_argument("--nats", default="nats://127.0.0.1:4222")
    parser.add_argument("--subject", default="transactions")
    parser.add_argument("--checkpoint", default="checkpoint.pth")
    parser.add_argument("--checkpoint_period", type=int, default=30)
    args = parser.parse_args()

    print("Starting Pathway trainer")
    print(f"  NATS: {args.nats}")
    print(f"  Subject: {args.subject}")
    print(f"  Checkpoint: {args.checkpoint}")
    print(f"  Period: {args.checkpoint_period}s")
    run_pipeline(args.nats, args.subject, args.checkpoint, args.checkpoint_period)