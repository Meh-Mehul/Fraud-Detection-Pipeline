# detector_train.py (FIXED - Memory Optimized)

import argparse, asyncio, json, time
from datetime import datetime
from collections import deque
import torch
import torch.nn as nn
import torch.optim as optim
from nats.aio.client import Client as NATS


# ------------------------------------------
# Utils
# ------------------------------------------

def parse_ts(ts):
    formats = [
        "%d-%m-%Y %H:%M:%S",  # DD-MM-YYYY
        "%Y-%m-%d %H:%M:%S",  # YYYY-MM-DD
    ]
    for fmt in formats:
        try:
            return datetime.strptime(ts, fmt).timestamp()
        except:
            continue
    return time.time()

def hash_bucket(s, buckets=512):
    return (hash(str(s)) & 0xffffffff) % buckets


# ------------------------------------------
# Node mapper
# ------------------------------------------

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
        return self.id2idx

    def load_state(self, s):
        self.id2idx = s
        self.next = max(s.values()) + 1 if len(s) else 0


# ------------------------------------------
# Message Encoder (categorical + amount)
# ------------------------------------------

class MessageEncoder(nn.Module):
    def __init__(self, msg_dim=32, buckets=512, emb_dim=8):
        super().__init__()
        self.emb = nn.Embedding(buckets, emb_dim)
        self.fc = nn.Sequential(
            nn.Linear(1 + emb_dim * 3, msg_dim),
            nn.ReLU(),
            nn.Linear(msg_dim, msg_dim),
        )

    def forward(self, amt_1x1, c1, c2, c3):
        e1 = self.emb(c1)
        e2 = self.emb(c2)
        e3 = self.emb(c3)
        x = torch.cat([amt_1x1, e1, e2, e3], dim=1)
        return self.fc(x)


# ------------------------------------------
# Memory Module (NO GRAD THROUGH MEMORY)
# ------------------------------------------

class MemoryModule:
    def __init__(self, mem_dim=64, msg_dim=32):
        self.mem_dim = mem_dim
        self.msg_dim = msg_dim
        self.gru = nn.GRUCell(msg_dim, mem_dim)

    def init_mem(self, n):
        return torch.zeros(n, self.mem_dim)

    def update(self, memory, idx, msg):
        """Pure inference memory: NO GRAD."""
        with torch.no_grad():
            prev = memory[idx:idx+1]
            new = self.gru(msg.detach(), prev)
            memory[idx:idx+1] = new


# ------------------------------------------
# Edge Classifier
# ------------------------------------------

class EdgeClassifier(nn.Module):
    def __init__(self, mem_dim=64):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(mem_dim * 2 + 1, 128),
            nn.ReLU(),
            nn.Linear(128, 1)
        )

    def forward(self, hu, hv, amt_raw):
        return self.net(torch.cat([hu, hv, amt_raw], dim=1))


# ------------------------------------------
# Online Trainer (MEMORY OPTIMIZED)
# ------------------------------------------

class OnlineTrainer:
    def __init__(self, initial_nodes=1000, mem_dim=64, msg_dim=32):
        self.mapper = NodeMapper()
        self.mem_dim = mem_dim
        self.msg_dim = msg_dim
        self.buckets = 512

        self.memory = MemoryModule(self.mem_dim, self.msg_dim)
        self.memory_tensor = self.memory.init_mem(initial_nodes)  # Start small

        self.encoder = MessageEncoder(self.msg_dim, self.buckets)
        self.clf = EdgeClassifier(self.mem_dim)

        self.opt = optim.Adam(
            list(self.encoder.parameters()) +
            list(self.clf.parameters()),
            lr=1e-3
        )

        self.loss_fn = nn.BCEWithLogitsLoss()
        self.replay = deque(maxlen=1000)  # Reduced from 3000
        self.steps = 0

    def _ensure(self, idx):
        """Grow memory tensor if needed"""
        if idx >= self.memory_tensor.size(0):
            old_size = self.memory_tensor.size(0)
            new_sz = min(idx + 1000, idx * 2)  # Grow more conservatively
            
            print(f"[MEMORY] Growing from {old_size} to {new_sz} nodes")
            
            new = torch.zeros(new_sz, self.mem_dim)
            new[:old_size] = self.memory_tensor
            self.memory_tensor = new

    # ----------------------------------------------------------
    # PROCESS EVENT
    # ----------------------------------------------------------

    def process(self, m, train=True):
        # Handle missing fields gracefully
        src = self.mapper.get(m.get("src", "unknown"))
        dst = self.mapper.get(m.get("dst", "unknown"))
        amt = float(m.get("amount", 0))

        self._ensure(max(src, dst))

        amt_norm = torch.tensor([[amt/10000]], dtype=torch.float32)
        amt_raw = torch.tensor([[amt]], dtype=torch.float32)

        # Handle missing categorical fields
        c1 = torch.tensor([hash_bucket(m.get("payment_type", ""), self.buckets)], dtype=torch.long)
        c2 = torch.tensor([hash_bucket(m.get("sender_bank_location", ""), self.buckets)], dtype=torch.long)
        c3 = torch.tensor([hash_bucket(m.get("receiver_bank_location", ""), self.buckets)], dtype=torch.long)

        msg_vec = self.encoder(amt_norm, c1, c2, c3)

        # 1. Update inference memory only (NO GRAD)
        self.memory.update(self.memory_tensor, src, msg_vec)
        self.memory.update(self.memory_tensor, dst, msg_vec)

        # 2. Compute prediction using current memory (detached)
        hu = self.memory_tensor[src:src+1].detach()
        hv = self.memory_tensor[dst:dst+1].detach()

        logit = self.clf(hu, hv, amt_raw)
        prob = torch.sigmoid(logit).item()

        # 3. If training, push example to replay
        label = m.get("label")
        if train and label is not None:
            self.replay.append((hu.clone(), hv.clone(), amt_raw.clone(), float(label)))
            self.train_step()

        return prob

    # ----------------------------------------------------------
    # FIXED TRAINING STEP (NO MEMORY GRAD)
    # ----------------------------------------------------------

    def train_step(self):
        if not self.replay:
            return

        hu, hv, amt_raw, lbl = self.replay[-1]

        lbl_t = torch.tensor([[lbl]], dtype=torch.float32)

        self.opt.zero_grad()
        logit = self.clf(hu, hv, amt_raw)
        loss = self.loss_fn(logit, lbl_t)
        loss.backward()
        self.opt.step()

        self.steps += 1
        if self.steps % 100 == 0:
            num_nodes = self.mapper.next
            mem_size = self.memory_tensor.size(0)
            mem_mb = (mem_size * self.mem_dim * 4) / (1024 * 1024)
            print(f"[TRAIN] step={self.steps} loss={loss.item():.4f} nodes={num_nodes}/{mem_size} mem={mem_mb:.1f}MB")

    # ----------------------------------------------------------
    # Checkpointing
    # ----------------------------------------------------------

    def save(self, path):
        torch.save({
            "mapper": self.mapper.state(),
            "memory": self.memory_tensor,
            "encoder": self.encoder.state_dict(),
            "clf": self.clf.state_dict(),
            "mem_dim": self.mem_dim,
            "msg_dim": self.msg_dim,
        }, path)
        print(f"[CHECKPOINT] Saved to {path}")

    def load(self, path):
        ck = torch.load(path, map_location="cpu")
        self.mapper.load_state(ck["mapper"])
        self.memory_tensor = ck["memory"]
        self.encoder.load_state_dict(ck["encoder"])
        self.clf.load_state_dict(ck["clf"])
        
        # Verify dimensions match
        if ck.get("mem_dim") != self.mem_dim or ck.get("msg_dim") != self.msg_dim:
            print(f"[WARNING] Checkpoint dimensions don't match!")
        
        print(f"[CHECKPOINT] Loaded from {path}")
        print(f"  Nodes: {self.mapper.next}, Memory size: {self.memory_tensor.size(0)}")


# ------------------------------------------
# NATS LOOP
# ------------------------------------------

async def run(url, subject, ckpt):
    # Start with smaller memory
    trainer = OnlineTrainer(initial_nodes=1000, mem_dim=64, msg_dim=32)
    
    try:
        trainer.load(ckpt)
        print("[TRAINER] Resumed from checkpoint")
    except Exception as e:
        print(f"[TRAINER] Starting new session: {e}")

    nc = NATS()
    await nc.connect(url)
    print(f"[TRAINER] Connected to NATS at {url}")
    print(f"[TRAINER] Subscribed to '{subject}'")

    count = 0

    async def handler(msg):
        nonlocal count
        
        try:
            m = json.loads(msg.data.decode())
            p = trainer.process(m, train=True)
            
            count += 1
            if count % 100 == 0:
                print(f"[EVENT] Processed {count} transactions")
            
            if count % 10 == 0:  # Print every 10th
                print(f"[EVENT] {m.get('src', '?')}->{m.get('dst', '?')} amt={m.get('amount', 0)} prob={p:.4f}")

            # Save checkpoint every 500 steps
            if trainer.steps > 0 and trainer.steps % 500 == 0:
                trainer.save(ckpt)
                
        except Exception as e:
            print(f"[ERROR] Failed to process message: {e}")

    await nc.subscribe(subject, cb=handler)
    print("[TRAINER] Waiting for transactions...")

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Saving final checkpoint...")
        trainer.save(ckpt)
        await nc.drain()
        await nc.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--nats", default="nats://127.0.0.1:4222")
    parser.add_argument("--subject", default="transactions")
    parser.add_argument("--checkpoint", default="checkpoint.pth")
    args = parser.parse_args()
    
    print(f"[STARTUP] NATS: {args.nats}")
    print(f"[STARTUP] Subject: {args.subject}")
    print(f"[STARTUP] Checkpoint: {args.checkpoint}")
    
    asyncio.run(run(args.nats, args.subject, args.checkpoint))