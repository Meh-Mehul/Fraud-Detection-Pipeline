"""
Pathway streaming inference with periodic checkpoint reload.
Runs in parallel with trainer on same NATS stream.
"""

import pathway as pw
import torch
import torch.nn as nn
import time
import argparse
import os


# ======================================================
# Utilities
# ======================================================

def hash_bucket(x, buckets=512):
    return (hash(str(x)) & 0xffffffff) % buckets


# ======================================================
# Model Components
# ======================================================

class MessageEncoder(nn.Module):
    def __init__(self, msg_dim=32, cat_buckets=512, emb_dim=8):
        super().__init__()
        self.emb = nn.Embedding(cat_buckets, emb_dim)
        self.fc = nn.Sequential(
            nn.Linear(1 + emb_dim * 3, msg_dim),
            nn.ReLU(),
            nn.Linear(msg_dim, msg_dim),
        )

    def forward(self, amt, c1, c2, c3):
        return self.fc(torch.cat([
            amt, self.emb(c1), self.emb(c2), self.emb(c3)
        ], dim=1))


class MemoryModule:
    def __init__(self, mem_dim=64, msg_dim=32):
        self.gru = nn.GRUCell(msg_dim, mem_dim)
        self.mem_dim = mem_dim

    def update(self, mem_tensor, idx, msg_vec):
        prev = mem_tensor[idx:idx+1]
        mem_tensor[idx:idx+1] = self.gru(msg_vec, prev)


class EdgeClassifier(nn.Module):
    def __init__(self, mem_dim=64):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(mem_dim * 2 + 1, 128),
            nn.ReLU(),
            nn.Linear(128, 1),
        )

    def forward(self, hu, hv, amt_raw):
        return self.net(torch.cat([hu, hv, amt_raw], dim=1))


# ======================================================
# Inference Model with Auto Reload
# ======================================================

class InferenceModel:
    def __init__(self, checkpoint_path):
        self.checkpoint_path = checkpoint_path
        self.last_loaded = 0
        self.mem_dim = 64
        self.msg_dim = 32
        self.cat_buckets = 512

        # Model components
        self.encoder = MessageEncoder(self.msg_dim, self.cat_buckets)
        self.classifier = EdgeClassifier(self.mem_dim)
        self.mem_module = MemoryModule(self.mem_dim, self.msg_dim)

        # State
        self.memory = torch.zeros(50_000, self.mem_dim)
        self.node_map = {}
        self.next_node = 0

        # Load initial checkpoint
        self._load_checkpoint(force=True)

    def _load_checkpoint(self, force=False):
        """Load checkpoint if modified or every 10s."""
        try:
            t = time.time()
            if not force and (t - self.last_loaded) < 10:
                return

            if not os.path.exists(self.checkpoint_path):
                print(f"[INFER] Checkpoint not found: {self.checkpoint_path}")
                return

            ck = torch.load(self.checkpoint_path, map_location="cpu")
            self.encoder.load_state_dict(ck["encoder"])
            self.classifier.load_state_dict(ck["clf"])
            
            # Load memory (size may grow)
            mem_size = ck["memory"].shape[0]
            self.memory[:mem_size] = ck["memory"]
            
            self.node_map = ck.get("mapper", {}).copy()
            self.next_node = max(self.node_map.values()) + 1 if self.node_map else 0
            
            self.last_loaded = t
            print(f"[INFER] Checkpoint reloaded. Nodes: {self.next_node}, Memory: {mem_size}")

        except Exception as e:
            print(f"[INFER] Checkpoint load failed: {e}")

    def get_node(self, x):
        """Get node index (creates if new)."""
        x = str(x)
        if x not in self.node_map:
            self.node_map[x] = self.next_node
            self.next_node += 1
        return self.node_map[x]

    def infer(self, src_val, dst_val, amount_val, payment_type_val, 
              sender_bank_location_val, receiver_bank_location_val, label_val):
        """Run inference on single record. Returns (prob, pred_label)."""
        # Periodic checkpoint reload
        self._load_checkpoint()

        try:
            src = self.get_node(src_val)
            dst = self.get_node(dst_val)

            # Grow memory if needed
            max_id = max(src, dst)
            if max_id >= self.memory.size(0):
                new_size = max_id * 2
                new_mem = torch.zeros(new_size, self.mem_dim)
                new_mem[:self.memory.size(0)] = self.memory
                self.memory = new_mem

            amt = float(amount_val or 0.0)
            amt_n = torch.tensor([[amt / 10000]], dtype=torch.float32)
            amt_raw = torch.tensor([[amt]], dtype=torch.float32)

            c1 = torch.tensor([hash_bucket(payment_type_val or "")], dtype=torch.long)
            c2 = torch.tensor([hash_bucket(sender_bank_location_val or "")], dtype=torch.long)
            c3 = torch.tensor([hash_bucket(receiver_bank_location_val or "")], dtype=torch.long)

            # Encode message
            msg_vec = self.encoder(amt_n, c1, c2, c3)

            # Copy memory for inference (non-destructive)
            mem_copy = self.memory.clone()
            self.mem_module.update(mem_copy, src, msg_vec)
            self.mem_module.update(mem_copy, dst, msg_vec)

            hu = mem_copy[src:src+1]
            hv = mem_copy[dst:dst+1]

            with torch.no_grad():
                out = self.classifier(hu, hv, amt_raw)
                prob = torch.sigmoid(out).item()

            pred_label = 1 if prob >= 0.5 else 0

            return prob, pred_label

        except Exception as e:
            print(f"[INFER] Error: {e}")
            return 0.0, 0


# ======================================================
# Pathway Pipeline
# ======================================================

def run_pipeline(nats_url, subject, checkpoint):
    # Define schema for incoming NATS JSON messages
    class TransactionSchema(pw.Schema):
        src: str
        dst: str
        amount: float
        payment_type: str
        sender_bank_location: str
        receiver_bank_location: str
        label: int

    # Source: read from NATS
    input_stream = pw.io.nats.read(
        uri=nats_url,
        subject=subject,
        topic="transactions",
        format="json",
        schema=TransactionSchema,
    )

    model = InferenceModel(checkpoint)

    # Define inference UDFs for each output
    def _get_probability(src_val, dst_val, amount_val, payment_type_val,
                        sender_bank_location_val, receiver_bank_location_val, label_val):
        prob, _ = model.infer(
            src_val, dst_val, amount_val, payment_type_val,
            sender_bank_location_val, receiver_bank_location_val,
            label_val
        )
        return prob

    def _get_pred_label(src_val, dst_val, amount_val, payment_type_val,
                       sender_bank_location_val, receiver_bank_location_val, label_val):
        _, pred = model.infer(
            src_val, dst_val, amount_val, payment_type_val,
            sender_bank_location_val, receiver_bank_location_val,
            label_val
        )
        return pred

    # Apply inference
    results = input_stream.select(
        src=pw.this.src,
        dst=pw.this.dst,
        amount=pw.this.amount,
        probability=pw.udf(_get_probability, return_type=float)(
            pw.this.src,
            pw.this.dst,
            pw.this.amount,
            pw.this.payment_type,
            pw.this.sender_bank_location,
            pw.this.receiver_bank_location,
            pw.this.label,
        ),
        pred_label=pw.udf(_get_pred_label, return_type=int)(
            pw.this.src,
            pw.this.dst,
            pw.this.amount,
            pw.this.payment_type,
            pw.this.sender_bank_location,
            pw.this.receiver_bank_location,
            pw.this.label,
        ),
        true_label=pw.this.label
    )

    # pw.io.stdout(results)
    pw.io.null.write(results)
    pw.run()


# ======================================================
# Entry Point
# ======================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pathway AML Inference")
    parser.add_argument("--nats", default="nats://127.0.0.1:4222")
    parser.add_argument("--subject", default="transactions")
    parser.add_argument("--checkpoint", default="checkpoint.pth")
    args = parser.parse_args()

    print("\n=== Pathway AML Inference (Auto Reload) ===\n")
    print(f"NATS: {args.nats}")
    print(f"Subject: {args.subject}")
    print(f"Checkpoint: {args.checkpoint}\n")
    
    run_pipeline(args.nats, args.subject, args.checkpoint)