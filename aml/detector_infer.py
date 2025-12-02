# detector_infer.py (AUTO-RELOADING + NON-BLOCKING)

import argparse, asyncio, json, time, signal, sys
from datetime import datetime
import torch
from nats.aio.client import Client as NATS
import threading


# -----------------------------------------------------
# Utilities
# -----------------------------------------------------

def parse_ts(ts_str):
    for fmt in ("%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(ts_str, fmt).timestamp()
        except:
            continue
    return time.time()


def hash_bucket(s, buckets=512):
    return (hash(str(s)) & 0xffffffff) % buckets


# -----------------------------------------------------
# Node mapper
# -----------------------------------------------------
class NodeMapper:
    def __init__(self):
        self.id2idx = {}
        self.next = 0

    def get(self, s):
        s = str(s)
        if s not in self.id2idx:
            self.id2idx[s] = self.next
            self.next += 1
        return self.id2idx[s]

    def load_state(self, s):
        self.id2idx = s
        self.next = max(s.values()) + 1 if len(s) else 0


# -----------------------------------------------------
# Message Encoder
# -----------------------------------------------------
class MessageEncoder(torch.nn.Module):
    def __init__(self, msg_dim=32, cat_buckets=512, emb_dim=8):
        super().__init__()
        self.emb = torch.nn.Embedding(cat_buckets, emb_dim)
        self.fc = torch.nn.Sequential(
            torch.nn.Linear(1 + emb_dim * 3, msg_dim),
            torch.nn.ReLU(),
            torch.nn.Linear(msg_dim, msg_dim),
        )

    def forward(self, amt, c1, c2, c3):
        return self.fc(torch.cat([amt, self.emb(c1), self.emb(c2), self.emb(c3)], dim=1))


# -----------------------------------------------------
# Memory Module (NO GRAD)
# -----------------------------------------------------
class MemoryModule:
    def __init__(self, mem_dim=64, msg_dim=32):
        self.gru = torch.nn.GRUCell(msg_dim, mem_dim)
        self.mem_dim = mem_dim

    def update(self, mem, idx, msg):
        with torch.no_grad():
            prev = mem[idx:idx + 1]
            mem[idx:idx + 1] = self.gru(msg, prev)


# -----------------------------------------------------
# Edge Classifier
# -----------------------------------------------------
class EdgeClassifier(torch.nn.Module):
    def __init__(self, mem_dim=64):
        super().__init__()
        self.net = torch.nn.Sequential(
            torch.nn.Linear(mem_dim * 2 + 1, 128),
            torch.nn.ReLU(),
            torch.nn.Linear(128, 1),
        )

    def forward(self, hu, hv, amt_raw):
        return self.net(torch.cat([hu, hv, amt_raw], dim=1))


# =====================================================================
#        INFERENCE ENGINE (with PERIODIC CHECKPOINT RELOADING)
# =====================================================================

class InferenceEngine:
    def __init__(self, checkpoint, reload_interval=10):
        self.checkpoint_path = checkpoint
        self.reload_interval = reload_interval
        self.reload_lock = threading.Lock()

        self.mapper = NodeMapper()
        self.mem_dim = 64
        self.msg_dim = 32
        self.cat_buckets = 512

        self.memory = torch.zeros(1000, self.mem_dim)
        self.encoder = MessageEncoder(self.msg_dim, self.cat_buckets)
        self.mem_module = MemoryModule(self.mem_dim, self.msg_dim)
        self.clf = EdgeClassifier(self.mem_dim)

        self.TP = self.TN = self.FP = self.FN = 0
        self.total = 0
        self.processed = 0

        self._load_checkpoint(silent=False)

    # -----------------------------------------------------
    # Load checkpoint (thread-safe)
    # -----------------------------------------------------
    def _load_checkpoint(self, silent=True):
        try:
            ck = torch.load(self.checkpoint_path, map_location="cpu", weights_only=False)

            with self.reload_lock:
                self.mapper.load_state(ck["mapper"])
                self.memory = ck["memory"]
                self.encoder.load_state_dict(ck["encoder"])
                self.clf.load_state_dict(ck["clf"])

            if not silent:
                print(f"[INFER] Checkpoint loaded successfully ({self.checkpoint_path})")

        except Exception as e:
            if not silent:
                print(f"[INFER] No checkpoint loaded: {e}")

    # -----------------------------------------------------
    # Background task: reload the checkpoint every N sec
    # -----------------------------------------------------
    async def checkpoint_reloader(self):
        while True:
            await asyncio.sleep(self.reload_interval)
            print(f"\n[INFER] Auto-reloading checkpoint...")
            self._load_checkpoint(silent=False)

    # -----------------------------------------------------
    # Memory growth
    # -----------------------------------------------------
    def ensure(self, idx):
        if idx >= self.memory.size(0):
            old = self.memory.size(0)
            new = idx * 2
            print(f"[INFER] Expanding memory {old} → {new}")
            tmp = torch.zeros(new, self.mem_dim)
            tmp[:old] = self.memory
            self.memory = tmp

    # -----------------------------------------------------
    # Perform inference
    # -----------------------------------------------------
    def infer_and_score(self, m):
        self.processed += 1

        src = self.mapper.get(m.get("src", "?"))
        dst = self.mapper.get(m.get("dst", "?"))
        self.ensure(max(src, dst))

        amt = float(m.get("amount", 0))
        amt_n = torch.tensor([[amt / 10000]], dtype=torch.float32)
        amt_raw = torch.tensor([[amt]], dtype=torch.float32)

        c1 = torch.tensor([hash_bucket(m.get("payment_type", ""))], dtype=torch.long)
        c2 = torch.tensor([hash_bucket(m.get("sender_bank_location", ""))], dtype=torch.long)
        c3 = torch.tensor([hash_bucket(m.get("receiver_bank_location", ""))], dtype=torch.long)

        msg_vec = self.encoder(amt_n, c1, c2, c3)

        mem_copy = self.memory.clone()
        self.mem_module.update(mem_copy, src, msg_vec)
        self.mem_module.update(mem_copy, dst, msg_vec)

        hu = mem_copy[src:src + 1]
        hv = mem_copy[dst:dst + 1]

        with torch.no_grad():
            logit = self.clf(hu, hv, amt_raw)
            prob = torch.sigmoid(logit).item()

        pred = 1 if prob >= 0.5 else 0
        true = int(m.get("label", -1))

        if true in (0, 1):
            self.total += 1
            if pred == true:
                if pred == 1: self.TP += 1
                else: self.TN += 1
            else:
                if pred == 1: self.FP += 1
                else: self.FN += 1

        if self.processed % 10 == 0:
            print(f"[INFER] {m.get('src')}->{m.get('dst')} amt={amt:.2f} prob={prob:.4f} pred={pred} true={true}")

        return prob

    # -----------------------------------------------------
    # Print metrics on shutdown
    # -----------------------------------------------------
    def print_metrics(self):
        print("\n================ FINAL METRICS ================")
        print(f"Processed: {self.processed}")
        print(f"Labeled:   {self.total}\n")

        if self.total == 0:
            print("No labeled samples.")
            return

        accuracy = (self.TP + self.TN) / self.total
        precision = self.TP / (self.TP + self.FP + 1e-9)
        recall = self.TP / (self.TP + self.FN + 1e-9)
        f1 = 2 * precision * recall / (precision + recall + 1e-9)

        print(f"TP={self.TP}, TN={self.TN}, FP={self.FP}, FN={self.FN}")
        print(f"Accuracy : {accuracy:.4f}")
        print(f"Precision: {precision:.4f}")
        print(f"Recall   : {recall:.4f}")
        print(f"F1 Score : {f1:.4f}")
        print("===============================================\n")


# =====================================================================
# NATS Event Loop
# =====================================================================

async def run(url, subject, ckpt):
    engine = InferenceEngine(ckpt)

    # start reloader task (non-blocking)
    asyncio.create_task(engine.checkpoint_reloader())

    nc = NATS()
    await nc.connect(url)

    # graceful exit
    def stop(sig, frame):
        print("\n[INFER] CTRL+C received → printing metrics…")
        engine.print_metrics()
        sys.exit(0)

    signal.signal(signal.SIGINT, stop)

    async def handler(msg):
        try:
            data = json.loads(msg.data.decode())
            engine.infer_and_score(data)
        except Exception as e:
            print("[ERROR] failed msg:", e)

    await nc.subscribe(subject, cb=handler)
    print(f"[INFER] Subscribed to '{subject}' — streaming inference live…")

    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--nats", default="nats://127.0.0.1:4222")
    parser.add_argument("--subject", default="transactions")
    parser.add_argument("--checkpoint", default="checkpoint.pth")
    parser.add_argument("--reload", type=int, default=10)
    args = parser.parse_args()

    print("=" * 60)
    print("AML DETECTOR — INFERENCE MODE (AUTO-RELOADING)")
    print("=" * 60)

    asyncio.run(run(args.nats, args.subject, args.checkpoint))
