## Fraud Detection Pipeline (Inter-IIT 14.0 Pathway PS)
### Submission by :- Team 82

### Information:
This is our prototype implementation of the Fraud detection pipeline, specifically a subset of our final product.
This pipeline reads a stream of credit-card transactions and detects whether there is some fraud in it or not? It uses an incrementally-learnt model for this as well as some rule-based decision boundaries. After that this all context is sent to report-generator node to generate reports.

From the video demo, we can see that the speed of generation is near-real time.

We are yet to measure exact metrics but we are planning to improve the decision making models as well as complicate pipelines a bit more to get better and more explainable decisions. 

##### About online-learning
For now, we are learning through the live-transaction's target variable and training our model (online) on basis of that, but in the final pipeline we are planning to have a feedback-based learning paradigm in which flagged fraud is sent to bank's fraud analysis team, which mark it as true or not, and the model learns on the basis of that decision.

---

## Quick Start (Docker - Recommended)

The entire pipeline runs in Docker containers using `pipeline.sh`:

### First Time Setup
```bash
# Start with pretrain (builds images, pretrains model, starts all services)
./pipeline.sh start
```
This will:
- Build all Docker images
- Start infrastructure (Redis, NATS, Prometheus, Grafana)
- Run model pretraining (~3-5 minutes)
- Load Redis stats
- Start all pipeline nodes (detector, feedback, report, stats-updater, publisher, frontend, negative-collector)

### Restart (Skip Pretrain)
```bash
# Quick restart - uses existing trained model, resets metrics
./pipeline.sh restart
```
This skips pretraining and uses the existing model from `pathway_persistence/`.

### Stop Pipeline
```bash
./pipeline.sh stop
```

### View Logs
```bash
./pipeline.sh logs
```

### Check Status
```bash
./pipeline.sh status
```

---

## Endpoints

| Service | URL | Description |
|---------|-----|-------------|
| **Frontend** | http://localhost:8000 | Fraud Investigation Center |
| **Grafana** | http://localhost:3000 | Metrics Dashboard (admin/admin) |
| **Prometheus** | http://localhost:9090 | Raw metrics |
| **NATS** | localhost:4222 | Message broker |
| **Redis** | localhost:6379 | Stats store |

---

## Pipeline Components

| Component | Port | Description |
|-----------|------|-------------|
| `detector` | 8001 | Real-time fraud detection using ML + rules |
| `stats-updater` | 8002 | Updates Redis stats from transactions |
| `feedback` | 8003 | Model training from labeled data |
| `report` | 8004 | PDF report generation for fraud alerts |
| `publisher` | - | Streams transactions from CSV to NATS |
| `frontend` | 8000 | Web UI for fraud investigation |
| `negative-collector` | - | Collects non-alert transactions for false negative review |

---

## Frontend Features

- **Fraud Alerts Tab**: Review detected fraud cases, mark as Fraud/Legitimate
- **False Negative Review Tab**: Review transactions marked legitimate (catch missed frauds)
- **Grafana Button**: Quick link to monitoring dashboard
- **Auto-refresh**: Queue updates every 5 seconds

---

## Manual Setup (Development)

If you prefer running components manually without Docker:

### 1. Initial Setup
```bash
pip install -r requirements.txt
python3 pretrain.py  # Wait till it ends
```

### 2. Start Infrastructure
```bash
docker-compose -f docker-compose-monitoring.yml up -d
```

### 3. Load Redis Stats
```bash
python redis_manager.py load
```

### 4. Start Components (Each in Separate Terminal)
```bash
python run_detector.py
python run_report.py
python run_stats_updater.py
python run_feedback.py
python publisher/pub_common.py
python frontend/main.py
```

---

## Useful Commands

```bash
# Check Redis stats
python redis_manager.py stats

# Export Redis data to JSON
python redis_manager.py export

# Clear all Redis data
python redis_manager.py clear

# Free up port if in use
lsof -ti:8000 | xargs kill -9
```

---

## Architecture

```
fraudTrain.csv
      │
      ▼
┌─────────────┐    NATS     ┌─────────────┐
│  Publisher  │────────────▶│  Detector   │──────┐
└─────────────┘             └─────────────┘      │
                                  │              │
                                  ▼              ▼
                            ┌─────────────┐  ┌─────────────┐
                            │ Stats Upd.  │  │   Report    │
                            └─────────────┘  └─────────────┘
                                  │              │
                                  ▼              ▼
                            ┌─────────────┐  ┌─────────────┐
                            │   Redis     │  │ PDF Reports │
                            └─────────────┘  └─────────────┘
                                                 │
                                                 ▼
                                          ┌─────────────┐
                                          │  Frontend   │
                                          └─────────────┘
```

---

## Sources

Dataset from: https://www.kaggle.com/datasets/kartik2112/fraud-detection?resource=download&select=fraudTrain.csv