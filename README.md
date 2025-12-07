# Real-Time Fraud Detection Pipeline

## Inter-IIT Tech Meet 14.0 | Pathway Problem Statement
### Team 82

---

## Overview

A **production-ready, real-time fraud detection system** built with Pathway for streaming data processing. The pipeline uses **online machine learning** with incremental learning capabilities, combining ML models with rule-based heuristics to detect fraudulent transactions in near real-time.

### Key Features

- **Real-time Processing**: Sub-second detection latency (~1-5ms)
- **Online Learning**: Models continuously improve from feedback without retraining
- **Hybrid Detection**: Combines ML (Hoeffding Adaptive Trees) with rule-based heuristics
- **Human-in-the-Loop**: Frontend for fraud analysts to review and provide feedback
- **Fully Containerized**: Single-command deployment with Docker
- **Production Metrics**: Grafana dashboard with F1-score, latency, and throughput monitoring

---

## Architecture

```
                                    ┌──────────────────────────────────────────────────────────────┐
                                    │                    FRAUD DETECTION PIPELINE                   │
                                    └──────────────────────────────────────────────────────────────┘
                                    
    ┌─────────────┐                              ┌─────────────────────────────────────────────────────┐
    │fraudTrain.csv│                              │                   NATS JetStream                    │
    └──────┬──────┘                              │         (Message Broker - 4222)                     │
           │                                      └─────────────────────────────────────────────────────┘
           ▼                                                    │
    ┌─────────────┐      fraud.transactions       ┌─────────────▼─────────────┐     fraud.alerts
    │  Publisher  │─────────────────────────────▶│       DETECTOR            │─────────────────────┐
    │             │      fraud.feedback           │   (Port 8001)             │                     │
    │  (pub_common)│─────────────────────────────▶│                           │                     │
    └─────────────┘                               │  • ML Inference           │                     │
                                                  │  • Rule Matching          │                     │
                                                  │  • Risk Scoring           │                     │
                                                  └───────────┬───────────────┘                     │
                                                              │                                     │
                                                              │ fraud.results                       │
                    ┌─────────────────────────────────────────┼─────────────────────────────────────┼───┐
                    │                                         │                                     │   │
                    ▼                                         ▼                                     ▼   │
    ┌───────────────────────────┐         ┌───────────────────────────┐         ┌───────────────────────▼───┐
    │     STATS UPDATER         │         │    FEEDBACK WRITER        │         │     REPORT GENERATOR      │
    │     (Port 8002)           │         │    (Port 8003)            │         │     (Port 8004)           │
    │                           │         │                           │         │                           │
    │  • Updates customer stats │         │  • Online model training  │         │  • PDF report generation  │
    │  • Updates merchant stats │         │  • Performance metrics    │         │  • JSON metadata          │
    │  • Updates category stats │         │  • F1/Precision/Recall    │         │  • Alert explanations     │
    └───────────┬───────────────┘         └───────────────────────────┘         └───────────────┬───────────┘
                │                                                                               │
                ▼                                                                               ▼
    ┌───────────────────────────┐                                               ┌───────────────────────────┐
    │        REDIS              │                                               │     FRONTEND              │
    │                           │                                               │     (Port 8000)           │
    │  • Customer profiles      │                                               │                           │
    │  • Merchant risk scores   │                                               │  • Fraud Investigation    │
    │  • Category risk rates    │                                               │  • Feedback submission    │
    └───────────────────────────┘                                               │  • False negative review  │
                                                                                └───────────────────────────┘
                    ┌───────────────────────────────────────────────────────────────────┐
                    │                         MONITORING                                 │
                    │  • Prometheus (Port 9090) - Metrics collection                    │
                    │  • Grafana (Port 3000) - Dashboard visualization                  │
                    └───────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Stream Processing** | Pathway | Real-time data pipeline framework |
| **Message Broker** | NATS JetStream | High-performance pub/sub messaging |
| **ML Framework** | River | Online/incremental machine learning |
| **Stats Store** | Redis | Real-time customer/merchant statistics |
| **Metrics** | Prometheus + Grafana | Observability and monitoring |
| **Frontend** | FastAPI + Jinja2 | Fraud investigation web UI |
| **Containerization** | Docker Compose | Production deployment |

---

## Machine Learning Approach

### Model Architecture

We use an **ensemble of two Hoeffding Adaptive Tree Classifiers**:

```python
# Main Model: Pipeline with StandardScaler + HAT
model_main = compose.Pipeline(
    preprocessing.StandardScaler(),
    tree.HoeffdingAdaptiveTreeClassifier(grace_period=200, delta=1e-5)
)

# Validator Model: Standalone HAT with different hyperparameters
model_validator = tree.HoeffdingAdaptiveTreeClassifier(grace_period=150, delta=1e-4)
```

### Feature Engineering

| Feature | Description | Calculation |
|---------|-------------|-------------|
| `amt` | Transaction amount | Raw value |
| `z_amt` | Normalized amount | (amt - avg_amt) / std_amt |
| `amt_ratio` | Amount relative to average | amt / avg_amt |
| `dist` | Distance to merchant | Haversine formula (km) |
| `z_dist` | Normalized distance | (dist - avg_dist) / std_dist |
| `hr` | Hour of transaction | Extracted from timestamp |
| `merch_risk` | Merchant fraud rate | Historical fraud % |
| `cat_risk` | Category fraud rate | Historical fraud % |
| `online` | Is online category | Binary flag |
| `late_night` | Late night transaction | 1 if 1am-5am |
| `fraud_history` | Customer fraud count | Historical count |
| `n` | Transaction count | min(txn_count, 1000) |

### Online Learning

The model uses **incremental learning** - it updates with each labeled transaction without needing to retrain on the entire dataset:

1. **Pre-training**: Models bootstrap on ~12K balanced samples (25% fraud, 75% legitimate)
2. **Live Learning**: Feedback from fraud analysts continuously improves the model
3. **Concept Drift**: Hoeffding Adaptive Trees automatically adapt to changing patterns

### Scoring System

```
ML Score = (model_main.predict_proba[fraud] + model_validator.predict_proba[fraud]) / 2 × 100

Alert Tiers:
  - Tier 1 (Critical): Rule match OR ML Score ≥ 80%
  - Tier 2 (High): ML Score ≥ 50%
  - Tier 3 (Medium): ML Score ≥ 30%
```

---

## Quick Start

### Prerequisites

- Docker & Docker Compose
- 8GB RAM recommended
- `fraudTrain.csv` dataset in project root

### Start Pipeline

```bash
# First time: Build, pretrain, and start all services
./pipeline.sh start
```

This executes:
1. Builds Docker images (no cache)
2. Starts infrastructure (Redis, NATS, Prometheus, Grafana)
3. Runs model pretraining (~3-5 minutes)
4. Loads statistics into Redis
5. Starts all pipeline nodes

### Restart (Skip Pretraining)

```bash
# Uses existing model, resets metrics
./pipeline.sh restart
```

### Stop Pipeline

```bash
./pipeline.sh stop
```

### Other Commands

```bash
./pipeline.sh status    # Check container status
./pipeline.sh logs      # View all logs
./pipeline.sh logs -f detector  # Follow specific container
```

---

## Endpoints

| Service | URL | Credentials |
|---------|-----|-------------|
| **Frontend** | http://localhost:8000 | - |
| **Grafana** | http://localhost:3000 | admin/admin |
| **Prometheus** | http://localhost:9090 | - |
| **NATS Monitoring** | http://localhost:8222 | - |

---

## Pipeline Components

| Component | Port | Description |
|-----------|------|-------------|
| `detector` | 8001 | Real-time fraud detection (ML + rules) |
| `stats-updater` | 8002 | Updates Redis with transaction statistics |
| `feedback` | 8003 | Online model training from labeled data |
| `report` | 8004 | PDF/JSON report generation |
| `publisher` | - | Streams transactions from CSV to NATS |
| `frontend` | 8000 | Web UI for fraud investigation |
| `negative-collector` | - | Collects non-alerts for false negative review |

---

## Performance Metrics

The Grafana dashboard displays real-time metrics:

| Metric | Description |
|--------|-------------|
| **F1 Score** | Harmonic mean of precision and recall |
| **Pipeline Latency** | End-to-end processing time (p50) |
| **Fraud Alerts/min** | Alert rate by tier |
| **Model Updates/hr** | Training frequency |
| **Detector Latency** | ML inference time |

### Expected Performance

- **Latency**: 1-5ms per transaction (p50)
- **Throughput**: 25+ TPS sustained
- **F1 Score**: ~85-90% after warm-up

---

## Project Structure

```
Fraud-Detection-Pipeline/
├── pipeline.sh              # Main orchestration script
├── pretrain.py              # Model pretraining script
├── redis_manager.py         # Redis stats management
├── detector/
│   ├── detector_ronly.py    # Main fraud detector (Pathway)
│   └── detector_stats_upd.py # Stats updater node
├── feedback/
│   ├── feedback_writer.py   # Online learning node
│   └── negative_collector.py # False negative collector
├── publisher/
│   ├── pub_common.py        # Combined publisher
│   ├── pub_det.py           # Detector stream publisher
│   └── pub_feed.py          # Feedback stream publisher
├── report/
│   └── pathway_nats_report.py # PDF report generator
├── frontend/
│   └── main.py              # FastAPI web application
├── shared/
│   ├── model_store.py       # ML model persistence
│   ├── stats_store.py       # File-based stats (pretrain)
│   ├── redis_stats_store.py # Redis stats interface
│   ├── metrics.py           # Prometheus metrics
│   └── rules_loader.py      # Fraud rules loader
├── docker/
│   ├── docker-compose-full.yml
│   └── Dockerfile.pipeline
├── monitoring/
│   ├── grafana/             # Dashboard configs
│   └── prometheus-docker.yml
└── pathway_persistence/     # Trained models & stats
```

---

## Redis Manager

Utility for managing Redis statistics:

```bash
python redis_manager.py load     # Load stats from JSON
python redis_manager.py stats    # Show summary
python redis_manager.py inspect <CC_NUM>  # View customer profile
python redis_manager.py export   # Export to JSON
python redis_manager.py clear    # Clear all data
```

---

## Development Setup

For running without Docker:

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start infrastructure
docker-compose -f docker/docker-compose-full.yml up redis nats prometheus grafana -d

# 3. Pretrain model
python pretrain.py

# 4. Load Redis stats
python redis_manager.py load

# 5. Start components (separate terminals)
python -c "from detector.detector_ronly import run_detector; run_detector()"
python -c "from feedback.feedback_writer import run_feedback_writer; run_feedback_writer()"
python -c "from report.pathway_nats_report import run_report_generator; run_report_generator()"
python publisher/pub_common.py
python frontend/main.py
```

---

## Dataset

**Source**: [Kaggle - Credit Card Fraud Detection](https://www.kaggle.com/datasets/kartik2112/fraud-detection)

The dataset contains simulated credit card transactions with:
- ~1.3M transactions
- ~7.5K fraud cases (0.58% fraud rate)
- Features: transaction amount, location, timestamp, merchant, category

---

## Team 82

**Inter-IIT Tech Meet 14.0 - Pathway Problem Statement**

---