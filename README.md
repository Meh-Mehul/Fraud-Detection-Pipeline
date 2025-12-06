## Fraud Detection Pipeline (Inter-IIT 14.0 Pathway PS)
### Submission by :- Team 82

### Information:
This is our prototype implementation of the Fraud detection pipeline, specifically a subset of our final product.
This pipeline reads a stream of credit-card transactions and detects whether there is some fraud in it or not? It uses an incrementally-learnt model for this as well as some rule-based decision boundaries. After that this all context is sent to report-generator node to generate reports.

From the video demo, we can see that the speed of generation is near-real time.

We are yet to measure exact metrics but we are planning to improve the decision making models as well as complicate pipelines a bit more to get better and more explainable decisions. 

##### About online-learning
For now, we are learning through the live-transaction's target variable and training our model (online) on basis of that, but in the final pipeline we are planning to have a feedback-based learning paradigm in which flagged fraud is sent to bank's fraud analysis team, which mark it as true or not, and the model learns on the basis of that decision.


### Steps to Run:

#### 1. Initial Setup (First Time Only)
```bash
pip install -r requirements.txt
python3 pretrain.py  # Wait till it ends
```

#### 2. Start Monitoring Stack (Fresh Start)
```bash
# Stop any existing containers and reset data
docker-compose -f docker-compose-monitoring.yml down
docker volume rm fraud-detection-pipeline_prometheus-data 2>/dev/null
docker volume rm fraud-detection-pipeline_grafana-data 2>/dev/null

# Start fresh
docker-compose -f docker-compose-monitoring.yml up -d
```

#### 3. Clean Up Previous Run Data
```bash
./clean.sh
```

#### 4. Load Redis Stats
```bash
python redis_manager.py load
```

#### 5. Start Pipeline Components (Each in Separate Terminal)
```bash
python run_detector.py
python run_report.py
python run_stats_updater.py
python run_feedback.py
python publisher/pub_common.py
```

#### 6. Start Frontend (Human Review Interface)
```bash
python frontend/main.py
```
Open http://localhost:8000 for the Fraud Investigation Center

#### 7. Access Grafana Dashboard
Open http://localhost:3000 (default login: admin/admin)

---

### Optional Components

#### Latency Monitor (Auto-Restart on High Latency)
Monitors pipeline latency and auto-restarts if latency exceeds 10 seconds:
```bash
python latency_monitor.py
```

#### False Negative Collector (Review Missed Frauds)
Collects transactions marked as "legitimate" for false negative review:
```bash
python run_negative_collector.py
```
Access via the **"False Negative Review"** tab in the frontend.


### Quick Restart (After Stopping Pipeline)
```bash
# 1. Stop containers and reset Prometheus data
docker-compose -f docker-compose-monitoring.yml down
docker volume rm fraud-detection-pipeline_prometheus-data 2>/dev/null

# 2. Restart containers
docker-compose -f docker-compose-monitoring.yml up -d

# 3. Clean checkpoints and temp files
./clean.sh

# 4. Reload Redis stats
python redis_manager.py load

# 5. Start all components again (each in separate terminal)
```


### Useful Commands
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


### Frontend Features
- **Fraud Alerts Tab**: Review detected fraud cases, mark as Fraud/Legitimate
- **False Negative Review Tab**: Review transactions marked legitimate (catch missed frauds)
- **Grafana Button**: Quick link to monitoring dashboard
- **Auto-refresh**: Queue updates every 5 seconds


#### Sources:
dataset from:
https://www.kaggle.com/datasets/kartik2112/fraud-detection?resource=download&select=fraudTrain.csv