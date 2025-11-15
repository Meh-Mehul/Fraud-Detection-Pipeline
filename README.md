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
1. Install all dependencies ```pip install -r requirements.txt``` as well as ```nats-server```.
2. Install the dataset as name it as ```fraudTrain.csv``` and store in the root directory of this project.
3. Run ```nats-server``` in a terminal.
4. Run the python files in the following order (all in different terminals):
-      python3 run_detector.py
-      python3 run_report.py
-      python3 run_publisher.py



#### Sources:
dataset from:
https://www.kaggle.com/datasets/kartik2112/fraud-detection?resource=download&select=fraudTrain.csv