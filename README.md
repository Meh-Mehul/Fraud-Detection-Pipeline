


run the following:
# run the conda activate env before - uploaded the yml file for the same.


1. nats-server
2. python fraud_publisher.py
3. python try6_det.py
4. python fraud_subscriber.py



# with this setup i got:

[STATS] Total:  1,275,000 | Alerts:  8,884
        Acc:  99.2% | Prec:  31.9% | Rec:  44.0% | F1:  37.0%
        FPR: 0.484% | T1/T2/T3: 6043/2841/0
        TP: 2,833 | FP: 6,051 | FN: 3,599

─────────────────────────────────────────────────────────
          TRANSACTION STREAM COMPLETED
─────────────────────────────────────────────────────────
Total Transactions:     1,296,675
Processing Time:             1621.85s
Target Rate:                1,000 txns/sec
Actual Rate:                  800 txns/sec
Expected Time:               1296.67s
─────────────────────────────────────────────────────────




# sota_detector.py has all the cool features and paper implementation but poor results.
# try6_det.py has best results, at faster transactions per second