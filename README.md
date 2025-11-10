To run this:

Open 4 terminals: (and then also conda activate the virtual env)
terminal 1: nats-server
terminal 2: python ultra_precision_detector.py
terminal 3: fraud_report_gen.py
terminal 4: fraud_publisher.py


dataset from:
https://drive.google.com/file/d/1-DQEylAJap9aLtvHCFzMzUQmRvZzXNP4/view?usp=drive_link

video:
https://drive.google.com/file/d/1-DQEylAJap9aLtvHCFzMzUQmRvZzXNP4/view?usp=sharing

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

