"""
FRAUD DETECTION EVALUATION SYSTEM with PATHWAY STORAGE
Uses local persistent storage to handle massive transaction volumes

Features:
- Persistent storage survives crashes/restarts
- Auto-cleanup of matched transactions
- Memory-efficient for slow detectors
- Recovery from interruptions

Save as: fraud_subscriber.py
Run: python fraud_subscriber.py
"""

import asyncio
import json
from nats.aio.client import Client as NATS
import time
import os
import sqlite3
from datetime import datetime

class TransactionStorage:
    """Local SQLite storage for transactions and alerts"""
    
    def __init__(self, db_path="fraud_detection.db"):
        self.db_path = db_path
        self.conn = None
        self.init_db()
    
    def init_db(self):
        """Initialize SQLite database"""
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        
        # Table for incoming transactions (ground truth)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                trans_num TEXT PRIMARY KEY,
                is_fraud INTEGER,
                amt REAL,
                merchant TEXT,
                category TEXT,
                location TEXT,
                cc_num INTEGER,
                timestamp REAL,
                matched INTEGER DEFAULT 0
            )
        ''')
        
        # Table for alerts (predictions)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS alerts (
                trans_num TEXT PRIMARY KEY,
                risk_score INTEGER,
                reasons TEXT,
                confidence INTEGER,
                timestamp REAL,
                matched INTEGER DEFAULT 0
            )
        ''')
        
        # Table for matched pairs (completed evaluations)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS matched_pairs (
                trans_num TEXT PRIMARY KEY,
                is_fraud INTEGER,
                predicted_fraud INTEGER,
                amt REAL,
                risk_score INTEGER,
                timestamp REAL
            )
        ''')
        
        # Create indexes for fast lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_txn_matched ON transactions(matched)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_alert_matched ON alerts(matched)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_txn_timestamp ON transactions(timestamp)')
        
        self.conn.commit()
        print(f"✓ SQLite storage initialized: {self.db_path}")
    
    async def store_transaction(self, data):
        """Store incoming transaction"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO transactions 
            (trans_num, is_fraud, amt, merchant, category, location, cc_num, timestamp, matched)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
        ''', (
            data['trans_num'],
            data['is_fraud'],
            data['amt'],
            data['merchant'],
            data['category'],
            data['location'],
            data['cc_num'],
            time.time()
        ))
        self.conn.commit()
    
    async def store_alert(self, data):
        """Store incoming alert"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO alerts 
            (trans_num, risk_score, reasons, confidence, timestamp, matched)
            VALUES (?, ?, ?, ?, ?, 0)
        ''', (
            data['trans_num'],
            data['risk_score'],
            data.get('reasons', ''),
            data.get('confidence', 0),
            time.time()
        ))
        self.conn.commit()
    
    async def try_match(self, trans_num):
        """Try to match transaction with alert"""
        cursor = self.conn.cursor()
        
        # Check if both exist and are unmatched
        cursor.execute('''
            SELECT t.trans_num, t.is_fraud, t.amt, t.merchant, t.category, t.location,
                   a.risk_score, a.reasons, a.confidence
            FROM transactions t
            INNER JOIN alerts a ON t.trans_num = a.trans_num
            WHERE t.trans_num = ? AND t.matched = 0 AND a.matched = 0
        ''', (trans_num,))
        
        result = cursor.fetchone()
        if result:
            trans_num, is_fraud, amt, merchant, category, location, risk_score, reasons, confidence = result
            
            # Store matched pair
            cursor.execute('''
                INSERT INTO matched_pairs 
                (trans_num, is_fraud, predicted_fraud, amt, risk_score, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (trans_num, is_fraud, 1, amt, risk_score, time.time()))
            
            # Mark as matched and DELETE from working tables
            cursor.execute('DELETE FROM transactions WHERE trans_num = ?', (trans_num,))
            cursor.execute('DELETE FROM alerts WHERE trans_num = ?', (trans_num,))
            
            self.conn.commit()
            
            return {
                'trans_num': trans_num,
                'is_fraud': is_fraud,
                'amt': amt,
                'merchant': merchant,
                'category': category,
                'location': location,
                'risk_score': risk_score,
                'reasons': reasons,
                'confidence': confidence
            }
        
        return None
    
    async def cleanup_old_transactions(self, max_age_seconds=60):
        """Mark old unmatched transactions as TN or FN"""
        cursor = self.conn.cursor()
        cutoff_time = time.time() - max_age_seconds
        
        # Get expired unmatched transactions
        cursor.execute('''
            SELECT trans_num, is_fraud, amt FROM transactions 
            WHERE timestamp < ? AND matched = 0
        ''', (cutoff_time,))
        
        expired = cursor.fetchall()
        tn_count = 0
        fn_count = 0
        
        for trans_num, is_fraud, amt in expired:
            # Store as matched pair (predicted as legitimate = 0)
            cursor.execute('''
                INSERT INTO matched_pairs 
                (trans_num, is_fraud, predicted_fraud, amt, risk_score, timestamp)
                VALUES (?, ?, 0, ?, 0, ?)
            ''', (trans_num, is_fraud, amt, time.time()))
            
            # DELETE from transactions
            cursor.execute('DELETE FROM transactions WHERE trans_num = ?', (trans_num,))
            
            if is_fraud == 1:
                fn_count += 1
            else:
                tn_count += 1
        
        self.conn.commit()
        return tn_count, fn_count, len(expired)
    
    async def cleanup_old_alerts(self, max_age_seconds=60):
        """Remove orphaned old alerts"""
        cursor = self.conn.cursor()
        cutoff_time = time.time() - max_age_seconds
        
        cursor.execute('DELETE FROM alerts WHERE timestamp < ? AND matched = 0', (cutoff_time,))
        deleted = cursor.rowcount
        self.conn.commit()
        return deleted
    
    def get_stats(self):
        """Get current storage statistics"""
        cursor = self.conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM transactions WHERE matched = 0')
        pending_txns = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM alerts WHERE matched = 0')
        pending_alerts = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM matched_pairs')
        matched = cursor.fetchone()[0]
        
        # Get confusion matrix from matched_pairs
        cursor.execute('''
            SELECT 
                SUM(CASE WHEN is_fraud = 1 AND predicted_fraud = 1 THEN 1 ELSE 0 END) as tp,
                SUM(CASE WHEN is_fraud = 0 AND predicted_fraud = 1 THEN 1 ELSE 0 END) as fp,
                SUM(CASE WHEN is_fraud = 0 AND predicted_fraud = 0 THEN 1 ELSE 0 END) as tn,
                SUM(CASE WHEN is_fraud = 1 AND predicted_fraud = 0 THEN 1 ELSE 0 END) as fn
            FROM matched_pairs
        ''')
        
        tp, fp, tn, fn = cursor.fetchone()
        
        return {
            'pending_txns': pending_txns,
            'pending_alerts': pending_alerts,
            'matched': matched,
            'tp': tp or 0,
            'fp': fp or 0,
            'tn': tn or 0,
            'fn': fn or 0
        }
    
    def get_db_size(self):
        """Get database file size in MB"""
        if os.path.exists(self.db_path):
            size_bytes = os.path.getsize(self.db_path)
            return size_bytes / (1024 * 1024)
        return 0
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()


async def main():
    print("═══════════════════════════════════════════════════════════")
    print("  FRAUD DETECTION EVALUATION with PERSISTENT STORAGE v3.0")
    print("═══════════════════════════════════════════════════════════")
    print()
    
    # Initialize storage
    storage = TransactionStorage()
    
    # Check for existing data
    existing_stats = storage.get_stats()
    if existing_stats['matched'] > 0 or existing_stats['pending_txns'] > 0:
        print(f"📂 Found existing data:")
        print(f"   - Matched pairs: {existing_stats['matched']:,}")
        print(f"   - Pending transactions: {existing_stats['pending_txns']:,}")
        print(f"   - Pending alerts: {existing_stats['pending_alerts']:,}")
        print(f"   - Database size: {storage.get_db_size():.2f} MB")
        
        response = input("\n   Continue with existing data? (y/n): ")
        if response.lower() != 'y':
            storage.close()
            os.remove("fraud_detection.db")
            print("   Database cleared. Starting fresh...\n")
            storage = TransactionStorage()
        else:
            print("   Resuming from previous session...\n")
    
    nc = NATS()
    print("⏳ Connecting to NATS message broker...")
    await nc.connect("nats://localhost:4222")
    print("✓ Connected to NATS (nats://localhost:4222)")
    print("✓ Subscribed to: fraud.transactions (ground truth)")
    print("✓ Subscribed to: fraud.alerts (ML predictions)")
    print("✓ Storage: SQLite persistent database")
    print()
    print("─────────────────────────────────────────────────────────")
    print("        REAL-TIME FRAUD DETECTION with PERSISTENCE")
    print("─────────────────────────────────────────────────────────")
    print()
    
    # Statistics tracking
    stats = {
        'total_transactions': 0,
        'total_frauds': 0,
        'total_legitimate': 0,
        'total_alerts': 0,
    }
    
    start_time = time.time()
    last_report_time = time.time()
    last_cleanup_time = time.time()
    
    # Handler for incoming transactions (ground truth)
    async def transaction_handler(msg):
        nonlocal last_report_time, last_cleanup_time
        try:
            data = json.loads(msg.data.decode())
            stats['total_transactions'] += 1
            
            if data['is_fraud'] == 1:
                stats['total_frauds'] += 1
            else:
                stats['total_legitimate'] += 1
            
            # Add location field if not present
            if 'location' not in data:
                data['location'] = f"{data['city']}, {data['state']}"
            
            # Store transaction
            await storage.store_transaction(data)
            
            # Try to match immediately
            match = await storage.try_match(data['trans_num'])
            if match:
                process_match(match)
        
        except Exception as e:
            print(f"⚠️ Transaction handler error: {e}")
    
    # Handler for fraud alerts (predictions)
    async def alert_handler(msg):
        try:
            data = json.loads(msg.data.decode())
            stats['total_alerts'] += 1
            
            # Store alert
            await storage.store_alert(data)
            
            # Try to match immediately
            match = await storage.try_match(data['trans_num'])
            if match:
                process_match(match)
                
        except Exception as e:
            print(f"⚠️ Alert handler error: {e}")
    
    def process_match(match):
        """Process matched transaction and alert"""
        is_actual_fraud = match['is_fraud'] == 1
        is_predicted_fraud = True  # Alert exists = predicted as fraud
        
        if is_actual_fraud and is_predicted_fraud:
            status = "✓ TRUE POS"
            emoji = "🎯"
        elif not is_actual_fraud and is_predicted_fraud:
            status = "✗ FALSE POS"
            emoji = "⚠️"
        
        # Print alert with evaluation
        risk = match['risk_score']
        print(f"{emoji} {status} | Risk: {risk:>3}/100 | ${match['amt']:>8.2f} | "
              f"{match['merchant'][:30]:<30} | {match['category']:<15} | "
              f"{match['location']}")
    
    # Subscribe to both topics
    await nc.subscribe("fraud.transactions", cb=transaction_handler)
    await nc.subscribe("fraud.alerts", cb=alert_handler)
    
    print("👀 Monitoring transaction stream and fraud alerts...")
    print("   Using persistent storage (survives crashes!)")
    print("   Press Ctrl+C to view final statistics\n")
    
    try:
        while True:
            await asyncio.sleep(5)
            
            current_time = time.time()
            
            # Cleanup old transactions every 30 seconds
            if current_time - last_cleanup_time >= 30:
                tn, fn, total_cleaned = await storage.cleanup_old_transactions(max_age_seconds=60)
                if total_cleaned > 0:
                    print(f"🧹 Cleaned {total_cleaned:,} expired transactions (TN: {tn:,}, FN: {fn:,})")
                
                cleaned_alerts = await storage.cleanup_old_alerts(max_age_seconds=60)
                if cleaned_alerts > 0:
                    print(f"🧹 Cleaned {cleaned_alerts:,} orphaned alerts")
                
                last_cleanup_time = current_time
            
            # Print status update every 10 seconds
            if current_time - last_report_time >= 10:
                elapsed = current_time - start_time
                
                # Get stats from storage
                storage_stats = storage.get_stats()
                
                total_classified = storage_stats['tp'] + storage_stats['fp'] + storage_stats['tn'] + storage_stats['fn']
                
                if total_classified > 0:
                    accuracy = (storage_stats['tp'] + storage_stats['tn']) / total_classified * 100
                else:
                    accuracy = 0
                
                total_predicted_positive = storage_stats['tp'] + storage_stats['fp']
                if total_predicted_positive > 0:
                    precision = storage_stats['tp'] / total_predicted_positive * 100
                else:
                    precision = 0
                
                total_actual_positive = storage_stats['tp'] + storage_stats['fn']
                if total_actual_positive > 0:
                    recall = storage_stats['tp'] / total_actual_positive * 100
                else:
                    recall = 0
                
                if precision + recall > 0:
                    f1_score = 2 * (precision * recall) / (precision + recall)
                else:
                    f1_score = 0
                
                txn_rate = stats['total_transactions'] / elapsed if elapsed > 0 else 0
                alert_rate = stats['total_alerts'] / elapsed if elapsed > 0 else 0
                lag = stats['total_transactions'] - stats['total_alerts']
                db_size = storage.get_db_size()
                
                print(f"\n📊 METRICS | Txns: {stats['total_transactions']:>7,} | "
                      f"Alerts: {stats['total_alerts']:>5,} | "
                      f"Pending: {storage_stats['pending_txns']:>5,} | Lag: {lag:>6,}")
                print(f"   Txn Rate: {txn_rate:>6.1f} tps | Alert Rate: {alert_rate:>6.1f} aps | "
                      f"DB: {db_size:>6.2f} MB")
                print(f"   Accuracy: {accuracy:>5.2f}% | Precision: {precision:>5.2f}% | "
                      f"Recall: {recall:>5.2f}% | F1: {f1_score:>5.2f}%")
                print(f"   TP: {storage_stats['tp']:>4} | FP: {storage_stats['fp']:>4} | "
                      f"TN: {storage_stats['tn']:>6} | FN: {storage_stats['fn']:>4}\n")
                
                last_report_time = current_time
                
    except KeyboardInterrupt:
        # Final cleanup
        print("\n🧹 Running final cleanup...")
        tn, fn, total_cleaned = await storage.cleanup_old_transactions(max_age_seconds=0)
        print(f"   Processed {total_cleaned:,} remaining transactions")
        
        elapsed = time.time() - start_time
        storage_stats = storage.get_stats()
        
        print()
        print("═══════════════════════════════════════════════════════════")
        print("          FINAL EVALUATION RESULTS")
        print("═══════════════════════════════════════════════════════════")
        print(f"Total Transactions:    {stats['total_transactions']:>10,}")
        print(f"  - Actual Frauds:     {stats['total_frauds']:>10,}")
        print(f"  - Legitimate:        {stats['total_legitimate']:>10,}")
        print(f"Total Alerts:          {stats['total_alerts']:>10,}")
        print(f"Matched Pairs:         {storage_stats['matched']:>10,}")
        print()
        print("Confusion Matrix:")
        print(f"  True Positives:      {storage_stats['tp']:>10,}  (Fraud → Fraud)")
        print(f"  False Positives:     {storage_stats['fp']:>10,}  (Legit → Fraud)")
        print(f"  True Negatives:      {storage_stats['tn']:>10,}  (Legit → Legit)")
        print(f"  False Negatives:     {storage_stats['fn']:>10,}  (Fraud → Legit)")
        print()
        
        # Calculate final metrics
        total_classified = storage_stats['tp'] + storage_stats['fp'] + storage_stats['tn'] + storage_stats['fn']
        
        if total_classified > 0:
            accuracy = (storage_stats['tp'] + storage_stats['tn']) / total_classified * 100
            print(f"Accuracy:              {accuracy:>13.2f}%")
        
        total_predicted_positive = storage_stats['tp'] + storage_stats['fp']
        if total_predicted_positive > 0:
            precision = storage_stats['tp'] / total_predicted_positive * 100
            print(f"Precision:             {precision:>13.2f}%")
        
        total_actual_positive = storage_stats['tp'] + storage_stats['fn']
        if total_actual_positive > 0:
            recall = storage_stats['tp'] / total_actual_positive * 100
            specificity = storage_stats['tn'] / (storage_stats['tn'] + storage_stats['fp']) * 100 if (storage_stats['tn'] + storage_stats['fp']) > 0 else 0
            print(f"Recall (Sensitivity):  {recall:>13.2f}%")
            print(f"Specificity:           {specificity:>13.2f}%")
        
        if total_predicted_positive > 0 and total_actual_positive > 0:
            precision_val = storage_stats['tp'] / total_predicted_positive
            recall_val = storage_stats['tp'] / total_actual_positive
            if precision_val + recall_val > 0:
                f1_score = 2 * (precision_val * recall_val) / (precision_val + recall_val) * 100
                print(f"F1-Score:              {f1_score:>13.2f}%")
        
        print()
        print(f"Processing Rate:       {stats['total_transactions']/elapsed:>10,.1f} txns/sec")
        print(f"Session Duration:      {elapsed:>13.2f}s")
        print(f"Database Size:         {storage.get_db_size():>13.2f} MB")
        print("═══════════════════════════════════════════════════════════")
        print()
        
        # Additional insights
        if stats['total_frauds'] > 0:
            fraud_detection_rate = storage_stats['tp'] / stats['total_frauds'] * 100
            print("📈 Key Insights:")
            print(f"   Fraud Detection Rate: {fraud_detection_rate:.2f}% of all frauds caught")
            
        if stats['total_legitimate'] > 0:
            false_alarm_rate = storage_stats['fp'] / stats['total_legitimate'] * 100
            print(f"   False Alarm Rate: {false_alarm_rate:.2f}% of legitimate flagged")
        print()
        print(f"💾 Data persisted in: fraud_detection.db")
        print(f"   Run again to continue from this point!")
        print()
    
    finally:
        await nc.close()
        storage.close()
        print("✓ Disconnected from NATS")
        print("✓ Database closed")

if __name__ == "__main__":
    asyncio.run(main())