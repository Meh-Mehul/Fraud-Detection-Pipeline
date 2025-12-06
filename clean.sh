#!/bin/bash

echo "🧹 Cleaning up fraud detection pipeline data..."

# Credit Card Fraud (Existing)
rm -rf pathway_persistence/cc_fraud_checkpoints/
rm -f fraud_stream.csv
rm -f publisher/temp_det_stream.csv
rm -f publisher/temp_feed_stream.csv
rm -f fraud_reports/*.pdf
rm -f fraud_reports/*.json

# ATO Fraud (New)
rm -rf pathway_persistence/ato_checkpoints/
rm -f publisher/temp_ato_logins.jsonl
rm -f publisher/temp_ato_profiles.jsonl
rm -f ato_fraud_reports/*.pdf
rm -f ato_fraud_reports/*.json

# Shared
rm -f *.log
rm -f injection_log.json

echo "✓ Cleanup complete!"