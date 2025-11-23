from detector.pathway_nats_det import run_detector, ml_state, PERSISTENCE_DIR

## Our prototype implementation of a Fraud detector on pathway
## it is trained online as well
if __name__ == "__main__":
    import os
    import signal
    import sys
    
    def signal_handler(sig, frame):
        print("\n\n═══════════════════════════════════════════════════════════")
        print("    DETECTOR SHUTDOWN - SAVING STATE...")
        print("═══════════════════════════════════════════════════════════")
        
        ml_state.save_models(force=True)
        
        print(f"Total Processed: {ml_state.stats['total']:,}")
        print(f"Unique Transactions: {len(ml_state.processed_transactions):,}")
        print(f"Alerts Generated: {ml_state.stats['alerts']:,}")
        print(f"  - Tier 1: {ml_state.stats['tier1']:,}")
        print(f"  - Tier 2: {ml_state.stats['tier2']:,}")
        print(f"  - Tier 3: {ml_state.stats['tier3']:,}")
        print(f"💾 State saved to: {PERSISTENCE_DIR}/")
        print("═══════════════════════════════════════════════════════════")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        run_detector()
    except KeyboardInterrupt:
        signal_handler(None, None)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        ml_state.save_models(force=True)
        import traceback
        traceback.print_exc()
        print("\nMake sure NATS server is running: nats-server")



# import sys
# from pathlib import Path
# sys.path.append(str(Path(__file__).resolve().parent))
# from detector.detector_ronly import run_detector
# if __name__ == "__main__":
#     run_detector()
