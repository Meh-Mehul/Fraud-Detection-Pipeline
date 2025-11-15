from publisher.pathway_nats_pub import run_publisher
## This file starts the publisher which simultates live transaction stream
if __name__ == "__main__":
    try:
        run_publisher()
    except KeyboardInterrupt:
        print("\n\n✓ Publisher stopped")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure NATS server is running:")
        print("  nats-server")
        import traceback
        traceback.print_exc()