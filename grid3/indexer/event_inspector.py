from grid3 import tfchain


def stream_events(network="main"):
    """Stream and print events from incoming blocks"""
    client = tfchain.TFChain(network=network)

    def callback(head, update_nr, subscription_id):
        block_number = head["header"]["number"]
        try:
            block = client.sub.get_block(block_number=block_number)
            if block is None:
                print(f"Block {block_number}: None")
                return

            events = client.sub.get_events(block["header"]["hash"])

            if not events:
                print(f"Block {block_number}: No events")
                return

            print(f"Block {block_number}:")
            for i, event in enumerate(events):
                event_data = event.value
                event_id = event_data.get("event_id", "Unknown")
                module_id = event_data.get("module_id", "Unknown")
                attributes = event_data.get("attributes", {})

                print(f"  Event {i}: {module_id}::{event_id}")
                print(f"    Attributes: {attributes}")

            print("-" * 30)

        except Exception as e:
            print(f"Error processing block {block_number}: {e}")

    print(f"Streaming events from {network} network...")
    print("=" * 50)

    client.sub.subscribe_block_headers(callback)


if __name__ == "__main__":
    stream_events()
