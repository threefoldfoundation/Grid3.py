import argparse
from grid3 import tfchain


def inspect_events(start_block=None, end_block=None, network="main"):
    """Inspect and print events from a range of blocks"""
    client = tfchain.TFChain(network=network)
    
    # If no start block specified, use a reasonable default
    if start_block is None:
        start_block = client.sub.get_block_number(client.sub.get_chain_head())
        if end_block is None:
            end_block = start_block
        start_block = max(1, start_block - 100)  # Default to last 100 blocks
    
    # If no end block specified, use start block
    if end_block is None:
        end_block = start_block

    print(f"Inspecting events from blocks {start_block} to {end_block} on {network} network...")
    print("=" * 50)

    for block_number in range(start_block, end_block + 1):
        try:
            block = client.sub.get_block(block_number=block_number)
            if block is None:
                print(f"Block {block_number}: None")
                continue

            events = client.sub.get_events(block["header"]["hash"])

            if not events:
                print(f"Block {block_number}: No events")
                continue

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
    parser = argparse.ArgumentParser(description="Inspect events on TFChain")
    parser.add_argument("--network", default="main", help="Network to connect to (main, dev, qa, test)")
    parser.add_argument("--start-block", type=int, help="Starting block number to inspect")
    parser.add_argument("--end-block", type=int, help="Ending block number to inspect")
    parser.add_argument("--stream", action="store_true", help="Stream live events instead of inspecting historical blocks")
    
    args = parser.parse_args()
    
    if args.stream:
        stream_events(args.network)
    else:
        inspect_events(args.start_block, args.end_block, args.network)
