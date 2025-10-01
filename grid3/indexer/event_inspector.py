import argparse
from grid3 import tfchain


def inspect_events(start_block=None, end_block=None, network="main", count=100, filters=None):
    """Inspect and print events from a range of blocks going backwards from the latest block"""
    client = tfchain.TFChain(network=network)
    
    # Get the latest block
    chain_head = client.sub.get_chain_head()
    latest_block = client.sub.get_block(block_hash=chain_head)
    latest_block_number = latest_block["header"]["number"]
    
    # If no start block specified, use the latest block
    if start_block is None:
        start_block = latest_block_number
    
    # If no end block specified, calculate it based on count
    if end_block is None:
        end_block = max(1, start_block - count + 1)
    
    # Ensure start_block is not less than end_block
    if start_block < end_block:
        start_block, end_block = end_block, start_block

    print(f"Inspecting events from blocks {start_block} down to {end_block} on {network} network...")
    if filters:
        print(f"Filters: {', '.join(filters)}")
    print("=" * 50)

    current_block_hash = latest_block["header"]["hash"]
    blocks_processed = 0
    
    # Traverse backwards through the chain
    while blocks_processed < count:
        try:
            block = client.sub.get_block(block_hash=current_block_hash)
            if block is None:
                print(f"Block with hash {current_block_hash}: None")
                break

            block_number = block["header"]["number"]
            
            # Stop if we've reached our target end block
            if block_number < end_block:
                break

            events = client.sub.get_events(block["header"]["hash"])

            if not events:
                print(f"Block {block_number}: No events")
            else:
                # Filter events if filters are provided
                if filters:
                    filtered_events = []
                    for event in events:
                        event_data = event.value
                        event_id = event_data.get("event_id", "Unknown")
                        module_id = event_data.get("module_id", "Unknown")
                        event_type = f"{module_id}::{event_id}"
                        
                        # Check if any filter string matches the event type
                        if any(filter_str in event_type for filter_str in filters):
                            filtered_events.append(event)
                    events = filtered_events

                # Print events if we have any (either no filters or filters matched)
                if events:
                    print(f"Block {block_number}:")
                    for i, event in enumerate(events):
                        event_data = event.value
                        event_id = event_data.get("event_id", "Unknown")
                        module_id = event_data.get("module_id", "Unknown")
                        attributes = event_data.get("attributes", {})

                        print(f"  Event {i}: {module_id}::{event_id}")
                        print(f"    Attributes: {attributes}")

                    print("-" * 30)

            # Move to parent block
            parent_hash = block["header"]["parentHash"]
            if parent_hash == '0x0000000000000000000000000000000000000000000000000000000000000000':
                print("Reached genesis block")
                break
                
            current_block_hash = parent_hash
            blocks_processed += 1

        except Exception as e:
            print(f"Error processing block: {e}")
            break


def stream_events(network="main", filters=None):
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

            # Filter events if filters are provided
            if filters:
                filtered_events = []
                for event in events:
                    event_data = event.value
                    event_id = event_data.get("event_id", "Unknown")
                    module_id = event_data.get("module_id", "Unknown")
                    event_type = f"{module_id}::{event_id}"
                    
                    # Check if any filter string matches the event type
                    if any(filter_str in event_type for filter_str in filters):
                        filtered_events.append(event)
                events = filtered_events

            # Print events if we have any (either no filters or filters matched)
            if events:
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
    if filters:
        print(f"Filters: {', '.join(filters)}")
    print("=" * 50)

    client.sub.subscribe_block_headers(callback)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect events on TFChain")
    parser.add_argument("--network", default="main", help="Network to connect to (main, dev, qa, test)")
    parser.add_argument("--start-block", type=int, help="Starting block number to inspect")
    parser.add_argument("--end-block", type=int, help="Ending block number to inspect")
    parser.add_argument("--count", type=int, default=100, help="Number of blocks to inspect (default: 100)")
    parser.add_argument("--stream", action="store_true", help="Stream live events instead of inspecting historical blocks")
    parser.add_argument("--filter", nargs="+", help="Filter events by type (e.g., 'SmartContract::ContractCreated')")
    
    args = parser.parse_args()
    
    if args.stream:
        stream_events(args.network, args.filter)
    else:
        inspect_events(args.start_block, args.end_block, args.network, args.count, args.filter)
