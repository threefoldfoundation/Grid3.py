import argparse
import time
from .. import tfchain

def inspect_events(start_block, end_block, network="main"):
    """Inspect and print all events in a range of blocks"""
    client = tfchain.TFChain(network=network)
    
    print(f"Inspecting events from block {start_block} to {end_block}")
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
            time.sleep(0.1)  # Be nice to the node
            
        except Exception as e:
            print(f"Error processing block {block_number}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect TFChain events")
    parser.add_argument("--start-block", type=int, required=True, help="Start block number")
    parser.add_argument("--end-block", type=int, required=True, help="End block number")
    parser.add_argument("--network", type=str, default="main", help="Network (main, dev, test, qa)")
    
    args = parser.parse_args()
    
    inspect_events(args.start_block, args.end_block, args.network)
