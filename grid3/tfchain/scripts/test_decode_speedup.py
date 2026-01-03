#!/usr/bin/env python3
"""
Test script to measure speedup from runtime version caching in decode_events_raw.

Compares decoding with and without block_number parameter to measure the
benefit of skipping init_runtime when the runtime version is already loaded.
"""

import time
from grid3.tfchain.tfchain import TFChain, SubstrateRPC


def fetch_block_data(rpc: SubstrateRPC, start_block: int, count: int):
    """Fetch block hashes and raw events for a range of blocks."""
    print(f"Fetching {count} blocks starting from {start_block}...")
    blocks = []
    for i in range(count):
        block_num = start_block + i
        block_hash = rpc.get_block_hash(block_num)
        events_raw = rpc.get_events_raw(block_hash)
        blocks.append((block_num, block_hash, events_raw))
        if (i + 1) % 10 == 0:
            print(f"  Fetched {i + 1}/{count}")
    return blocks


def test_decode_without_block_number(tfchain: TFChain, blocks: list):
    """Decode events without block_number (always calls init_runtime)."""
    start = time.perf_counter()
    total_events = 0
    for block_num, block_hash, events_raw in blocks:
        events = tfchain.decode_events_raw(block_hash, events_raw)
        total_events += len(events)
    elapsed = time.perf_counter() - start
    return elapsed, total_events


def test_decode_with_block_number(tfchain: TFChain, blocks: list):
    """Decode events with block_number (skips init_runtime when possible)."""
    start = time.perf_counter()
    total_events = 0
    for block_num, block_hash, events_raw in blocks:
        events = tfchain.decode_events_raw(block_hash, events_raw, block_number=block_num)
        total_events += len(events)
    elapsed = time.perf_counter() - start
    return elapsed, total_events


def main():
    # Configuration
    START_BLOCK = 6_000_000  # Recent blocks (runtime version > 100)
    BLOCK_COUNT = 50

    print("Initializing clients...")
    rpc = SubstrateRPC()

    # Fetch test data first
    blocks = fetch_block_data(rpc, START_BLOCK, BLOCK_COUNT)
    rpc.close()

    print(f"\nTesting decode performance on {BLOCK_COUNT} blocks")
    print("=" * 60)

    # Test WITHOUT block_number (baseline)
    print("\n1. Testing WITHOUT block_number (always init_runtime)...")
    tfchain1 = TFChain(use_http=True)
    # Prime the runtime cache with first block
    tfchain1.decode_events_raw(blocks[0][1], blocks[0][2])
    time_without, events_without = test_decode_without_block_number(tfchain1, blocks)
    print(f"   Time: {time_without:.3f}s")
    print(f"   Events decoded: {events_without}")
    print(f"   Blocks/sec: {BLOCK_COUNT / time_without:.1f}")

    # Test WITH block_number (optimized)
    print("\n2. Testing WITH block_number (skip init when possible)...")
    tfchain2 = TFChain(use_http=True)
    # Refresh runtime tip so we can use the fast path
    tip_block, tip_version = tfchain2.refresh_runtime_tip()
    print(f"   Runtime tip: block {tip_block}, version {tip_version}")
    # Prime the runtime cache with first block
    tfchain2.decode_events_raw(blocks[0][1], blocks[0][2], block_number=blocks[0][0])
    time_with, events_with = test_decode_with_block_number(tfchain2, blocks)
    print(f"   Time: {time_with:.3f}s")
    print(f"   Events decoded: {events_with}")
    print(f"   Blocks/sec: {BLOCK_COUNT / time_with:.1f}")

    # Results
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    speedup = time_without / time_with if time_with > 0 else float('inf')
    print(f"Speedup: {speedup:.2f}x")
    print(f"Time saved: {time_without - time_with:.3f}s ({(1 - time_with/time_without) * 100:.1f}%)")

    # Verify correctness
    if events_without == events_with:
        print(f"\n✓ Both methods decoded the same number of events ({events_without})")
    else:
        print(f"\n✗ Event count mismatch! {events_without} vs {events_with}")


if __name__ == "__main__":
    main()
