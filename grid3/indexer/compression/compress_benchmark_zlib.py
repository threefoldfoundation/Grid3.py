# The idea here is to see how compression performance, both in terms of speed and size reduction is impacted by batching blocks together. We compare the average of storing each block separately to an average of sets of ten (using all adjacent sets) and finally to the full set of 100.
#
# A sample result is shown below.
#
# 1 block:
#   Avg original size: 18052 bytes
#   Avg compressed size: 2885 bytes
#   Compression ratio: 84.0%
#   Avg compression time: 255.95 µs

# 10 blocks:
#   Avg original size: 183974 bytes
#   Avg compressed size: 20509 bytes
#   Compression ratio: 88.9%
#   Avg compression time per block: 265.87 µs

# 100 blocks:
#   Original size: 1805217 bytes
#   Avg compressed size: 192294 bytes
#   Compression ratio: 89.3%
#   Avg compression time per block: 208.79 µs


import json
import time
import zlib

import grid3.tfchain

# Fetch 100 blocks
blocks = []
tfchain = grid3.tfchain.TFChain()
head_number = tfchain.sub.get_block_header()["header"]["number"]
for i in range(100):
    block_number = head_number - i
    block = tfchain.sub.get_block(block_number=block_number)
    block["events"] = tfchain.sub.get_events(block["header"]["hash"])
    blocks.append(block)

ITERATIONS = 100  # Some code below assumes size 100


# --- HELPER FUNCTIONS ---
def measure_zlib_compress(serialized_data):
    start = time.time()
    compressed = zlib.compress(serialized_data)
    elapsed = time.time() - start
    return len(compressed), elapsed


# --- BENCHMARK ---
def run_benchmark():
    # Benchmark for 1 block (different block each iteration)
    print("\n1 block:")
    zlib_sizes_1, zlib_times_1 = [], []
    original_sizes_1 = []
    for i in range(ITERATIONS):
        # Use a different block for each iteration, cycling through available blocks
        block_idx = i % len(blocks)
        data = [blocks[block_idx]]
        serialized_data = json.dumps(data, default=lambda x: x.serialize()).encode()
        original_sizes_1.append(len(serialized_data))
        size, t = measure_zlib_compress(serialized_data)
        zlib_sizes_1.append(size)
        zlib_times_1.append(t)

    avg_original_size_1 = sum(original_sizes_1) / ITERATIONS
    avg_compressed_size_1 = sum(zlib_sizes_1) / ITERATIONS
    compression_percentage_1 = (1 - avg_compressed_size_1 / avg_original_size_1) * 100
    print(f"  Avg original size: {avg_original_size_1:.0f} bytes")
    print(f"  Avg compressed size: {avg_compressed_size_1:.0f} bytes")
    print(f"  Compression ratio: {compression_percentage_1:.1f}%")
    print(f"  Avg compression time: {sum(zlib_times_1) / ITERATIONS * 1e6:.2f} µs")

    # Benchmark for 10 blocks (different set of 10 blocks each iteration)
    print("\n10 blocks:")
    zlib_sizes_10, zlib_times_10, original_sizes_10 = [], [], []
    for i in range(ITERATIONS):
        # Use a different starting point for each set of 10 blocks
        start_idx = i * 10 % (len(blocks) - 9)  # Ensure we have 10 consecutive blocks
        data = blocks[start_idx : start_idx + 10]
        serialized_data = json.dumps(data, default=lambda x: x.serialize()).encode()
        original_size = len(serialized_data)
        original_sizes_10.append(original_size)
        size, t = measure_zlib_compress(serialized_data)
        zlib_sizes_10.append(size)
        zlib_times_10.append(t)

    avg_original_size_10 = sum(original_sizes_10) / ITERATIONS
    avg_compressed_size_10 = sum(zlib_sizes_10) / ITERATIONS
    compression_percentage_10 = (
        1 - avg_compressed_size_10 / avg_original_size_10
    ) * 100
    print(f"  Avg original size: {avg_original_size_10:.0f} bytes")
    print(f"  Avg compressed size: {avg_compressed_size_10:.0f} bytes")
    print(f"  Compression ratio: {compression_percentage_10:.1f}%")
    print(
        f"  Avg compression time per block: {sum(zlib_times_10) / ITERATIONS / 10 * 1e6:.2f} µs"
    )

    # Benchmark for 100 blocks (only one set available, repeated)
    print("\n100 blocks:")
    data = blocks[:100]
    serialized_data = json.dumps(data, default=lambda x: x.serialize()).encode()
    original_size = len(serialized_data)

    zlib_sizes_100, zlib_times_100 = [], []
    for _ in range(ITERATIONS):
        size, t = measure_zlib_compress(serialized_data)
        zlib_sizes_100.append(size)
        zlib_times_100.append(t)

    avg_compressed_size_100 = sum(zlib_sizes_100) / ITERATIONS
    compression_percentage_100 = (1 - avg_compressed_size_100 / original_size) * 100
    print(f"  Original size: {original_size} bytes")
    print(f"  Avg compressed size: {avg_compressed_size_100:.0f} bytes")
    print(f"  Compression ratio: {compression_percentage_100:.1f}%")
    print(
        f"  Avg compression time per block: {sum(zlib_times_100) / ITERATIONS / 100 * 1e6:.2f} µs"
    )


if __name__ == "__main__":
    run_benchmark()
