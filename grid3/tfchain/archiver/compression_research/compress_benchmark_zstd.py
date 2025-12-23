# Dictionary size: 100.0 KB

# 1 block:
#   Avg original size: 19591 bytes
#   Avg compressed size: 1889 bytes
#   Compression ratio: 90.4%
#   Avg compression time: 780.51 µs

# 10 blocks:
#   Avg original size: 195297 bytes
#   Avg compressed size: 15569 bytes
#   Compression ratio: 92.0%
#   Avg compression time per block: 152.21 µs

# 100 blocks:
#   Original size: 1959078 bytes
#   Avg compressed size: 153853 bytes
#   Compression ratio: 92.1%
#   Avg compression time per block: 56.37 µs

# Dictionary size: 10.0 KB

# 1 block:
#   Avg original size: 16771 bytes
#   Avg compressed size: 1707 bytes
#   Compression ratio: 89.8%
#   Avg compression time: 155.68 µs

# 10 blocks:
#   Avg original size: 160776 bytes
#   Avg compressed size: 15238 bytes
#   Compression ratio: 90.5%
#   Avg compression time per block: 60.97 µs

# 100 blocks:
#   Original size: 1677142 bytes
#   Avg compressed size: 162513 bytes
#   Compression ratio: 90.3%
#   Avg compression time per block: 46.15 µs

# Dictionary size: 1.0 KB

# 1 block:
#   Avg original size: 15194 bytes
#   Avg compressed size: 2279 bytes
#   Compression ratio: 85.0%
#   Avg compression time: 90.51 µs

# 10 blocks:
#   Avg original size: 152235 bytes
#   Avg compressed size: 15646 bytes
#   Compression ratio: 89.7%
#   Avg compression time per block: 39.56 µs

# 100 blocks:
#   Original size: 1519402 bytes
#   Avg compressed size: 149947 bytes
#   Compression ratio: 90.1%
#   Avg compression time per block: 26.64 µs

import json
import time

from compression import zstd

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

# Prepare training data for zstd dictionary
training_data = [
    json.dumps(block, default=lambda x: x.serialize()).encode() for block in blocks
]

# Train zstd dictionary
dict_size = 1 * 1024  # Dictionary size (kilobytes on left of *)
zstd_dict = zstd.train_dict(training_data, dict_size)


# --- HELPER FUNCTIONS ---
def measure_zstd_compress(serialized_data):
    start = time.time()
    compressed = zstd.compress(serialized_data, zstd_dict=zstd_dict)
    elapsed = time.time() - start
    return len(compressed), elapsed


# --- BENCHMARK ---
def run_benchmark():
    print(f"Dictionary size: {dict_size / 1024:.1f} KB")
    # Benchmark for 1 block (different block each iteration)
    print("\n1 block:")
    zstd_sizes_1, zstd_times_1 = [], []
    original_sizes_1 = []
    for i in range(ITERATIONS):
        # Use a different block for each iteration, cycling through available blocks
        block_idx = i % len(blocks)
        data = [blocks[block_idx]]
        serialized_data = json.dumps(data, default=lambda x: x.serialize()).encode()
        original_sizes_1.append(len(serialized_data))
        size, t = measure_zstd_compress(serialized_data)
        zstd_sizes_1.append(size)
        zstd_times_1.append(t)

    avg_original_size_1 = sum(original_sizes_1) / ITERATIONS
    avg_compressed_size_1 = sum(zstd_sizes_1) / ITERATIONS
    compression_percentage_1 = (1 - avg_compressed_size_1 / avg_original_size_1) * 100
    print(f"  Avg original size: {avg_original_size_1:.0f} bytes")
    print(f"  Avg compressed size: {avg_compressed_size_1:.0f} bytes")
    print(f"  Compression ratio: {compression_percentage_1:.1f}%")
    print(f"  Avg compression time: {sum(zstd_times_1) / ITERATIONS * 1e6:.2f} µs")

    # Benchmark for 10 blocks (different set of 10 blocks each iteration)
    print("\n10 blocks:")
    zstd_sizes_10, zstd_times_10, original_sizes_10 = [], [], []
    for i in range(ITERATIONS):
        # Use a different starting point for each set of 10 blocks
        start_idx = i * 10 % (len(blocks) - 9)  # Ensure we have 10 consecutive blocks
        data = blocks[start_idx : start_idx + 10]
        serialized_data = json.dumps(data, default=lambda x: x.serialize()).encode()
        original_size = len(serialized_data)
        original_sizes_10.append(original_size)
        size, t = measure_zstd_compress(serialized_data)
        zstd_sizes_10.append(size)
        zstd_times_10.append(t)

    avg_original_size_10 = sum(original_sizes_10) / ITERATIONS
    avg_compressed_size_10 = sum(zstd_sizes_10) / ITERATIONS
    compression_percentage_10 = (
        1 - avg_compressed_size_10 / avg_original_size_10
    ) * 100
    print(f"  Avg original size: {avg_original_size_10:.0f} bytes")
    print(f"  Avg compressed size: {avg_compressed_size_10:.0f} bytes")
    print(f"  Compression ratio: {compression_percentage_10:.1f}%")
    print(
        f"  Avg compression time per block: {sum(zstd_times_10) / ITERATIONS / 10 * 1e6:.2f} µs"
    )

    # Benchmark for 100 blocks (only one set available, repeated)
    print("\n100 blocks:")
    data = blocks[:100]
    serialized_data = json.dumps(data, default=lambda x: x.serialize()).encode()
    original_size = len(serialized_data)

    zstd_sizes_100, zstd_times_100 = [], []
    for _ in range(ITERATIONS):
        size, t = measure_zstd_compress(serialized_data)
        zstd_sizes_100.append(size)
        zstd_times_100.append(t)

    avg_compressed_size_100 = sum(zstd_sizes_100) / ITERATIONS
    compression_percentage_100 = (1 - avg_compressed_size_100 / original_size) * 100
    print(f"  Original size: {original_size} bytes")
    print(f"  Avg compressed size: {avg_compressed_size_100:.0f} bytes")
    print(f"  Compression ratio: {compression_percentage_100:.1f}%")
    print(
        f"  Avg compression time per block: {sum(zstd_times_100) / ITERATIONS / 100 * 1e6:.2f} µs"
    )


if __name__ == "__main__":
    run_benchmark()
