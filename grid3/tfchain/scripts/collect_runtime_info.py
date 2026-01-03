"""
Collect runtime version info from TFChain and find exact upgrade blocks.

Uses coarse sampling followed by binary search to efficiently find
the exact block where each runtime upgrade occurred.
"""

import grid3.tfchain


def get_version_at_block(tfchain, block_num):
    """Get runtime specVersion at a given block number."""
    block_hash = tfchain.sub.get_block_hash(block_num)
    if block_hash is None:
        return None
    return tfchain.sub.get_block_runtime_version(block_hash)["specVersion"]


def binary_search_upgrade(tfchain, low, high, old_version, new_version):
    """
    Binary search to find the exact block where version changed.
    Returns the first block with new_version.
    """
    while low < high:
        mid = (low + high) // 2
        version = get_version_at_block(tfchain, mid)

        if version == old_version:
            low = mid + 1
        else:
            high = mid

    return low


def find_runtime_upgrades(tfchain, coarse_step=100_000):
    """
    Find all runtime upgrade blocks using coarse scan + binary search.

    Returns dict mapping specVersion -> first block with that version
    """
    # Get current chain head
    head = tfchain.sub.get_block_header()
    head_number = head["header"]["number"]

    print(f"Chain head: {head_number}")
    print(f"Coarse scanning every {coarse_step} blocks...")

    # Phase 1: Coarse scan to find approximate upgrade ranges
    version_ranges = []  # [(version, first_seen_block, last_seen_block)]
    last_version = None
    last_block = 0

    for block_num in range(0, head_number + 1, coarse_step):
        try:
            version = get_version_at_block(tfchain, block_num)
            if version is None:
                continue

            if last_version is None:
                last_version = version
                last_block = block_num
            elif version != last_version:
                # Found a version change somewhere between last_block and block_num
                version_ranges.append((last_version, last_block, block_num))
                print(f"  Found v{last_version} -> v{version} between {last_block} and {block_num}")
                last_version = version
                last_block = block_num

        except Exception as e:
            print(f"  Error at block {block_num}: {e}")
            break

    # Add the final version range
    if last_version is not None:
        version_ranges.append((last_version, last_block, head_number))

    print(f"\nFound {len(version_ranges)} version ranges")

    # Phase 2: Binary search to find exact upgrade blocks
    print("\nBinary searching for exact upgrade blocks...")

    upgrades = {}  # specVersion -> first block

    for i, (version, range_start, range_end) in enumerate(version_ranges):
        if i == 0:
            # First version starts at block 0 (or 1)
            upgrades[version] = 0
            print(f"  v{version}: block 0 (genesis)")
        else:
            # Binary search between previous range end and this range start
            prev_version = version_ranges[i - 1][0]
            search_low = version_ranges[i - 1][1]
            search_high = range_start

            exact_block = binary_search_upgrade(
                tfchain, search_low, search_high, prev_version, version
            )
            upgrades[version] = exact_block
            print(f"  v{version}: block {exact_block}")

    return upgrades


def main():
    print("Connecting to TFChain mainnet...")
    tfchain = grid3.tfchain.TFChain()

    upgrades = find_runtime_upgrades(tfchain)

    # Output as a Python data structure
    print("\n" + "=" * 60)
    print("RUNTIME_UPGRADES: dict mapping specVersion -> first block")
    print("=" * 60)
    print("\nRUNTIME_UPGRADES = {")
    for version in sorted(upgrades.keys()):
        print(f"    {version}: {upgrades[version]},")
    print("}")

    # Also output the inverse mapping (useful for lookups)
    print("\n# Sorted list of (block, version) for range lookups")
    print("RUNTIME_UPGRADE_BLOCKS = [")
    for version in sorted(upgrades.keys()):
        print(f"    ({upgrades[version]}, {version}),")
    print("]")


if __name__ == "__main__":
    main()
