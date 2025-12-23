"""
Compression Statistics Calculator
This script crawls through all blocks in an archive and calculates the total compression ratio.
It uses the existing Archiver class and methods for database access and decompression.
"""

import argparse
import json

from grid3.tfchain.archiver import Archiver


def calculate_compression_stats(archiver: Archiver) -> dict:
    """Calculate compression statistics for all archived blocks.

    Args:
        archiver: An instance of the Archiver class

    Returns:
        Dictionary containing compression statistics
    """
    con = archiver.new_connection()

    # Query all batches from the archive
    cursor = con.execute("""
        SELECT start_block, end_block, compressed_data
        FROM archive_blocks
        ORDER BY start_block
    """)

    total_compressed_size = 0
    total_uncompressed_size = 0
    total_blocks = 0
    batch_count = 0

    print("Scanning archive batches...")
    for row in cursor:
        batch_start, batch_end, compressed_data = row

        # Count compressed size
        compressed_size = len(compressed_data)
        total_compressed_size += compressed_size

        # Decompress to get uncompressed size
        blocks = archiver.decompress_block_batch(compressed_data)
        uncompressed_size = sum(len(json.dumps(block, default=archiver._serialize_default).encode()) for block in blocks)
        total_uncompressed_size += uncompressed_size

        block_count = len(blocks)
        total_blocks += block_count
        batch_count += 1

        print(
            f"Batch {batch_start}-{batch_end}: "
            f"{block_count} blocks, "
            f"compressed: {compressed_size:,} bytes, "
            f"uncompressed: {uncompressed_size:,} bytes, "
            f"ratio: {uncompressed_size / compressed_size:.2f}x"
        )

    con.close()

    # Calculate overall compression ratio
    if total_compressed_size > 0:
        compression_ratio = total_uncompressed_size / total_compressed_size
        space_saving = (1 - total_compressed_size / total_uncompressed_size) * 100
    else:
        compression_ratio = 0
        space_saving = 0

    return {
        "total_batches": batch_count,
        "total_blocks": total_blocks,
        "total_compressed_size": total_compressed_size,
        "total_uncompressed_size": total_uncompressed_size,
        "compression_ratio": compression_ratio,
        "space_saving_percent": space_saving,
    }


def format_size(size_bytes: int) -> str:
    """Format byte size to human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def main():
    """Main entry point for the compression stats calculator."""
    parser = argparse.ArgumentParser(
        description="Calculate compression statistics for a TFChain archive"
    )
    parser.add_argument(
        "-f",
        "--file",
        help="Specify the database file name.",
        type=str,
        default="tfchain_archive.db",
    )

    args = parser.parse_args()

    print(f"Opening archive: {args.file}")

    # Create an Archiver instance to access the database and methods
    archiver = Archiver(db_path=args.file)

    # Calculate compression statistics
    stats = calculate_compression_stats(archiver)

    # Print summary
    print("\n" + "=" * 60)
    print("COMPRESSION STATISTICS SUMMARY")
    print("=" * 60)
    print(f"Total batches:        {stats['total_batches']:,}")
    print(f"Total blocks:         {stats['total_blocks']:,}")
    print(f"Compressed size:      {format_size(stats['total_compressed_size'])}")
    print(f"Uncompressed size:    {format_size(stats['total_uncompressed_size'])}")
    print(f"Compression ratio:    {stats['compression_ratio']:.2f}x")
    print(f"Space saved:          {stats['space_saving_percent']:.2f}%")
    print("=" * 60)


if __name__ == "__main__":
    main()
