"""
Independent TFChain Archiver
This archiver's sole purpose is to archive the full chain starting at block 0
and continue archiving new blocks as they become available.
Key features:
- Simple batch-based archiving (batches of 10 blocks)
- Periodic checking for new blocks (no active subscription)
- Compressed storage of complete block data including events
- Independent operation from the indexer
"""

import argparse

from .archiver import Archiver


def main():
    """Main entry point for the archiver module."""
    parser = argparse.ArgumentParser(description="TFChain Independent Archiver")
    parser.add_argument(
        "-f",
        "--file",
        help="Specify the database file name.",
        type=str,
        default="tfchain_archive.db",
    )
    parser.add_argument(
        "-b",
        "--batch-size",
        help="Number of blocks per batch",
        type=int,
        default=10,
    )
    parser.add_argument(
        "-w",
        "--max-workers",
        help="Maximum number of worker processes",
        type=int,
        default=10,
    )
    parser.add_argument(
        "-i",
        "--check-interval",
        help="Seconds between checking for new blocks",
        type=int,
        default=60,
    )
    parser.add_argument(
        "-s",
        "--start-from-scratch",
        help="Reset archive and start fresh from the specified block (or block 0 if not specified)",
        action="store_true",
    )
    parser.add_argument(
        "--start-block",
        help="Block number to start archiving from (requires --start-from-scratch)",
        type=int,
    )
    parser.add_argument(
        "--training-blocks",
        help="Number of blocks to sample for compression dictionary training",
        type=int,
        default=1000,
    )
    parser.add_argument(
        "--tfchain-url",
        help="TFChain WebSocket URL (e.g., wss://tfchain.grid.tf)",
        type=str,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Enable verbose batch-level logging",
        action="store_true",
    )

    args = parser.parse_args()

    # Create an instance of the Archiver class
    archiver = Archiver(
        db_path=args.file,
        batch_size=args.batch_size,
        max_workers=args.max_workers,
        check_interval=args.check_interval,
        training_blocks=args.training_blocks,
        tfchain_url=args.tfchain_url,
        verbose=args.verbose,
    )

    # Run the archiver
    archiver.run(
        start_from_scratch=args.start_from_scratch,
        start_block=args.start_block,
    )


if __name__ == "__main__":
    main()
