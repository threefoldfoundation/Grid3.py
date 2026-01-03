# TFChain Independent Archiver
"""
This module provides a standalone archiver for TFChain that:
- Archives the full chain starting at any block
- Uses batch-based archiving with compression
- Periodically checks for new blocks
- Stores complete block data including events
- Operates independently from the indexer
- Supports dictionary training for improved compression

Usage:
    python -m grid3.tfchain.archiver  # Run with default settings
    python -m grid3.tfchain.archiver --help  # Show all options
"""

import datetime
import json
import queue as queue_module
import random
import sqlite3
import threading
import time
from multiprocessing import JoinableQueue, Process
from typing import Dict, List, Optional, Tuple

from compression import zstd

from .. import tfchain


class Archiver:
    def __init__(
        self,
        db_path: str = "tfchain_archive.db",
        batch_size: int = 10,
        max_workers: int = 10,
        check_interval: int = 60,
        db_timeout: int = 30,
        dict_size: int = 1024,
        training_blocks: int = 1000,
        tfchain_url: str = "wss://tfchain.grid.tf",
        verbose: bool = False,
    ):
        """Initialize the independent archiver.

        Args:
            db_path: Path to the SQLite database for archive storage
            batch_size: Number of blocks per batch (default: 10)
            max_workers: Maximum number of worker processes
            check_interval: Seconds between checking for new blocks
            db_timeout: SQLite connection timeout in seconds
            dict_size: Size of the zstd dictionary in bytes (default: 1024)
            training_blocks: Number of blocks to sample for dictionary training (default: 1000)
            tfchain_url: URL of the tfchain node to connect to (default: "wss://tfchain.grid.tf")
            verbose: Whether to print verbose output (default: False)
        """
        self.db_path = db_path
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.check_interval = check_interval
        self.db_timeout = db_timeout
        self.dict_size = dict_size
        self.training_blocks = training_blocks
        self.tfchain_url = tfchain_url
        self.verbose = verbose
        self.start_time = time.time()

        # Initialize queues
        # block_queue: Queue for workers (threads in same process)
        self.block_queue = queue_module.Queue()
        # write_queue: Queue for writer process (inter-process)
        self.write_queue = JoinableQueue()
        self.write_queue.cancel_join_thread()

        # State tracking
        self.running = True

        # Initialize database connection and load dictionary
        con = self.new_connection()
        self.prepare_database(con)

        # Check if we need to backfill total_blocks_processed from existing data
        self._backfill_total_blocks(con)

        # Try to load compression dictionary from database
        self.zstd_dict_bytes: Optional[bytes] = self.load_dict_from_db(con)

        # Close the connection as it will be reopened when needed
        con.close()

    def _get_http_url(self) -> str:
        """Convert WebSocket URL to HTTP URL for SubstrateRPC."""
        url = self.tfchain_url
        if url.startswith("wss://"):
            return url.replace("wss://", "https://", 1)
        elif url.startswith("ws://"):
            return url.replace("ws://", "http://", 1)
        return url  # Already HTTP

    def new_connection(self) -> sqlite3.Connection:
        """Create a new database connection."""
        con = sqlite3.connect(self.db_path, timeout=self.db_timeout)
        con.execute("PRAGMA journal_mode=wal")
        return con

    def prepare_database(self, con: sqlite3.Connection):
        """Prepare the database tables for archiving."""
        # Create archive_blocks table for storing compressed block batches
        con.execute("""
        CREATE TABLE IF NOT EXISTS archive_blocks (
            batch_id INTEGER PRIMARY KEY,
            start_block INTEGER NOT NULL,
            end_block INTEGER NOT NULL,
            compressed_data BLOB NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(start_block, end_block)
        )
        """)

        # Create metadata table for tracking progress
        con.execute("""
        CREATE TABLE IF NOT EXISTS archive_metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)

        # Create kv table for dictionary storage
        con.execute("""
        CREATE TABLE IF NOT EXISTS kv (
            key TEXT PRIMARY KEY,
            value BLOB
        )
        """)

        # Initialize metadata if not exists
        con.execute("""
        INSERT OR IGNORE INTO archive_metadata(key, value)
        VALUES('last_archived_block', '0')
        """)

        # Initialize total blocks processed in kv store if not exists
        con.execute(
            """
        INSERT OR IGNORE INTO kv(key, value)
        VALUES('total_blocks_processed', ?)
        """,
            (b"0",),
        )

        # Store batch size as metadata
        con.execute(
            """
        INSERT OR REPLACE INTO archive_metadata(key, value)
        VALUES('batch_size', ?)
        """,
            (str(self.batch_size),),
        )
        con.commit()

    def get_last_archived_block(self, con: sqlite3.Connection) -> int:
        """Get the last archived block number from metadata."""
        result = con.execute(
            "SELECT value FROM archive_metadata WHERE key='last_archived_block'"
        ).fetchone()
        return int(result[0]) if result and result[0] is not None else 0

    def get_batch_size_from_metadata(self, con: sqlite3.Connection) -> int:
        """Get the batch size from metadata."""
        result = con.execute(
            "SELECT value FROM archive_metadata WHERE key='batch_size'"
        ).fetchone()
        return int(result[0]) if result else self.batch_size

    def update_batch_size_in_metadata(self, con: sqlite3.Connection, batch_size: int):
        """Update the batch size in metadata."""
        con.execute(
            "UPDATE archive_metadata SET value=? WHERE key='batch_size'",
            (str(batch_size),),
        )
        con.commit()

    def get_current_batch_size(self, con: sqlite3.Connection) -> int:
        """Get the current batch size from metadata, falling back to instance batch_size if not found."""
        return self.get_batch_size_from_metadata(con)

    def update_last_archived_block(self, con: sqlite3.Connection, block_number: int):
        """Update the last archived block number in metadata."""
        con.execute(
            "UPDATE archive_metadata SET value=? WHERE key='last_archived_block'",
            (str(block_number),),
        )

    def get_total_blocks_processed(self, con: sqlite3.Connection) -> int:
        """Get the total number of blocks processed from kv store."""
        result = con.execute(
            "SELECT value FROM kv WHERE key='total_blocks_processed'"
        ).fetchone()
        return int(result[0]) if result and result[0] is not None else 0

    def update_total_blocks_processed(self, con: sqlite3.Connection, block_count: int):
        """Increment the total blocks processed count."""
        con.execute(
            "UPDATE kv SET value=value+? WHERE key='total_blocks_processed'",
            (block_count,),
        )

    def _backfill_total_blocks(self, con: sqlite3.Connection):
        """Backfill total_blocks_processed from existing archived blocks if needed. This is to support older databases from before block counting was implemented."""
        current_total = self.get_total_blocks_processed(con)
        # Check if there are archived blocks but no count
        if current_total != 0:
            return

        cursor = con.execute("SELECT COUNT(*) FROM archive_blocks").fetchone()

        if cursor and cursor[0] > 0:
            print("Found existing archived blocks but no block count. Backfilling...")

            # Calculate total blocks from archive_blocks table
            cursor = con.execute(
                "SELECT SUM(end_block - start_block + 1) FROM archive_blocks"
            ).fetchone()

            if cursor and cursor[0]:
                total_from_db = cursor[0]
                print(f"Calculated {int(total_from_db)} blocks from archive_blocks")

                # Update the kv store
                with con:
                    con.execute(
                        "UPDATE kv SET value=? WHERE key='total_blocks_processed'",
                        (int(total_from_db),),
                    )
                print("Block count backfilled successfully")
            else:
                print("Warning: Could not calculate block count from archive_blocks")

    def find_missing_batches(self, con: sqlite3.Connection) -> List[Tuple[int, int]]:
        """Find missing batches by comparing expected vs actual batch ranges.

        Returns:
            List of (start_block, end_block) tuples for missing batches
        """
        highest_block = self.get_last_archived_block(con)

        if highest_block == 0:
            return []

        # Get all archived batch ranges from the database
        cursor = con.execute(
            "SELECT start_block, end_block FROM archive_blocks ORDER BY start_block"
        )
        existing_batches = [(start, end) for start, end in cursor]

        # Build a set of all archived batch ranges
        existing_ranges = set()
        for start, end in existing_batches:
            existing_ranges.add((start, end))

        # Generate expected batch ranges from 1 to highest_block
        missing_batches = []
        for start in range(1, highest_block + 1, self.batch_size):
            end = min(start + self.batch_size - 1, highest_block)
            if (start, end) not in existing_ranges:
                missing_batches.append((start, end))

        return missing_batches

    def queue_missing_blocks(self, con: sqlite3.Connection):
        """Find and queue missing blocks for processing."""
        missing_batches = self.find_missing_batches(con)

        if not missing_batches:
            print("No missing batches detected")
            return

        print(
            f"Found {len(missing_batches)} missing batches: {missing_batches[:10]}..."
        )
        if len(missing_batches) > 10:
            print(f"... and {len(missing_batches) - 10} more")

        # Queue missing batches
        for batch_range in missing_batches:
            self.block_queue.put(batch_range)

        print(f"Queued {len(missing_batches)} batches for re-archiving")

    def compress_block_batch(self, blocks: List[Dict]) -> bytes:
        """Compress a batch of blocks using zstd.

        Args:
            blocks: List of block dictionaries to compress

        Returns:
            Compressed bytes
        """
        # Serialize blocks to JSON
        serialized = json.dumps(blocks, default=self._serialize_default).encode()
        # Compress with zstd, using dictionary if available
        if self.zstd_dict_bytes is not None:
            zstd_dict = zstd.ZstdDict(self.zstd_dict_bytes)
            compressed = zstd.compress(serialized, zstd_dict=zstd_dict)
        else:
            compressed = zstd.compress(serialized)
        return compressed

    def decompress_block_batch(self, compressed_data: bytes) -> List[Dict]:
        """Decompress a batch of blocks.

        Args:
            compressed_data: Compressed block data

        Returns:
            List of decompressed block dictionaries
        """
        # Decompress with zstd, using dictionary if available
        if self.zstd_dict_bytes is not None:
            zstd_dict = zstd.ZstdDict(self.zstd_dict_bytes)
            decompressed = zstd.decompress(compressed_data, zstd_dict=zstd_dict)
        else:
            decompressed = zstd.decompress(compressed_data)
        # Deserialize JSON
        blocks = json.loads(decompressed.decode())
        return blocks

    def retrieve_blocks_as_json(self, start_block: int, end_block: int) -> List[Dict]:
        """Retrieve a range of blocks from the archive as JSON.

        Args:
            start_block: Starting block number (inclusive)
            end_block: Ending block number (inclusive)

        Returns:
            List of block dictionaries in the requested range

        Raises:
            ValueError: If start_block > end_block
        """
        if start_block > end_block:
            raise ValueError("start_block must be less than or equal to end_block")

        con = self.new_connection()

        # Query for all batches that overlap with the requested range
        cursor = con.execute(
            """
            SELECT start_block, end_block, compressed_data
            FROM archive_blocks
            WHERE end_block >= ? AND start_block <= ?
            ORDER BY start_block
            """,
            (start_block, end_block),
        )

        blocks = []
        for row in cursor:
            batch_start, batch_end, compressed_data = row
            # Decompress the batch
            batch_blocks = self.decompress_block_batch(compressed_data)

            # Check if the entire batch fits within the requested range
            if batch_start >= start_block and batch_end <= end_block:
                # Entire batch is within range, add all blocks
                blocks.extend(batch_blocks)
            else:
                # Only part of the batch is within range, take a slice
                # Find the slice indices within this batch
                start_idx = max(0, start_block - batch_start)
                end_idx = min(len(batch_blocks) - 1, end_block - batch_start)

                # Add only the blocks in the slice using slice syntax
                blocks.extend(batch_blocks[start_idx : end_idx + 1])

        return blocks

    def _serialize_default(self, obj):
        """Default serialization function for objects that aren't JSON serializable."""
        if hasattr(obj, "serialize"):
            return obj.serialize()
        elif hasattr(obj, "__dict__"):
            return obj.__dict__
        else:
            return str(obj)

    def train_compression_dict(
        self, sample_blocks: List[Dict], dict_size: Optional[int] = None
    ) -> zstd.ZstdDict:
        """Train a zstd compression dictionary from sample blocks.

        Args:
            sample_blocks: List of block dictionaries to use for training
            dict_size: Size of dictionary to train (overrides instance default if provided)

        Returns:
            Trained zstd dictionary bytes
        """
        if dict_size is None:
            dict_size = self.dict_size

        # Serialize blocks to JSON for training
        training_data = []
        for block in sample_blocks:
            try:
                # Handle potential serialization issues
                serialized = json.dumps(block, default=self._serialize_default).encode()
                training_data.append(serialized)
            except Exception as e:
                print(f"Warning: Could not serialize block for training: {e}")
                continue

        if not training_data:
            raise ValueError("No valid training data available")

        # Train the zstd dictionary
        zstd_dict = zstd.train_dict(training_data, dict_size)
        print(f"Trained zstd dictionary with size {len(zstd_dict)} bytes")
        return zstd_dict

    def load_dict_from_db(self, con: sqlite3.Connection) -> Optional[bytes]:
        """Load compression dictionary from database.

        Args:
            con: SQLite connection

        Returns:
            Dictionary bytes length if found, None if not found
        """
        cursor = con.execute("SELECT value FROM kv WHERE key='zstd_dict'")
        result = cursor.fetchone()
        if result:
            self.zstd_dict_bytes = result[0]
            return result[0]
        else:
            return None

    def save_dict_to_db(self, con: sqlite3.Connection, zstd_dict_bytes: bytes):
        """Save compression dictionary to database.

        Args:
            con: SQLite connection
            zstd_dict_bytes: Dictionary bytes to save
        """
        try:
            con.execute(
                "INSERT OR REPLACE INTO kv(key, value) VALUES(?, ?)",
                ("zstd_dict", zstd_dict_bytes),
            )
            con.commit()
        except Exception as e:
            print(f"Error saving dictionary to DB: {e}")
            raise

    def sample_random_blocks(self, client, count: int = 1000) -> List[Dict]:
        """Sample random blocks for dictionary training.

        Args:
            client: TFChain client
            count: Number of blocks to sample

        Returns:
            List of sampled block dictionaries
        """
        # Get current block height
        current_height = client.sub.get_block_header()["header"]["number"]

        # Sample blocks randomly across the chain
        sampled_blocks = []
        max_attempts = count * 2  # Try twice as many times to get valid blocks

        for attempt in range(max_attempts):
            if len(sampled_blocks) >= count:
                break

            block_number = random.randint(1, current_height)

            try:
                block = client.get_block(block_number=block_number)
                # Get events for this block too
                block["events"] = client.get_events(block["header"]["hash"])
                sampled_blocks.append(block)

                if len(sampled_blocks) % 100 == 0:
                    print(f"Sampled {len(sampled_blocks)}/{count} blocks...")

            except Exception as e:
                print(f"Warning: Could not fetch block {block_number}: {e}")
                continue

        print(f"Finished sampling: {len(sampled_blocks)} blocks")
        return sampled_blocks

    def get_block_data(self, client: tfchain.TFChain, block_number: int) -> Dict:
        """Get block data including events and spec version.

        Args:
            client: TFChain client
            block_number: Block number to fetch

        Returns:
            Block dictionary with events and spec_version included
        """
        block = client.get_block(block_number=block_number)
        if block is None:
            raise ValueError(f"Block {block_number} not found")
        block_hash = block["header"]["hash"]
        events = client.get_events(block_hash) or {}
        if type(client.sub.runtime_version) is int:
            spec_version = client.sub.runtime_version
        else:
            raise ValueError(
                f"Invalid runtime version type: {type(client.sub.runtime_version)}"
            )
        block["events"] = events
        block["spec_version"] = spec_version
        return block

    def fetch_block_range(
        self, client: tfchain.TFChain, start_block: int, end_block: int
    ) -> List[Tuple[int, Dict]]:
        """Fetch a range of blocks with their data.

        Args:
            client: TFChain client
            start_block: Starting block number
            end_block: Ending block number

        Returns:
            List of tuples (block_number, block)
        """
        blocks_data = []
        current_block = client.get_block(block_number=end_block)
        if current_block is None:
            raise ValueError(f"Block {end_block} not found")

        while (
            current_block is not None
            and current_block["header"]["number"] >= start_block
        ):
            block_number = current_block["header"]["number"]
            block_hash = current_block["header"]["hash"]
            events = client.get_events(block_hash) or {}
            if type(client.sub.runtime_version) is int:
                spec_version = client.sub.runtime_version
            else:
                raise ValueError(
                    f"Invalid runtime version type: {type(client.sub.runtime_version)}"
                )
            current_block["events"] = events
            current_block["spec_version"] = spec_version
            blocks_data.append((block_number, current_block))

            if block_number == start_block:
                break

            parent_hash = current_block["header"]["parentHash"]
            current_block = client.get_block(block_hash=parent_hash)

        return list(reversed(blocks_data))

    def fetch_block_range_raw(
        self, rpc: tfchain.SubstrateRPC, start_block: int, end_block: int
    ) -> List[Tuple[str, Dict, Optional[str], int]]:
        """Fetch raw block data using parentHash walk (saves RPC calls).

        Args:
            rpc: SubstrateRPC client for raw HTTP requests
            start_block: Starting block number
            end_block: Ending block number

        Returns:
            List of tuples (block_hash, block_data, events_raw, block_number)
        """
        blocks_raw = []

        # Get the end block first (requires hash lookup)
        block_hash = rpc.get_block_hash(end_block)
        if block_hash is None:
            raise ValueError(f"Block {end_block} not found")
        block_data = rpc.get_block(block_hash)

        # Walk backwards via parentHash
        while block_data is not None:
            block_number = int(block_data["block"]["header"]["number"], 16)
            events_raw = rpc.get_events_raw(block_hash)
            blocks_raw.append((block_hash, block_data, events_raw, block_number))

            if block_number == start_block:
                break

            # Get parent block using parentHash (no hash lookup needed)
            block_hash = block_data["block"]["header"]["parentHash"]
            block_data = rpc.get_block(block_hash)

        return list(reversed(blocks_raw))  # Return in ascending order

    def process_block_batch(
        self, blocks_data: List[Tuple[int, Dict]]
    ) -> Optional[Dict]:
        """Process a batch of blocks for archiving.

        Args:
            blocks_data: List of tuples (block_number, block)

        Returns:
            Dictionary containing batch data for storage (serialized but uncompressed)
        """
        if not blocks_data:
            return None

        start_block = blocks_data[0][0]
        end_block = blocks_data[-1][0]

        blocks = [block for _, block in blocks_data]

        # Serialize blocks to JSON (picklable format)
        serialized_blocks = json.dumps(blocks, default=self._serialize_default)

        return {
            "batch_id": start_block // self.batch_size,
            "start_block": start_block,
            "end_block": end_block,
            "blocks": serialized_blocks,
            "block_count": len(blocks),
        }

    def process_block_batch_raw(
        self, blocks_raw: List[Tuple[str, Dict, Optional[str], int]]
    ) -> Optional[Dict]:
        """Process a batch of raw blocks for archiving.

        Args:
            blocks_raw: List of tuples (block_hash, block_data, events_raw, block_number)

        Returns:
            Dictionary containing batch data with raw blocks for decoding in writer process
        """
        if not blocks_raw:
            return None

        start_block = blocks_raw[0][3]
        end_block = blocks_raw[-1][3]

        return {
            "batch_id": start_block // self.batch_size,
            "start_block": start_block,
            "end_block": end_block,
            "blocks_raw": blocks_raw,
            "block_count": len(blocks_raw),
        }

    def archive_batch_worker(self):
        """Worker thread that fetches raw block data for archiving."""
        rpc = None
        max_retries = 3

        while self.running:
            batch_range = self.block_queue.get()
            if batch_range is None:
                return

            start_block, end_block = batch_range

            for attempt in range(max_retries):
                try:
                    if self.verbose:
                        print(f"Fetching batch: blocks {start_block}-{end_block}")

                    # Ensure we have an RPC client
                    if rpc is None:
                        rpc = tfchain.SubstrateRPC(url=self._get_http_url())

                    # Fetch raw block data
                    blocks_raw = self.fetch_block_range_raw(rpc, start_block, end_block)

                    if blocks_raw:
                        # Process the batch (raw data)
                        batch_data = self.process_block_batch_raw(blocks_raw)

                        if batch_data:
                            # Send raw batch to writer for decoding and storage
                            self.write_queue.put(("archive_batch", batch_data))

                    # Success - break retry loop
                    break

                except Exception as e:
                    error_msg = str(e)

                    # Check for connection/RPC errors
                    is_connection_error = (
                        "Connection" in error_msg
                        or "Timeout" in error_msg
                        or "timeout" in error_msg
                        or rpc is None
                    )

                    if is_connection_error and attempt < max_retries - 1:
                        print(
                            f"RPC error detected (attempt {attempt + 1}/{max_retries}): {e}"
                        )
                        print(
                            f"Recreating RPC client for batch {start_block}-{end_block}"
                        )
                        rpc = None
                        # Wait a bit before retrying
                        time.sleep(1)
                    else:
                        # Other error or last retry failed
                        print(f"Error fetching batch {start_block}-{end_block}: {e}")
                        # Re-queue the batch for retry
                        self.block_queue.put(batch_range)
                        break

    @staticmethod
    def db_writer(write_queue, db_path, db_timeout, verbose, dict_bytes, tfchain_url):
        """Database writer process that handles decoding and writing archive batches.

        Args:
            write_queue: Queue for receiving write jobs
            db_path: Path to the database file
            db_timeout: SQLite connection timeout
            verbose: Whether to print verbose output
            dict_bytes: Zstd dictionary bytes for compression
            tfchain_url: HTTP URL for TFChain client (used for decoding)
        """
        con = sqlite3.connect(db_path, timeout=db_timeout)
        con.execute("PRAGMA journal_mode=wal")

        # Reconstruct the zstd dictionary from bytes
        if dict_bytes:
            zstd_dict = zstd.ZstdDict(dict_bytes)
        else:
            zstd_dict = None

        # Create TFChain client for decoding raw blocks
        client = tfchain.TFChain(url=tfchain_url)

        def update_last_archived_block(con, block_number):
            """Update the last archived block number in metadata."""
            con.execute(
                "UPDATE archive_metadata SET value=? WHERE key='last_archived_block'",
                (str(block_number),),
            )

        def update_total_blocks_processed(con, block_count: int):
            """Increment the total blocks processed count."""
            con.execute(
                "UPDATE kv SET value=value+? WHERE key='total_blocks_processed'",
                (block_count,),
            )

        def serialize_default(obj):
            """Default serialization function for objects that aren't JSON serializable."""
            if hasattr(obj, "serialize"):
                return obj.serialize()
            elif hasattr(obj, "__dict__"):
                return obj.__dict__
            else:
                return str(obj)

        def compress_blocks(blocks: list) -> bytes:
            """Serialize and compress a batch of blocks using zstd.

            Args:
                blocks: List of block dictionaries

            Returns:
                Compressed bytes
            """
            # Serialize to JSON
            serialized = json.dumps(blocks, default=serialize_default).encode()
            # Compress with zstd, using dictionary if available
            if zstd_dict is not None:
                compressed = zstd.compress(serialized, zstd_dict=zstd_dict)
            else:
                compressed = zstd.compress(serialized)
            return compressed

        def decode_blocks(blocks_raw: list) -> list:
            """Decode a list of raw blocks.

            Args:
                blocks_raw: List of (block_hash, block_data, events_raw, block_number) tuples

            Returns:
                List of decoded block dictionaries
            """
            decoded_blocks = []
            for block_hash, block_data, events_raw, block_number in blocks_raw:
                try:
                    decoded = client.decode_block_raw(
                        block_hash=block_hash,
                        block_data=block_data,
                        events_raw=events_raw,
                        block_number=block_number,
                    )
                    decoded_blocks.append(decoded)
                except Exception as e:
                    print(f"Warning: Error decoding block {block_number}: {e}")
                    # Store raw data on decode failure
                    block_data["events"] = events_raw
                    block_data["decode_error"] = str(e)
                    decoded_blocks.append(block_data)
            return decoded_blocks

        while True:
            job = write_queue.get()
            if job is None:
                return

            try:
                if job[0] == "archive_batch":
                    batch_data = job[1]

                    with con:
                        # Decode raw blocks
                        decoded_blocks = decode_blocks(batch_data["blocks_raw"])

                        # Compress the decoded blocks
                        compressed_data = compress_blocks(decoded_blocks)

                        # Store the compressed batch
                        con.execute(
                            """
                            INSERT OR REPLACE INTO archive_blocks
                            (batch_id, start_block, end_block, compressed_data)
                            VALUES (?, ?, ?, ?)
                            """,
                            (
                                batch_data["batch_id"],
                                batch_data["start_block"],
                                batch_data["end_block"],
                                compressed_data,
                            ),
                        )

                        # Update metadata
                        update_last_archived_block(con, batch_data["end_block"])
                        update_total_blocks_processed(con, batch_data["block_count"])

                        if verbose:
                            print(
                                f"Archived batch {batch_data['batch_id']}: "
                                f"blocks {batch_data['start_block']}-{batch_data['end_block']} "
                                f"({batch_data['block_count']} blocks)"
                            )

            except Exception as e:
                print(f"Error writing to database: {e}")
                print(f"Failed job: {job}")

            finally:
                write_queue.task_done()

    def _spawn_worker(self) -> threading.Thread:
        """Create and start a new worker thread.

        Returns:
            The started Thread object
        """
        thread = threading.Thread(target=self.archive_batch_worker)
        thread.daemon = True
        thread.start()
        return thread

    def get_current_block_height(self, client: tfchain.TFChain) -> int:
        """Get the current block height from the chain.

        Args:
            client: TFChain client

        Returns:
            Current block height

        Raises:
            Exception: If unable to get current block height
        """
        current_header = client.sub.get_block_header()
        if current_header is None:
            raise ValueError("No block header returned")
        if not isinstance(current_header, dict) or "header" not in current_header:
            raise ValueError("Invalid block header response format")
        return current_header["header"]["number"]

    def queue_new_batches(self, con: sqlite3.Connection, client: tfchain.TFChain):
        """Queue new batches of blocks that need to be archived.

        Args:
            con: Database connection
            client: TFChain client

        Returns:
            Number of batches queued
        """
        try:
            current_height = self.get_current_block_height(client)
        except Exception as e:
            print(f"Error getting current block height, skipping queue: {e}")
            return 0

        last_archived = self.get_last_archived_block(con)

        print(f"Current block height: {current_height}, Last archived: {last_archived}")

        if current_height <= last_archived:
            print("No new blocks to archive")
            return 0

        # Calculate batches to process
        start_block = last_archived + 1
        end_block = min(
            current_height, last_archived + self.batch_size * self.max_workers
        )

        print(f"Queuing blocks {start_block} to {end_block}")

        # Queue batches
        queued_batches = 0
        for batch_start in range(start_block, end_block + 1, self.batch_size):
            batch_end = min(batch_start + self.batch_size - 1, end_block)
            self.block_queue.put((batch_start, batch_end))
            queued_batches += 1

        if self.verbose:
            print(f"Queued {queued_batches} batches for processing")
        return queued_batches

    def archive_from_scratch(self, client: tfchain.TFChain, start_block: int = 0):
        """Archive the chain from scratch starting at a specific block.

        Args:
            client: TFChain client
            start_block: Block number to start from
        """
        con = self.new_connection()
        print(f"Starting archive from scratch from block {start_block}")

        # Reset metadata in a transaction
        with con:
            con.execute(
                "UPDATE archive_metadata SET value=? WHERE key='last_archived_block'",
                (str(start_block - 1),),  # Set to block before start
            )
            con.execute(
                "UPDATE kv SET value=? WHERE key='total_blocks_processed'",
                (b"0",),  # Reset total blocks count
            )

        # Get current height
        try:
            current_height = self.get_current_block_height(client)
        except Exception as e:
            print(f"Error getting current block height: {e}")
            raise

        print(f"Current chain height: {current_height}")
        print(f"Will archive blocks {start_block} to {current_height}")

        # Queue all batches
        batches_queued = 0
        for batch_start in range(start_block, current_height + 1, self.batch_size):
            batch_end = min(batch_start + self.batch_size - 1, current_height)
            self.block_queue.put((batch_start, batch_end))
            batches_queued += 1

        if self.verbose:
            print(f"Queued {batches_queued} batches for initial archive")

    def run(self, start_from_scratch: bool = False, start_block: Optional[int] = None):
        """Run the independent archiver.

        Args:
            start_from_scratch: Whether to start archiving from scratch
            start_block: Specific block to start from (if start_from_scratch=True)
        """
        print("Starting TFChain Independent Archiver")
        print(f"Database: {self.db_path}")
        print(f"Batch size: {self.batch_size}")
        print(f"Max workers: {self.max_workers}")
        print(f"Check interval: {self.check_interval}s")

        # Initialize database
        con = self.new_connection()

        # Check for missing blocks on startup
        self.queue_missing_blocks(con)

        # Check if batch size has changed and update if needed
        stored_batch_size = self.get_batch_size_from_metadata(con)
        if stored_batch_size != self.batch_size:
            print(f"Batch size changed from {stored_batch_size} to {self.batch_size}")
            self.update_batch_size_in_metadata(con, self.batch_size)

        # Initialize TFChain client
        client = tfchain.TFChain(url=self.tfchain_url)

        # We attempt to load the dict during init, if None it wasn't found
        if self.zstd_dict_bytes is None:
            print("No compression dictionary found, training new one...")
            sample_blocks = self.sample_random_blocks(
                client, count=self.training_blocks
            )
            zstd_dict = self.train_compression_dict(sample_blocks, self.dict_size)
            self.zstd_dict_bytes = zstd_dict.dict_content
            self.save_dict_to_db(con, self.zstd_dict_bytes)
            print("Compression dictionary saved to database")

        # Start from scratch if requested
        if start_from_scratch:
            if start_block is None:
                start_block = 0
            self.archive_from_scratch(client, start_block)
        else:
            # Queue any existing blocks that need archiving
            self.queue_new_batches(con, client)

        # Start worker threads
        worker_threads = [self._spawn_worker() for _ in range(self.max_workers)]

        # Start database writer process
        writer_proc = Process(
            target=self.db_writer,
            args=(
                self.write_queue,
                self.db_path,
                self.db_timeout,
                self.verbose,
                self.zstd_dict_bytes,
                self._get_http_url(),
            ),
        )
        writer_proc.daemon = True
        writer_proc.start()

        total_blocks_at_start = self.get_total_blocks_processed(con)

        print(f"Started {len(worker_threads)} worker threads")
        print("Archiver running...")

        try:
            while self.running:
                # Check for new blocks periodically
                time.sleep(self.check_interval)

                # Queue new batches if available
                self.queue_new_batches(con, client)

                # Clean up completed threads
                worker_threads = [t for t in worker_threads if t.is_alive()]

                # Spawn replacement workers if any died
                while len(worker_threads) < self.max_workers:
                    worker_threads.append(self._spawn_worker())
                    print(f"Started replacement worker (total: {len(worker_threads)})")

                # Print status
                queue_size = self.block_queue.qsize()
                write_queue_size = self.write_queue.qsize()
                try:
                    current_height = self.get_current_block_height(client)
                    last_archived = self.get_last_archived_block(con)

                    # Calculate ETA
                    remaining_blocks = current_height - last_archived
                    total_blocks = self.get_total_blocks_processed(con)
                    completed_blocks = total_blocks - total_blocks_at_start
                    elapsed_time = time.time() - self.start_time
                    if completed_blocks > 0 and elapsed_time > 0:
                        blocks_per_second = completed_blocks / elapsed_time
                        if blocks_per_second > 0:
                            eta_seconds = remaining_blocks / blocks_per_second
                            eta_str = str(datetime.timedelta(seconds=int(eta_seconds)))

                        else:
                            eta_str = "calculating..."
                    else:
                        eta_str = "calculating..."

                    print(
                        f"{datetime.datetime.now()} | "
                        f"Queue: {queue_size} | "
                        f"Write Q: {write_queue_size} | "
                        f"Workers: {len(worker_threads)} | "
                        f"Height: {current_height} | "
                        f"Archived: {last_archived} | "
                        f"Total: {self.get_total_blocks_processed(con)} | "
                        f"ETA: {eta_str}"
                    )

                    # If queue is empty and we're caught up, just wait
                    if queue_size == 0 and current_height <= last_archived:
                        print("Caught up with chain, waiting for new blocks...")
                except Exception as e:
                    print(f"Error getting current block height for status: {e}")
                    print(
                        f"{datetime.datetime.now()} | "
                        f"Queue: {queue_size} | "
                        f"Write Q: {write_queue_size} | "
                        f"Workers: {len(worker_threads)} | "
                        f"Height: unknown | "
                        f"Total: {self.get_total_blocks_processed(con)}"
                    )

        except KeyboardInterrupt:
            print("\nShutting down archiver...")
            self.running = False

            # Signal workers to exit
            for _ in range(len(worker_threads)):
                self.block_queue.put(None)
            self.write_queue.put(None)

            # Wait for threads to finish
            for thread in worker_threads:
                thread.join(timeout=30)
            writer_proc.join(timeout=30)
