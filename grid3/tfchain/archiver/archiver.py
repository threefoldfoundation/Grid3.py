# Compression module for grid3 indexer
# This module provides zstd compression functionality for archive mode

import json
import random
import sqlite3
from typing import Dict, List

import zstd


class Archiver:
    def __init__(self, db_path: str, dict_size: int = 1024):
        """Initialize the archive compressor.

        Args:
            db_path: Path to the SQLite database
            dict_size: Size of the zstd dictionary in bytes (default: 1024)
        """
        self.db_path = db_path
        self.dict_size = dict_size
        self.zstd_dict = None

    def train_compression_dict(
        self, sample_blocks: List[Dict], dict_size: int = None
    ) -> bytes:
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

        # Note: The standard zstd Python module doesn't support dictionary training
        # For now, we'll use a simple approach and just return None
        # In a production environment, you would need zstd with dictionary support
        print(
            "Warning: Dictionary training not supported with this zstd implementation"
        )
        return b""  # Return empty bytes as placeholder

    def _serialize_default(self, obj):
        """Default serialization function for objects that aren't JSON serializable."""
        if hasattr(obj, "serialize"):
            return obj.serialize()
        elif hasattr(obj, "__dict__"):
            return obj.__dict__
        else:
            return str(obj)

    def compress_block_batch(self, blocks: List[Dict]) -> bytes:
        """Compress a batch of blocks using the current dictionary.

        Args:
            blocks: List of block dictionaries to compress

        Returns:
            Compressed bytes
        """
        # Serialize blocks to JSON
        serialized = json.dumps(blocks, default=self._serialize_default).encode()

        # Compress with zstd (without dictionary for now)
        compressed = zstd.compress(serialized)
        return compressed

    def decompress_block_batch(self, compressed_data: bytes) -> List[Dict]:
        """Decompress a batch of blocks.

        Args:
            compressed_data: Compressed block data

        Returns:
            List of decompressed block dictionaries
        """
        # Decompress with zstd
        decompressed = zstd.decompress(compressed_data)

        # Deserialize JSON
        blocks = json.loads(decompressed.decode())
        return blocks

    def load_dict_from_db(self, con: sqlite3.Connection) -> bool:
        """Load compression dictionary from database.

        Args:
            con: SQLite connection

        Returns:
            True if dictionary was loaded, False if not found
        """
        try:
            cursor = con.execute("SELECT value FROM kv WHERE key='zstd_dict'")
            result = cursor.fetchone()
            if result:
                self.zstd_dict = result[0]
                return True
            return False
        except Exception as e:
            print(f"Error loading dictionary from DB: {e}")
            return False

    def save_dict_to_db(self, con: sqlite3.Connection, zstd_dict: bytes):
        """Save compression dictionary to database.

        Args:
            con: SQLite connection
            zstd_dict: Dictionary bytes to save
        """
        try:
            con.execute(
                "INSERT OR REPLACE INTO kv(key, value) VALUES(?, ?)",
                ("zstd_dict", zstd_dict),
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
                block = client.sub.get_block(block_number=block_number)
                # Get events for this block too
                block["events"] = client.sub.get_events(block["header"]["hash"])
                sampled_blocks.append(block)

                if len(sampled_blocks) % 100 == 0:
                    print(f"Sampled {len(sampled_blocks)}/{count} blocks...")

            except Exception as e:
                print(f"Warning: Could not fetch block {block_number}: {e}")
                continue

        print(f"Finished sampling: {len(sampled_blocks)} blocks")
        return sampled_blocks

    def prepare_db_for_archive(self, con: sqlite3.Connection):
        """Prepare database tables for archive mode.

        Args:
            con: SQLite connection
        """

        # Create archive_blocks table for storing compressed block batches
        con.execute("""
        CREATE TABLE IF NOT EXISTS archive_blocks (
            start_block INTEGER PRIMARY KEY,
            end_block INTEGER NOT NULL,
            compressed_data BLOB NOT NULL,
            spec_version INTEGER
        )

        """)

        con.commit()

    def store_block_batch(
        self,
        con: sqlite3.Connection,
        batch_id: int,
        start_block: int,
        end_block: int,
        compressed_data: bytes,
        spec_version: int,
    ):
        """Store a compressed block batch in the database.

        Args:
            con: SQLite connection
            batch_id: Batch ID
            start_block: First block number in batch
            end_block: Last block number in batch
            compressed_data: Compressed block data
            spec_version: Runtime spec version
        """
        con.execute(
            """
            INSERT OR REPLACE INTO archive_blocks
            (batch_id, start_block, end_block, compressed_data, spec_version)
            VALUES (?, ?, ?, ?, ?)
        """,
            (batch_id, start_block, end_block, compressed_data, spec_version),
        )
        con.commit()
