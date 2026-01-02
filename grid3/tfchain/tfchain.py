import bisect
import importlib.resources
import json
from typing import Any, Dict, List, Optional, Tuple

import requests
import substrateinterface
from scalecodec.base import ScaleBytes
from substrateinterface.exceptions import SubstrateRequestException

from .data.runtime_upgrades import RUNTIME_DATA_BLOCK_TIP, RUNTIME_UPGRADES

BLOCK_TIME_SECONDS = 6
# Runtime versions larger than this support automatic type info retrieval
AUTO_TYPES_CUTOFF = 100


class TFChain:
    def __init__(self, network="main", session=None, use_http=False):
        if session is not None and not use_http:
            raise ValueError("session parameter is only allowed when use_http=True")

        if use_http:
            if network == "main":
                url = "https://tfchain.grid.tf"
            else:
                url = "https://tfchain.{}.grid.tf".format(network)
        else:
            if network == "main":
                url = "wss://tfchain.grid.tf"
            else:
                url = "wss://tfchain.{}.grid.tf".format(network)

        # Load the type registry from package data. We don't add this to our
        # substrate client yet, because it doesn't have the ability to apply
        # different registries for different chain versions. We'll have to do
        # that ourselves by wrapping the relevant functions. For now it's just
        # "get_block" and "get_events". Note that mixing the use of these with
        # methods from the substrate client can cause unexpected behavior.
        with importlib.resources.open_text(__name__, "data/tfchain_types.json") as file:
            self.types = json.load(file)

        self.sub = substrateinterface.SubstrateInterface(
            url=url,
            ss58_format=42,
            type_registry_preset="polkadot",
        )

        # Override the session if provided (for HTTP connection pooling)
        if session is not None:
            self.sub.session = session

        self.keys = None

        self._runtime_upgrades = RUNTIME_UPGRADES
        self._runtime_upgrade_blocks = [b for b, _ in self._runtime_upgrades]
        # Known chain tip and its version (updated by refresh_runtime_tip)
        self._runtime_known_tip: Optional[int] = RUNTIME_DATA_BLOCK_TIP
        self._runtime_tip_version: Optional[int] = self._runtime_upgrades[-1][1]

    def get_runtime_version(self, block_number: int) -> Optional[int]:
        """
        Get the runtime spec version for a given block number.

        Uses pre-collected upgrade block data for O(log n) lookup. For blocks
        beyond the static data, uses the dynamically fetched tip version if
        available and the runtime hasn't changed.

        Args:
            block_number: The block number to look up

        Returns:
            The spec version for that block, or None if block is beyond known data
        """
        if block_number < 0:
            return None

        # Check if beyond static data but within dynamically known tip
        last_static_block = self._runtime_upgrades[-1][0]
        if block_number > last_static_block:
            if (
                self._runtime_known_tip is not None
                and block_number <= self._runtime_known_tip
                and self._runtime_tip_version == self._runtime_upgrades[-1][1]
            ):
                # Tip version matches last known upgrade, so no new upgrades
                return self._runtime_tip_version
            return None

        # Binary search for the version
        idx = bisect.bisect_right(self._runtime_upgrade_blocks, block_number) - 1
        if idx < 0:
            return None
        return self._runtime_upgrades[idx][1]

    def refresh_runtime_tip(self) -> Tuple[int, int]:
        """
        Query the current chain tip and its runtime version, storing it for
        use by get_runtime_version.

        If the tip version matches the last known upgrade version, blocks up to
        the new tip can be resolved without RPC calls.

        If a new runtime version is detected, you should re-run the collection
        script to update the static data.

        Returns:
            Tuple of (tip_block_number, spec_version)
        """
        header = self.sub.get_block_header()
        tip_number = header["header"]["number"]
        tip_hash = header["header"]["hash"]
        version = self.sub.get_block_runtime_version(tip_hash)["specVersion"]

        self._runtime_known_tip = tip_number
        self._runtime_tip_version = version

        return tip_number, version

    def get_block(self, block_hash=None, block_number=None):
        if block_hash and block_number:
            raise ValueError("Either block_hash or block_number should be be set")

        if block_number is not None:
            block_hash = self.sub.get_block_hash(block_number)

            if block_hash is None:
                return

        self.setup_runtime_types(block_hash)
        return self.sub.get_block(block_hash)

    def get_events(self, block_hash):
        self.setup_runtime_types(block_hash)
        return self.sub.get_events(block_hash)

    def decode_events_raw(self, block_hash, events_raw, block_number=None):
        """
        Decode raw SCALE-encoded events for a specific block.

        This implements the same flow as the decode steps from
        SubstrateInterface.get_events. The main idea here is to avoid RPC calls
        when possible by looking up the runtime version by block number and
        skipping runtime init when the correct runtime is already loaded.

        Args:
            block_hash: Block hash for runtime context
            events_raw: Raw SCALE-encoded events as hex string
            block_number: Optional block number for fast runtime version lookup

        Returns:
            List of decoded events
        """
        if not events_raw:
            return []

        # Check if we can skip runtime initialization using block number lookup
        skip_init = False
        runtime_version = None
        if block_number is not None:
            # Runtime for a block is determined by the parent block (except genesis)
            parent_number = block_number - 1 if block_number > 0 else 0
            runtime_version = self.get_runtime_version(parent_number)

            if (
                runtime_version is not None
                and runtime_version == self.sub.runtime_version
            ):
                skip_init = True

        if not skip_init:
            # Setup custom types for old runtimes (spec version <= 100)
            # Must be called BEFORE init_runtime, which loads these types
            # Pass runtime_version to avoid RPC call when available
            self.setup_runtime_types(block_hash, spec_version=runtime_version)
            self.sub.init_runtime(block_hash=block_hash)

        # Get type from metadata (e.g. "scale_info::18") instead of hardcoding
        pallet = self.sub.metadata.get_metadata_pallet("System")
        storage = pallet.get_storage_function("Events")
        events_type = storage.get_value_type_string()

        if type(events_raw) == str:
            events_raw = ScaleBytes(events_raw)

        events_obj = self.sub.runtime_config.create_scale_object(
            type_string=events_type, data=events_raw, metadata=self.sub.metadata
        )

        events_obj.decode(check_remaining=self.sub.config.get("strict_scale_decode"))

        # Return list of event values (same format as get_events)
        return [e.value for e in events_obj.elements]

    def decode_extrinsics_raw(
        self, block_hash: str, extrinsics: List[str], block_number: int = None
    ) -> List[Any]:
        """
        Decode raw SCALE-encoded extrinsics for a specific block.

        This follows the same RPC avoidance pattern as decode_events_raw:
        - Look up runtime version by block number (O(log n), no RPC)
        - Skip runtime init when correct runtime is already loaded
        - Cache custom types for old runtimes (spec version <= 100)

        Args:
            block_hash: Block hash for runtime context
            extrinsics: List of raw SCALE-encoded extrinsic hex strings
            block_number: Optional block number for fast runtime version lookup

        Returns:
            List of decoded extrinsic values (or raw hex on decode failure)
        """
        if not extrinsics:
            return []

        # Check if we can skip runtime initialization using block number lookup
        skip_init = False
        runtime_version = None
        if block_number is not None:
            # Runtime for a block is determined by the parent block (except genesis)
            parent_number = block_number - 1 if block_number > 0 else 0
            runtime_version = self.get_runtime_version(parent_number)

            if (
                runtime_version is not None
                and runtime_version == self.sub.runtime_version
            ):
                skip_init = True

        if not skip_init:
            # Setup custom types for old runtimes (spec version <= 100)
            # Must be called BEFORE init_runtime, which loads these types
            # Pass runtime_version to avoid RPC call when available
            self.setup_runtime_types(block_hash, spec_version=runtime_version)
            self.sub.init_runtime(block_hash=block_hash)

        # Get Extrinsic decoder class from runtime config
        extrinsic_cls = self.sub.runtime_config.get_decoder_class("Extrinsic")

        decoded = []
        for ext_data in extrinsics:
            try:
                ext_decoder = extrinsic_cls(
                    data=ScaleBytes(ext_data),
                    metadata=self.sub.metadata,
                    runtime_config=self.sub.runtime_config,
                )
                ext_decoder.decode()
                decoded.append(ext_decoder.value)
            except Exception:
                # Keep raw hex on decode failure
                decoded.append(ext_data)

        return decoded

    def decode_block_raw(
        self,
        block_hash: str,
        block_data: Dict[str, Any],
        events_raw: str,
        block_number: int = None,
    ) -> Dict[str, Any]:
        """
        Decode an entire block (extrinsics + events) without extra RPC calls.

        This is the main entry point for decoding raw block data. It decodes both
        extrinsics and events in a single runtime context, avoiding redundant
        runtime initialization.

        Args:
            block_hash: Block hash for runtime context
            block_data: Raw block data from chain_getBlock RPC call
            events_raw: Raw SCALE-encoded events hex string
            block_number: Optional block number for fast runtime version lookup

        Returns:
            Block data dict with decoded extrinsics and events added
        """
        # Check if we can skip runtime initialization using block number lookup
        skip_init = False
        runtime_version = None
        if block_number is not None:
            # Runtime for a block is determined by the parent block (except genesis)
            parent_number = block_number - 1 if block_number > 0 else 0
            runtime_version = self.get_runtime_version(parent_number)

            if (
                runtime_version is not None
                and runtime_version == self.sub.runtime_version
            ):
                skip_init = True

        if not skip_init:
            # Setup custom types for old runtimes (spec version <= 100)
            # Must be called BEFORE init_runtime, which loads these types
            # Pass runtime_version to avoid RPC call when available
            self.setup_runtime_types(block_hash, spec_version=runtime_version)
            self.sub.init_runtime(block_hash=block_hash)

        # Convert block number from hex if needed
        header = block_data.get("block", {}).get("header", {})
        header_number = header.get("number")
        if isinstance(header_number, str):
            header["number"] = int(header_number, 16)

        # Add block hash to header
        header["hash"] = block_hash

        # Decode events
        events = []
        if events_raw:
            pallet = self.sub.metadata.get_metadata_pallet("System")
            storage = pallet.get_storage_function("Events")
            events_type = storage.get_value_type_string()

            events_obj = self.sub.runtime_config.create_scale_object(
                type_string=events_type,
                data=ScaleBytes(events_raw),
                metadata=self.sub.metadata,
            )
            events_obj.decode(
                check_remaining=self.sub.config.get("strict_scale_decode")
            )
            events = [e.value for e in events_obj.elements]

        block_data["events"] = events

        # Decode extrinsics
        raw_extrinsics = block_data.get("block", {}).get("extrinsics", [])
        if raw_extrinsics:
            extrinsic_cls = self.sub.runtime_config.get_decoder_class("Extrinsic")
            decoded_extrinsics = []

            for ext_data in raw_extrinsics:
                try:
                    ext_decoder = extrinsic_cls(
                        data=ScaleBytes(ext_data),
                        metadata=self.sub.metadata,
                        runtime_config=self.sub.runtime_config,
                    )
                    ext_decoder.decode()
                    decoded_extrinsics.append(ext_decoder.value)
                except Exception:
                    # Keep raw hex on decode failure
                    decoded_extrinsics.append(ext_data)

            block_data["block"]["extrinsics"] = decoded_extrinsics

        # Add spec version for reference
        block_data["spec_version"] = self.sub.runtime_version

        return block_data

    def setup_runtime_types(self, block_hash: str, spec_version: int = None):
        """
        Setup custom type registry for old runtimes (spec_version <= 100).

        Args:
            block_hash: Block hash for RPC lookup (only used if spec_version not provided)
            spec_version: Optional spec version to avoid RPC call
        """
        if spec_version is None:
            runtime_info = self.sub.get_block_runtime_version(block_hash)
            spec_version = runtime_info["specVersion"]

        if (
            spec_version > AUTO_TYPES_CUTOFF
            or self.sub.runtime_version == spec_version
        ):
            return

        # Build a single dict with the most up-to-date types for the given spec_version
        merged_types = {}

        # Iterate through the type definitions in our JSON
        types_sets = self.types["spec"]["substrate-threefold"]["types"]
        for types in types_sets:
            # Each spec has minmax and types
            spec_minmax = types.get("minmax", [])
            spec_types = types.get("types", {})

            min_version, max_version = spec_minmax
            # max_version can be null, indicating no upper bound
            if min_version <= spec_version and (
                max_version is None or spec_version <= max_version
            ):
                # Merge these types into our combined dict
                merged_types.update(spec_types)

        # For some reason, we'll also get hits to try to decode types with a
        # "type::" prefix. Rather than think too deeply about this, we just
        # insert them into the map
        for type_name, type_def in list(merged_types.items()):
            prefixed_key = f"types::{type_name}"
            merged_types[prefixed_key] = type_def

        self.sub.type_registry = {"types": merged_types}

    def create_keypair(self, mnemonic):
        """
        Creates a keypair from an existing mnemonic and saves it into the instance for signing operations. Not required for data queries.
        """
        self.keys = substrateinterface.Keypair.create_from_mnemonic(mnemonic)

    def find_block(self, timestamp):
        """
        Find the block that was created nearest to the provided timestamp. Note that this can be a block created after the specified timestamp
        """
        head = self.sub.get_block()
        head_time = self.get_timestamp(head) // 1000  # Convert to 10 digits
        head_number = head["header"]["number"]

        time_diff = head_time - timestamp
        blocks_diff = time_diff // 6  # Six second blocks
        guess_number = head_number - blocks_diff

        while blocks_diff > 0:
            guess_block = self.sub.get_block(block_number=guess_number)
            guess_block_time = self.get_timestamp(guess_block) // 1000
            time_diff = guess_block_time - timestamp
            blocks_diff = time_diff // 6  # Six second blocks
            guess_number -= blocks_diff
            last_time_diff = time_diff
            # print(last_time_diff, time_diff, guess_number)
        if time_diff < 4:
            return guess_number
        else:
            return guess_number - 1

    def find_block_minting(self, timestamp):
        """This is a port of the code found in the tfchain client that's included in the minting repo: https://github.com/threefoldtech/minting_v3/blob/c93b0c69dffec68fc5f5478db6b999112a27ad02/client/src/client.rs#L121

        It's behavior is such that it always returns a future block that was not yet created at the given timestamp, even if the timestamp matches the block timestamp exactly. I'm not sure if that was intended, but in any case, this is the one to reach for when trying to get the block that is considered the beginning or end of a period for minting purposes
        """
        latest_ts = self.get_time_at_block() // 1000
        if latest_ts < timestamp:
            raise ValueError("can't fetch block for future timestamp")
        height = 1
        last_height = 1

        while 1:
            block_hash = self.sub.get_block_hash(height)
            if block_hash is None:
                height = (height + last_height) // 2
                continue

            block_time = self.get_timestamp(self.sub.get_block(block_hash)) // 1000
            time_delta = timestamp - block_time
            block_delta = time_delta // BLOCK_TIME_SECONDS
            if block_delta == 0:
                if time_delta >= 0:
                    return height + 1
                else:
                    return height

            if (height + block_delta) < 0:
                raise RuntimeError()

            last_height = height
            height = height + block_delta

    def find_uptime_report(self, nodeid):
        head = self.sub.get_block_header()["header"]
        while 1:
            events = sub.get_events(head["hash"])
            for event in events:
                if (
                    event.value["event_id"] == "NodeUptimeReported"
                    and event.value["attributes"][0] == nodeid
                ):
                    return head["hash"], event.value["attributes"]
                    break
            else:
                head = self.sub.get_block_header(head["parentHash"])["header"]
                continue
            break

    def find_transfer(self, address):
        head = self.sub.get_block_header()["header"]
        while 1:
            events = self.sub.get_events(head["hash"])
            for event in events:
                try:
                    if event.value["event_id"] == "Transfer" and (
                        event.value["attributes"][0] == address
                        or event.value["attributes"][1] == address
                    ):
                        return head["hash"], event.value["attributes"]
                        break
                except IndexError:
                    print(event)
            else:
                head = self.sub.get_block_header(head["parentHash"])["header"]
                continue
            break

    def get_balance(self, wallet_address, block_hash=None):
        result = self.sub.query("System", "Account", [wallet_address], block_hash)
        return result.value["data"]["free"] / 1e7

    def get_next_farm_id(self):
        return self.sub.query("TfgridModule", "FarmID").value

    def get_farm(self, farm_id, block_hash=None):
        return self.sub.query("TfgridModule", "Farms", [farm_id], block_hash).value

    def get_node(self, node_id, block_hash=None):
        return self.sub.query("TfgridModule", "Nodes", [node_id], block_hash).value

    def get_node_id(self, block_hash=None):
        # Returns highest assigned node id
        return self.sub.query("TfgridModule", "NodeID", [], block_hash).value

    def get_node_by_twin(self, twin_id, block_hash=None):
        return self.sub.query(
            "TfgridModule", "NodeIdByTwinID", [twin_id], block_hash
        ).value

    def get_node_power(self, node_id, block_hash=None):
        return self.sub.query("TfgridModule", "NodePower", [node_id], block_hash).value

    def get_timestamp(self, block):
        # Timestamp should always be first extrinsic (right?)
        # Has millisecond precision, divide by 1000 to get seconds
        return block["extrinsics"][0].value["call"]["call_args"][0]["value"]

    def get_time_at_block(self, block_number=None):
        if block_number is None:
            block = self.sub.get_block()
        else:
            block = self.sub.get_block(block_number=block_number)
        return self.get_timestamp(block)

    def get_twin(self, twin_id):
        return self.sub.query("TfgridModule", "Twins", [twin_id]).value

    def get_twin_by_account(self, account_id, block_hash=None):
        return self.sub.query(
            "TfgridModule", "TwinIdByAccountID", [account_id], block_hash
        ).value

    def set_power_target(self, node_id, target):
        if self.keys is None:
            raise Exception("Please create a keypair first")

        params = {"node_id": node_id, "power_target": target}
        call = self.sub.compose_call("TfgridModule", "change_power_target", params)
        signed = self.sub.create_signed_extrinsic(call, self.keys)
        try:
            receipt = self.sub.submit_extrinsic(signed, wait_for_inclusion=True)
            return receipt

        except SubstrateRequestException as e:
            print("Failed with error: {}".format(e))


class SubstrateRPC:
    """Lightweight sync JSON-RPC client for Substrate (no type registry overhead)"""

    # Events storage key: twox128("System") + twox128("Events")
    # Pre "V9" apparently used a different key, but testing suggests that we are
    # V9 or after for the life of tfchain
    # https://github.com/JAMdotTech/py-polkadot-sdk/blob/a76e28b05f77dbe91032f290fee5b253197c82b1/substrateinterface/constants.py#L18
    EVENTS_STORAGE_KEY = (
        "0x26aa394eea5630e07c48ae0c9558cef780d41e5e16056765bc8461851072c9d7"
    )

    def __init__(self, url: str = "https://tfchain.grid.tf", timeout: int = 30):
        """
        Initialize sync RPC client.

        Args:
            url: Substrate node URL (http:// or https://)
            timeout: Request timeout in seconds (default: 30)
        """
        self.url = url
        self.timeout = timeout
        self.request_id = 0
        self._session: Optional[requests.Session] = None

    def __enter__(self):
        """Context manager entry"""
        self._ensure_session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

    def _ensure_session(self):
        """Lazy initialization of requests session"""
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({"Content-Type": "application/json"})

    def rpc_request(self, method: str, params: List[Any]) -> Any:
        """
        Make JSON-RPC call to Substrate node.

        Args:
            method: RPC method name (e.g., "chain_getBlock")
            params: List of parameters for the RPC call

        Returns:
            Result from the RPC call

        Raises:
            requests.RequestException: On network errors
            ValueError: If RPC returns an error
        """
        self._ensure_session()

        self.request_id += 1
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self.request_id,
        }

        resp = self._session.post(self.url, json=payload, timeout=self.timeout)
        resp.raise_for_status()
        result = resp.json()

        if "error" in result:
            error = result["error"]
            raise ValueError(f"RPC error: {error.get('message', error)}")

        return result.get("result")

    def get_block_hash(self, block_number: int) -> Optional[str]:
        """Get block hash from block number."""
        return self.rpc_request("chain_getBlockHash", [block_number])

    def get_block(self, block_hash: Optional[str] = None) -> Dict[str, Any]:
        """Get block by hash. If None, gets latest block."""
        params = [block_hash] if block_hash else []
        return self.rpc_request("chain_getBlock", params)

    def get_block_by_number(self, block_number: int) -> Dict[str, Any]:
        """Get block by number."""
        block_hash = self.get_block_hash(block_number)
        if block_hash is None:
            raise ValueError(f"Block {block_number} not found")
        return self.get_block(block_hash)

    def get_events_raw(self, block_hash: str) -> Optional[str]:
        """Get raw SCALE-encoded events from storage."""
        return self.rpc_request(
            "state_getStorage", [self.EVENTS_STORAGE_KEY, block_hash]
        )

    def get_runtime_version(self, block_hash: Optional[str] = None) -> Dict[str, Any]:
        """Get runtime version for a block."""
        params = [block_hash] if block_hash else []
        return self.rpc_request("state_getRuntimeVersion", params)

    def get_finalized_head(self) -> str:
        """Get the hash of the last finalized block."""
        return self.rpc_request("chain_getFinalizedHead", [])

    def get_header(self, block_hash: Optional[str] = None) -> Dict[str, Any]:
        """Get block header."""
        params = [block_hash] if block_hash else []
        return self.rpc_request("chain_getHeader", params)

    def close(self):
        """Close the requests session"""
        if self._session:
            self._session.close()
            self._session = None
