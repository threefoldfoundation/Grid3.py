"""
Full Python wrapper for the tfcmd command line application
Provides a complete interface for interacting with the ThreeFold Grid

This module provides a Python interface to the ThreeFold Grid CLI (tfcmd) tool,
allowing developers to deploy and manage resources on the ThreeFold Grid network
including VMs, Kubernetes clusters, ZDBs, and Gateways.
"""

import json
import os
import re
import subprocess
from typing import Any, Dict, List, Optional


class TFCmdError(Exception):
    """Custom exception for tfcmd errors"""

    pass


class TFCmd:
    """
    Main class for interacting with the tfcmd
    """

    def __init__(self, binary_path: str = "tfcmd"):
        """
        Initialize the tfcmd wrapper

        Args:
            binary_path: Path to the tfcmd binary (default: "tfcmd")
        """
        self.binary_path = binary_path
        # Verify the binary exists
        if not self._binary_exists():
            raise TFCmdError(f"tfcmd binary not found: {binary_path}")

    def _ensure_ssh_key(self, ssh_path: Optional[str] = None) -> str:
        """
        Ensure SSH key path is valid, using default if not provided

        Args:
            ssh_path: Optional path to SSH key

        Returns:
            Valid SSH key path
        """
        if ssh_path is None:
            ssh_path = os.path.expanduser("~/.ssh/id_rsa.pub")

        if not os.path.exists(ssh_path):
            raise TFCmdError(f"SSH key not found: {ssh_path}")

        return ssh_path

    def _binary_exists(self) -> bool:
        """
        Check if the tfcmd binary exists
        """
        try:
            result = subprocess.run(
                [self.binary_path, "--help"], capture_output=True, text=True, timeout=10
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False

    def _parse_json_output(self, result: subprocess.CompletedProcess) -> Dict[str, Any]:
        """Helper to parse JSON from subprocess result."""
        output_to_parse = result.stderr.strip()
        if not output_to_parse:
            output_to_parse = result.stdout.strip()

        if output_to_parse:
            # Iterate backwards to find the start of the last JSON object
            for i in range(len(output_to_parse) - 1, -1, -1):
                if output_to_parse[i] in ("{", "["):
                    json_part = output_to_parse[i:]
                    try:
                        return json.loads(json_part)
                    except json.JSONDecodeError:
                        # This is not the start of the JSON, continue searching
                        continue

            # If no valid JSON is found after checking all possibilities
            raise TFCmdError(
                f"Failed to find valid JSON in output.\nOutput: {output_to_parse}"
            )
        raise TFCmdError("No output to parse for JSON")

    def _execute(self, args: List[str]) -> subprocess.CompletedProcess:
        """
        Execute a tfcmd command and return the result

        Args:
            args: List of arguments to pass to tfcmd

        Returns:
            The subprocess.CompletedProcess object
        """

        try:
            full_args = [self.binary_path] + args

            result = subprocess.run(
                full_args, capture_output=True, text=True, timeout=300
            )  # 5 minute timeout for long operations

            if result.returncode != 0:
                error_msg = result.stderr or result.stdout

                raise TFCmdError(
                    f"Command failed: {' '.join(full_args)}\nError: {error_msg}"
                )

            return result

        except subprocess.TimeoutExpired:
            raise TFCmdError(f"Command timed out: {' '.join(full_args)}")

        except Exception as e:
            raise TFCmdError(f"Unexpected error executing command: {e}")

    def version(self) -> str:
        """
        Get the version of the tfcmd

        Returns:
            Version string
        """
        result = self._execute(["version"])
        return result.stdout.strip()

    def login(self, mnemonics: str, network: str = "main") -> str:
        """
        Login to the grid by saving mnemonics and network to config file

        Args:
            mnemonics: Secret words for authentication
            network: Network to use (default: "main")

        Returns:
            Success message
        """
        # Create config directory if it doesn't exist
        config_dir = os.path.expanduser("~/.config")
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)

        # Write the config file
        config_path = os.path.join(config_dir, ".tfgridconfig")
        config_data = {"mnemonics": mnemonics, "network": network}

        with open(config_path, "w") as f:
            json.dump(config_data, f)

        return "Login successful. Credentials saved to ~/.config/.tfgridconfig"

    def is_logged_in(self) -> bool:
        """
        Check if user is already logged in by checking the config file

        Returns:
            True if logged in, False otherwise
        """
        config_path = os.path.expanduser("~/.config/.tfgridconfig")

        if not os.path.exists(config_path):
            return False

        try:
            with open(config_path, "r") as f:
                config_data = json.load(f)

            # Check that mnemonics and network exist and are filled
            if "mnemonics" not in config_data or not config_data["mnemonics"]:
                return False

            if "network" not in config_data or not config_data["network"]:
                return False

            return True
        except (json.JSONDecodeError, IOError):
            return False

    def cancel(self, name: str) -> str:
        """
        Cancel resources on Threefold grid

        Args:
            name: Name of the project to cancel

        Returns:
            Cancel result message
        """
        return self._execute(["cancel", name]).stderr

    def cancel_contracts(
        self, contracts: Optional[List[int]] = None, all_contracts: bool = False
    ) -> str:
        """
        Cancel twin contracts

        Args:
            contracts: List of contract IDs to cancel (ignored if all_contracts=True)
            all_contracts: Whether to cancel all contracts

        Returns:
            Cancel contracts result message
        """
        if all_contracts:
            return self._execute(["cancel", "contracts", "--all"]).stderr
        else:
            if not contracts:
                raise TFCmdError(
                    "Either provide contracts list or set all_contracts=True"
                )
            return self._execute(
                ["cancel", "contracts"] + [str(c) for c in contracts]
            ).stderr

    def get_contracts(self) -> str:
        """
        Get twin contracts

        Returns:
            String containing contracts information in tabular format
        """
        # This command outputs to stdout in tabular format, so we don't parse JSON
        result = self._execute(["get", "contracts", "--no-color"])
        # Ensure that newline escape sequences are properly interpreted
        output = result.stdout
        if isinstance(output, str):
            output = output.encode().decode("unicode_escape")
        return output

    # Get methods
    def get_vm(self, name: str) -> Dict[str, Any]:
        """
        Get deployed VM

        Args:
            name: Name of the VM to retrieve

        Returns:
            VM information as dictionary
        """
        result = self._execute(["get", "vm", name, "--no-color"])
        return self._parse_json_output(result)

    def get_kubernetes(self, name: str) -> Dict[str, Any]:
        """
        Get deployed Kubernetes cluster

        Args:
            name: Name of the Kubernetes cluster to retrieve

        Returns:
            Kubernetes cluster information as dictionary
        """
        result = self._execute(["get", "kubernetes", name, "--no-color"])
        return self._parse_json_output(result)

    def get_zdb(self, name: str) -> Dict[str, Any]:
        """
        Get deployed ZDB

        Args:
            name: Name of the ZDB to retrieve

        Returns:
            ZDB information as dictionary
        """
        result = self._execute(["get", "zdb", name, "--no-color"])
        return self._parse_json_output(result)

    def get_gateway_fqdn(self, name: str) -> Dict[str, Any]:
        """
        Get deployed gateway FQDN

        Args:
            name: Name of the gateway FQDN to retrieve

        Returns:
            Gateway FQDN information as dictionary
        """
        result = self._execute(["get", "gateway", "fqdn", name, "--no-color"])
        return self._parse_json_output(result)

    def get_gateway_name(self, name: str) -> Dict[str, Any]:
        """
        Get deployed gateway name

        Args:
            name: Name of the gateway name to retrieve

        Returns:
            Gateway name information as dictionary
        """
        result = self._execute(["get", "gateway", "name", name, "--no-color"])
        return self._parse_json_output(result)

    # Deploy methods
    def deploy_vm(
        self,
        name: str,
        ssh: str = None,
        node: int = 0,
        farm: int = 1,
        cpu: int = 1,
        memory: int = 1,
        rootfs: int = 2,
        disk: int = 0,
        volume: int = 0,
        flist: str = "https://hub.grid.tf/tf-official-apps/threefoldtech-ubuntu-22.04.flist",
        entrypoint: str = "/sbin/zinit init",
        gpus: List[str] = None,
        ipv4: bool = False,
        ipv6: bool = False,
        ygg: bool = False,
        mycelium: bool = True,
        env: Dict[str, str] = None,
    ) -> Dict[str, Any]:
        """
        Deploy a virtual machine

        Args:
            name: Name of the virtual machine
            ssh: Path to public ssh key (defaults to ~/.ssh/id_rsa.pub)
            node: Node ID VM should be deployed on (0 = auto-select)
            farm: Farm ID VM should be deployed on (default: 1)
            cpu: Number of CPU units (default: 1)
            memory: Memory size in GB (default: 1)
            rootfs: Root filesystem size in GB (default: 2)
            disk: Disk size in GB mounted on /data (default: 0)
            volume: Volume size in GB mounted on /volume (default: 0)
            flist: FLIST for VM (default: Ubuntu 22.04)
            entrypoint: Entrypoint for VM (default: /sbin/zinit init)
            gpus: List of GPUs for VM
            ipv4: Assign public IPv4 for VM (default: False)
            ipv6: Assign public IPv6 for VM (default: False)
            ygg: Assign Yggdrasil IP for VM (default: False)
            mycelium: Assign Mycelium IP for VM (default: True)
            env: Environment variables for the VM

        Returns:
            Dictionary with deployment result including IP addresses

        Raises:
            TFCmdError: If deployment fails
        """
        # Validate inputs
        if not name:
            raise TFCmdError("VM name cannot be empty")

        ssh = self._ensure_ssh_key(ssh)

        args = ["deploy", "vm", "--name", name, "--no-color"]
        args.extend(["--ssh", ssh])

        if node != 0:
            args.extend(["--node", str(node)])
        else:
            args.extend(["--farm", str(farm)])

        args.extend(["--cpu", str(cpu)])
        args.extend(["--memory", str(memory)])
        args.extend(["--rootfs", str(rootfs)])
        if disk > 0:
            args.extend(["--disk", str(disk)])
        if volume > 0:
            args.extend(["--volume", str(volume)])

        # Only add flist and entrypoint if custom ones are provided
        if (
            flist
            != "https://hub.grid.tf/tf-official-apps/threefoldtech-ubuntu-22.04.flist"
        ):
            args.extend(["--flist", flist])
        args.extend(["--entrypoint", entrypoint])

        if gpus:
            args.extend(["--gpus"] + gpus)

        if ipv4:
            args.append("--ipv4")
        if ipv6:
            args.append("--ipv6")
        if ygg:
            args.append("--ygg")
        if not mycelium:
            args.extend(["--mycelium", "false"])

        if env:
            for key, value in env.items():
                args.extend(["--env", f"{key}={value}"])

        result = self._execute(args)
        output = result.stdout + result.stderr

        # Parse IP addresses from output
        ipv6_match = re.search(r"vm ipv6: ([\w:/]+)", output)
        mycelium_match = re.search(r"vm mycelium ip: ([\w:]+)", output)

        return {
            "ipv6": ipv6_match.group(1) if ipv6_match else None,
            "mycelium_ip": mycelium_match.group(1) if mycelium_match else None,
            "output": output,
        }

    def deploy_kubernetes(
        self,
        name: str,
        ssh: str = None,
        master_node: int = 0,
        master_farm: int = 1,
        master_cpu: int = 1,
        master_memory: int = 1,
        master_disk: int = 2,
        ipv4: bool = False,
        ipv6: bool = False,
        ygg: bool = True,
        mycelium: bool = True,
        workers_number: int = 0,
        workers_nodes: List[int] = None,
        workers_farm: int = 1,
        workers_cpu: int = 1,
        workers_memory: int = 1,
        workers_disk: int = 2,
        workers_ipv4: bool = False,
        workers_ipv6: bool = False,
        workers_ygg: bool = True,
        workers_mycelium: bool = True,
    ) -> str:
        """
        Deploy a Kubernetes cluster

        Args:
            name: Name of the Kubernetes cluster
            ssh: Path to public ssh key
            master_node: Node ID for master (0 = auto-select)
            master_farm: Farm ID for master (default: 1)
            master_cpu: Master CPU units (default: 1)
            master_memory: Master memory in GB (default: 1)
            master_disk: Master disk in GB (default: 2)
            ipv4: Assign public IPv4 for master (default: False)
            ipv6: Assign public IPv6 for master (default: False)
            ygg: Assign Yggdrasil IP for master (default: True)
            mycelium: Assign Mycelium IP for master (default: True)
            workers_number: Number of worker nodes (default: 0)
            workers_nodes: List of node IDs for workers
            workers_farm: Farm ID for workers (default: 1)
            workers_cpu: Workers CPU units (default: 1)
            workers_memory: Workers memory in GB (default: 1)
            workers_disk: Workers disk in GB (default: 2)
            workers_ipv4: Assign public IPv4 for workers (default: False)
            workers_ipv6: Assign public IPv6 for workers (default: False)
            workers_ygg: Assign Yggdrasil IP for workers (default: True)
            workers_mycelium: Assign Mycelium IP for workers (default: True)

        Returns:
            Deployment result message

        Raises:
            TFCmdError: If deployment fails
        """
        # Validate inputs
        if not name:
            raise TFCmdError("Kubernetes cluster name cannot be empty")

        ssh = self._ensure_ssh_key(ssh)

        args = ["deploy", "kubernetes", "--name", name, "--no-color"]
        args.extend(["--ssh", ssh])

        if master_node != 0:
            args.extend(["--master-node", str(master_node)])
        else:
            args.extend(["--master-farm", str(master_farm)])

        args.extend(["--master-cpu", str(master_cpu)])
        args.extend(["--master-memory", str(master_memory)])
        args.extend(["--master-disk", str(master_disk)])

        if ipv4:
            args.append("--ipv4")
        if ipv6:
            args.append("--ipv6")
        if not ygg:
            args.extend(["--ygg", "false"])
        if not mycelium:
            args.extend(["--mycelium", "false"])

        args.extend(["--workers-number", str(workers_number)])
        args.extend(["--workers-cpu", str(workers_cpu)])
        args.extend(["--workers-memory", str(workers_memory)])
        args.extend(["--workers-disk", str(workers_disk)])

        if workers_nodes:
            args.extend(["--workers-nodes"] + [str(n) for n in workers_nodes])
        else:
            args.extend(["--workers-farm", str(workers_farm)])

        if workers_ipv4:
            args.append("--workers-ipv4")
        if workers_ipv6:
            args.append("--workers-ipv6")
        if not workers_ygg:
            args.extend(["--workers-ygg", "false"])
        if not workers_mycelium:
            args.extend(["--workers-mycelium", "false"])

        return self._execute(args).stdout

    def deploy_zdb(
        self,
        project_name: str,
        size: int,
        count: int = 1,
        names: List[str] = None,
        password: str = None,
        description: str = None,
        mode: str = "user",
        node: int = 0,
        farm: int = 1,
        public: bool = False,
    ) -> str:
        """
        Deploy ZDB (Zero Database)

        Args:
            project_name: Project name of the ZDBs to be deployed
            size: Size of ZDB in GB
            count: Number of ZDBs to deploy (default: 1)
            names: List of names for the ZDBs
            password: Password for the ZDB
            description: Description of the ZDB
            mode: Mode of ZDB (user or seq, default: user)
            node: Node ID for deployment (0 = auto-select)
            farm: Farm ID for deployment (default: 1)
            public: Whether ZDB gets a public IP (default: False)

        Returns:
            Deployment result message

        Raises:
            TFCmdError: If deployment fails
        """
        # Validate inputs
        if not project_name:
            raise TFCmdError("Project name cannot be empty")
        if size <= 0:
            raise TFCmdError("Size must be greater than 0")
        if count <= 0:
            raise TFCmdError("Count must be greater than 0")
        if mode not in ["user", "seq"]:
            raise TFCmdError("Mode must be either 'user' or 'seq'")

        args = [
            "deploy",
            "zdb",
            "--project_name",
            project_name,
            "--size",
            str(size),
            "--no-color",
        ]
        args.extend(["--count", str(count)])
        args.extend(["--mode", mode])

        if names:
            args.extend(["--names"] + names)
        if password:
            args.extend(["--password", password])
        if description:
            args.extend(["--description", description])
        if public:
            args.append("--public")

        if node != 0:
            args.extend(["--node", str(node)])
        else:
            args.extend(["--farm", str(farm)])

        return self._execute(args).stdout

    def deploy_gateway_fqdn(
        self, name: str, backends: List[str], fqdn: str, node: int, tls: bool = False
    ) -> str:
        """
        Deploy a gateway FQDN proxy

        Args:
            name: Name of the gateway
            backends: List of backend URLs
            fqdn: FQDN pointing to the specified node
            node: Node ID gateway should be deployed on
            tls: Add TLS passthrough (default: False)

        Returns:
            Deployment result message

        Raises:
            TFCmdError: If deployment fails
        """
        # Validate inputs
        if not name:
            raise TFCmdError("Gateway name cannot be empty")
        if not backends:
            raise TFCmdError("Backends list cannot be empty")
        if not fqdn:
            raise TFCmdError("FQDN cannot be empty")
        if node <= 0:
            raise TFCmdError("Node ID must be greater than 0")

        args = ["deploy", "gateway", "fqdn", "--name", name, "--no-color"]
        args.extend(["--backends"] + backends)
        args.extend(["--fqdn", fqdn])
        if tls:
            args.append("--tls")

        return self._execute(args).stdout

    def deploy_gateway_name(
        self,
        name: str,
        backends: List[str],
        node: int = 0,
        farm: int = 1,
        tls: bool = False,
    ) -> str:
        """
        Deploy a gateway name proxy

        Args:
            name: Name of the gateway
            backends: List of backend URLs
            node: Node ID gateway should be deployed on (0 = auto-select)
            farm: Farm ID gateway should be deployed on (default: 1)
            tls: Add TLS passthrough (default: False)

        Returns:
            Deployment result message

        Raises:
            TFCmdError: If deployment fails
        """
        # Validate inputs
        if not name:
            raise TFCmdError("Gateway name cannot be empty")
        if not backends:
            raise TFCmdError("Backends list cannot be empty")

        args = ["deploy", "gateway", "name", "--name", name, "--no-color"]
        args.extend(["--backends"] + backends)

        if node != 0:
            args.extend(["--node", str(node)])
        else:
            args.extend(["--farm", str(farm)])

        if tls:
            args.append("--tls")

        return self._execute(args).stdout

    # Update methods
    def add_kubernetes_worker(
        self,
        name: str,
        ssh: str,
        workers_number: int = 1,
        workers_nodes: List[int] = None,
        workers_farm: int = 1,
        workers_cpu: int = 1,
        workers_memory: int = 1,
        workers_disk: int = 2,
        workers_ipv4: bool = False,
        workers_ipv6: bool = False,
        workers_ygg: bool = True,
        workers_mycelium: bool = True,
    ) -> str:
        """
        Add workers to a Kubernetes cluster

        Args:
            name: Name of the Kubernetes cluster
            ssh: Path to public ssh key
            workers_number: Number of workers to add (default: 1)
            workers_nodes: List of node IDs for workers
            workers_farm: Farm ID for workers (default: 1)
            workers_cpu: Workers CPU units (default: 1)
            workers_memory: Workers memory in GB (default: 1)
            workers_disk: Workers disk in GB (default: 2)
            workers_ipv4: Assign public IPv4 for workers (default: False)
            workers_ipv6: Assign public IPv6 for workers (default: False)
            workers_ygg: Assign Yggdrasil IP for workers (default: True)
            workers_mycelium: Assign Mycelium IP for workers (default: True)

        Returns:
            Update result message

        Raises:
            TFCmdError: If update fails
        """
        # Validate inputs
        if not name:
            raise TFCmdError("Kubernetes cluster name cannot be empty")
        ssh = self._ensure_ssh_key(ssh)

        if not workers_mycelium:
            args.extend(["--workers-mycelium", "false"])

        return self._execute(args).stdout

    def delete_kubernetes_worker(self, name: str, worker_name: str) -> str:
        """
        Remove a worker from a Kubernetes cluster

        Args:
            name: Name of the Kubernetes cluster
            worker_name: Name of the worker to delete

        Returns:
            Update result message

        Raises:
            TFCmdError: If update fails
        """
        # Validate inputs
        if not name:
            raise TFCmdError("Kubernetes cluster name cannot be empty")
        if not worker_name:
            raise TFCmdError("Worker name cannot be empty")

        return self._execute(
            [
                "update",
                "kubernetes",
                "delete",
                "--name",
                name,
                "--worker-name",
                worker_name,
            ]
        ).stdout
