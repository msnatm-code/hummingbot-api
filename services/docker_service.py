import logging
import os
import shutil
from pathlib import Path
import threading
import time
from typing import Dict

import docker
from docker.errors import DockerException
from docker.types import LogConfig

from config import settings
from models import V2ControllerDeployment
from utils.file_system import fs_util

# Create module-specific logger
logger = logging.getLogger(__name__)



class DockerService:
    # Class-level configuration for cleanup
    PULL_STATUS_MAX_AGE_SECONDS = 3600  # Keep status for 1 hour
    PULL_STATUS_MAX_ENTRIES = 100  # Maximum number of entries to keep
    CLEANUP_INTERVAL_SECONDS = 300  # Run cleanup every 5 minutes

    def __init__(self):
        self.SOURCE_PATH = os.getcwd()
        self._pull_status: Dict[str, Dict] = {}
        self._cleanup_thread = None
        self._stop_cleanup = threading.Event()

        try:
            self.client = docker.from_env()
            # Start background cleanup thread
            self._start_cleanup_thread()
        except DockerException as e:
            logger.error(f"It was not possible to connect to Docker. Please make sure Docker is running. Error: {e}")

    def _resolve_bots_host_path(self) -> str:
        """
        Resolve the real host path for the shared bots directory.

        When the API itself runs in Docker, os.getcwd() points to an in-container
        path like /hummingbot-api, which is not a valid bind source for sibling
        containers. In that case we inspect this container's mounts and reuse the
        host source that is bound to /hummingbot-api/bots.
        """
        container_bots_path = os.path.join(self.SOURCE_PATH, "bots")

        try:
            current_hostname = os.environ.get("HOSTNAME")
            if current_hostname:
                current_container = self.client.containers.get(current_hostname)
                for mount in current_container.attrs.get("Mounts", []):
                    if mount.get("Destination") == container_bots_path:
                        source = mount.get("Source")
                        if source:
                            return source
        except Exception as e:
            logger.warning(f"Failed to resolve host bots path from current container mounts: {e}")

        return container_bots_path

    @staticmethod
    def _normalize_bind_source_path(path: str) -> str:
        """
        Preserve Windows-style absolute paths when running inside Linux containers.

        On Linux, os.path.abspath("C:\\Users\\...") incorrectly prefixes the current
        working directory because the drive-letter path is not considered absolute.
        Docker Desktop expects the original Windows host path string unchanged.
        """
        normalized = path.replace("\\", "/")
        if len(normalized) >= 3 and normalized[1] == ":" and normalized[2] == "/":
            return normalized
        return os.path.abspath(path)

    @classmethod
    def _resolve_host_bots_root_from_env(cls, configured_path: str) -> str:
        """
        Accept either the project root (e.g. C:/Users/.../MiTAHbot) or the bots root
        (e.g. C:/Users/.../MiTAHbot/bots) and always return the host bots root.
        """
        normalized = cls._normalize_bind_source_path(configured_path)
        return normalized if normalized.rstrip("/").endswith("/bots") else normalized.rstrip("/") + "/bots"

    @staticmethod
    def _normalize_script_module_name(script_name: str) -> str:
        """Return a script filename with exactly one .py suffix."""
        if not script_name:
            return script_name
        script_leaf = Path(script_name).name
        return script_leaf if script_leaf.endswith(".py") else f"{script_leaf}.py"

    def get_active_containers(self, name_filter: str = None):
        try:
            all_containers = self.client.containers.list(filters={"status": "running"})
            if name_filter:
                containers_info = [
                    {
                        "id": container.id,
                        "name": container.name,
                        "status": container.status,
                        "image": container.image.tags[0] if container.image.tags else container.image.id[:12]
                    }
                    for container in all_containers if name_filter.lower() in container.name.lower()
                ]
            else:
                containers_info = [
                    {
                        "id": container.id,
                        "name": container.name,
                        "status": container.status,
                        "image": container.image.tags[0] if container.image.tags else container.image.id[:12]
                    }
                    for container in all_containers
                ]
            return containers_info
        except DockerException as e:
            return str(e)

    def get_available_images(self):
        try:
            images = self.client.images.list()
            return {"images": images}
        except DockerException as e:
            return str(e)

    def pull_image(self, image_name):
        try:
            return self.client.images.pull(image_name)
        except DockerException as e:
            return str(e)

    def pull_image_sync(self, image_name):
        """Synchronous pull operation for background tasks"""
        try:
            result = self.client.images.pull(image_name)
            return {"success": True, "image": image_name, "result": str(result)}
        except DockerException as e:
            return {"success": False, "error": str(e)}

    def get_exited_containers(self, name_filter: str = None):
        try:
            all_containers = self.client.containers.list(filters={"status": "exited"}, all=True)
            if name_filter:
                containers_info = [
                    {
                        "id": container.id,
                        "name": container.name,
                        "status": container.status,
                        "image": container.image.tags[0] if container.image.tags else container.image.id[:12]
                    }
                    for container in all_containers if name_filter.lower() in container.name.lower()
                ]
            else:
                containers_info = [
                    {
                        "id": container.id,
                        "name": container.name,
                        "status": container.status,
                        "image": container.image.tags[0] if container.image.tags else container.image.id[:12]
                    }
                    for container in all_containers
                ]
            return containers_info
        except DockerException as e:
            return str(e)

    def clean_exited_containers(self):
        try:
            self.client.containers.prune()
        except DockerException as e:
            return str(e)

    def is_docker_running(self):
        try:
            self.client.ping()
            return True
        except DockerException:
            return False

    def stop_container(self, container_name):
        try:
            container = self.client.containers.get(container_name)
            container.stop()
        except DockerException as e:
            return str(e)

    def start_container(self, container_name):
        try:
            container = self.client.containers.get(container_name)
            container.start()
        except DockerException as e:
            return str(e)

    def get_container_status(self, container_name):
        """Get the status of a container"""
        try:
            container = self.client.containers.get(container_name)
            return {
                "success": True,
                "state": {
                    "status": container.status,
                    "running": container.status == "running",
                    "exit_code": getattr(container.attrs.get("State", {}), "ExitCode", None)
                }
            }
        except DockerException as e:
            return {"success": False, "message": str(e)}

    def remove_container(self, container_name, force=True):
        try:
            container = self.client.containers.get(container_name)
            container.remove(force=force)
            return {"success": True, "message": f"Container {container_name} removed successfully."}
        except DockerException as e:
            return {"success": False, "message": str(e)}

    @staticmethod
    def _ensure_contained(path: str, base_dir: str, label: str):
        """
        Defense in depth: verify that `path` stays inside `base_dir` after resolving symlinks and
        traversal sequences. Raises ValueError if it escapes the allowed base directory.
        """
        resolved_base = os.path.realpath(base_dir)
        resolved_path = os.path.realpath(path)
        if os.path.commonpath([resolved_base, resolved_path]) != resolved_base:
            raise ValueError(f"Invalid {label}: '{path}' resolves outside of '{base_dir}'.")
        return resolved_path

    def create_hummingbot_instance(self, config: V2ControllerDeployment):
        configured_bots_path = os.environ.get('BOTS_PATH')
        bots_path = (
            self._resolve_host_bots_root_from_env(configured_bots_path)
            if configured_bots_path
            else self._resolve_bots_host_path()
        )
        instance_name = config.instance_name
        sanitized_instance_name = instance_name.replace("_", "-")
        instance_dir = os.path.join("bots", 'instances', instance_name)
        # Defense in depth: ensure the resolved paths stay within their allowed base directories
        # before any filesystem mutation (makedirs/copytree) takes place.
        self._ensure_contained(instance_dir, os.path.join("bots", "instances"), "instance_name")
        source_credentials_dir = os.path.join("bots", 'credentials', config.credentials_profile)
        self._ensure_contained(source_credentials_dir, os.path.join("bots", "credentials"), "credentials_profile")
        if not os.path.exists(instance_dir):
            os.makedirs(instance_dir)
            os.makedirs(os.path.join(instance_dir, 'data'))
            os.makedirs(os.path.join(instance_dir, 'logs'))

        # Copy credentials to instance directory
        destination_credentials_dir = os.path.join(instance_dir, 'conf')

        # Remove the destination directory if it already exists
        if os.path.exists(destination_credentials_dir):
            shutil.rmtree(destination_credentials_dir)

        # Copy the entire contents of source_credentials_dir to destination_credentials_dir
        shutil.copytree(source_credentials_dir, destination_credentials_dir)

        # Copy specific script config and referenced controllers if provided
        if config.script_config:
            script_config_dir = os.path.join("bots", 'conf', 'scripts')
            controllers_config_dir = os.path.join("bots", 'conf', 'controllers')
            destination_scripts_config_dir = os.path.join(instance_dir, 'conf', 'scripts')
            destination_controllers_config_dir = os.path.join(instance_dir, 'conf', 'controllers')

            os.makedirs(destination_scripts_config_dir, exist_ok=True)

            # Copy the specific script config file
            source_script_config_file = os.path.join(script_config_dir, config.script_config)
            destination_script_config_file = os.path.join(destination_scripts_config_dir, config.script_config)

            if os.path.exists(source_script_config_file):
                shutil.copy2(source_script_config_file, destination_script_config_file)

                # Load the script config to find referenced controllers
                try:
                    # Path relative to fs_util base_path (which is "bots")
                    script_config_relative_path = f"conf/scripts/{config.script_config}"
                    script_config_content = fs_util.read_yaml_file(script_config_relative_path)
                    controllers_list = script_config_content.get('controllers_config', [])

                    # If there are controllers referenced, copy them
                    if controllers_list:
                        os.makedirs(destination_controllers_config_dir, exist_ok=True)

                        for controller_file in controllers_list:
                            source_controller_file = os.path.join(controllers_config_dir, controller_file)
                            destination_controller_file = os.path.join(
                                destination_controllers_config_dir, controller_file
                            )

                            if os.path.exists(source_controller_file):
                                shutil.copy2(source_controller_file, destination_controller_file)
                                logger.info(f"Copied controller config: {controller_file}")
                            else:
                                logger.warning(
                                    f"Controller config file {controller_file} not found in {controllers_config_dir}"
                                )

                except Exception as e:
                    logger.error(f"Error reading script config file {config.script_config}: {e}")
            else:
                logger.warning(f"Script config file {config.script_config} not found in {script_config_dir}")
        # Path relative to fs_util base_path (which is "bots")
        conf_file_path = f"instances/{instance_name}/conf/conf_client.yml"
        client_config = fs_util.read_yaml_file(conf_file_path)
        client_config['instance_id'] = sanitized_instance_name
        fs_util.dump_dict_to_yaml(conf_file_path, client_config)

        # Set up Docker volumes. `bots_path` points to the host's shared `bots`
        # directory root, so we must build paths relative to that root instead of
        # prefixing another `bots/` segment.
        host_instance_dir = os.path.join(bots_path, 'instances', instance_name)
        instance_conf = self._normalize_bind_source_path(os.path.join(host_instance_dir, 'conf'))
        instance_connectors = self._normalize_bind_source_path(os.path.join(host_instance_dir, 'conf', 'connectors'))
        instance_scripts = self._normalize_bind_source_path(os.path.join(host_instance_dir, 'conf', 'scripts'))
        instance_controllers = self._normalize_bind_source_path(os.path.join(host_instance_dir, 'conf', 'controllers'))
        instance_data = self._normalize_bind_source_path(os.path.join(host_instance_dir, 'data'))
        instance_logs = self._normalize_bind_source_path(os.path.join(host_instance_dir, 'logs'))
        shared_scripts = self._normalize_bind_source_path(os.path.join(bots_path, 'scripts'))
        shared_controllers = self._normalize_bind_source_path(os.path.join(bots_path, 'controllers'))

        bybit_eu_path = os.environ.get('BYBIT_EU_PATH')
        bybit_eu_testnet_path = os.environ.get('BYBIT_EU_TESTNET_PATH')

        volumes = {
            instance_conf: {'bind': '/home/hummingbot/conf', 'mode': 'rw'},
            instance_connectors: {'bind': '/home/hummingbot/conf/connectors', 'mode': 'rw'},
            instance_scripts: {'bind': '/home/hummingbot/conf/scripts', 'mode': 'rw'},
            instance_controllers: {'bind': '/home/hummingbot/conf/controllers', 'mode': 'rw'},
            instance_data: {'bind': '/home/hummingbot/data', 'mode': 'rw'},
            instance_logs: {'bind': '/home/hummingbot/logs', 'mode': 'rw'},
            shared_scripts: {'bind': '/home/hummingbot/scripts', 'mode': 'rw'},
            shared_controllers: {'bind': '/home/hummingbot/controllers', 'mode': 'rw'},
        }

        if bybit_eu_path:
            volumes[self._normalize_bind_source_path(bybit_eu_path)] = {
                'bind': '/home/hummingbot/hummingbot/connector/exchange/bybit_eu',
                'mode': 'ro'
            }
        if bybit_eu_testnet_path:
            volumes[self._normalize_bind_source_path(bybit_eu_testnet_path)] = {
                'bind': '/home/hummingbot/hummingbot/connector/exchange/bybit_eu_testnet',
                'mode': 'ro'
            }

        logger.warning(
            "VOLUMES: %s | bots_path=%s | bybit_eu_path=%s | bybit_eu_testnet_path=%s",
            volumes,
            bots_path,
            bybit_eu_path,
            bybit_eu_testnet_path,
        )

        # Set up environment variables
        environment = {}
        password = settings.security.config_password
        if password:
            environment["CONFIG_PASSWORD"] = password

        if config.script_config:
            if password:
                environment['SCRIPT_CONFIG'] = config.script_config
            else:
                return {"success": False, "message": "Password not provided. We cannot start the bot without a password."}

        if config.headless:
            environment["HEADLESS_MODE"] = "true"

        log_config = LogConfig(
            type="json-file",
            config={
                'max-size': '10m',
                'max-file': "5",
            })

        quickstart_parts = [
            "conda activate hummingbot",
            "python ./bin/hummingbot_quickstart.py",
        ]

        if config.script_config:
            quickstart_parts[-1] += f' --v2 "{config.script_config}" --config-password "{password}"'

        quickstart_command = " && ".join(quickstart_parts)
        container_name = sanitized_instance_name

        try:
            self.client.containers.run(
                image=config.image,
                name=container_name,
                command=["/bin/bash", "-lc", quickstart_command],
                volumes=volumes,
                environment=environment,
                network="mitahbot_mitahbot-net",
                detach=True,
                tty=True,
                stdin_open=True,
                restart_policy={"Name": "unless-stopped"},
                log_config=log_config,
            )
            return {"success": True, "message": f"Instance {instance_name} created successfully."}
        except docker.errors.DockerException as e:
            return {"success": False, "message": str(e)}

    def _start_cleanup_thread(self):
        """Start the background cleanup thread"""
        if self._cleanup_thread is None or not self._cleanup_thread.is_alive():
            self._cleanup_thread = threading.Thread(target=self._periodic_cleanup, daemon=True)
            self._cleanup_thread.start()
            logger.info("Started Docker pull status cleanup thread")

    def _periodic_cleanup(self):
        """Periodically clean up old pull status entries"""
        while not self._stop_cleanup.is_set():
            try:
                self._cleanup_old_pull_status()
            except Exception as e:
                logger.error(f"Error in cleanup thread: {e}")

            # Wait for the next cleanup interval
            self._stop_cleanup.wait(self.CLEANUP_INTERVAL_SECONDS)

    def _cleanup_old_pull_status(self):
        """Remove old entries to prevent memory growth"""
        current_time = time.time()
        to_remove = []

        # Find entries older than max age
        for image_name, status_info in self._pull_status.items():
            # Skip ongoing pulls
            if status_info["status"] == "pulling":
                continue

            # Check age of completed/failed operations
            end_time = status_info.get("completed_at") or status_info.get("failed_at")
            if end_time and (current_time - end_time > self.PULL_STATUS_MAX_AGE_SECONDS):
                to_remove.append(image_name)

        # Remove old entries
        for image_name in to_remove:
            del self._pull_status[image_name]
            logger.info(f"Cleaned up old pull status for {image_name}")

        # If still over limit, remove oldest completed/failed entries
        if len(self._pull_status) > self.PULL_STATUS_MAX_ENTRIES:
            completed_entries = [
                (name, info) for name, info in self._pull_status.items()
                if info["status"] in ["completed", "failed"]
            ]
            # Sort by end time (oldest first)
            completed_entries.sort(
                key=lambda x: x[1].get("completed_at") or x[1].get("failed_at") or 0
            )

            # Remove oldest entries to get under limit
            excess_count = len(self._pull_status) - self.PULL_STATUS_MAX_ENTRIES
            for i in range(min(excess_count, len(completed_entries))):
                del self._pull_status[completed_entries[i][0]]
                logger.info(f"Cleaned up excess pull status for {completed_entries[i][0]}")

    def pull_image_async(self, image_name: str):
        """Start pulling a Docker image asynchronously with status tracking"""
        # Check if pull is already in progress
        if image_name in self._pull_status:
            current_status = self._pull_status[image_name]
            if current_status["status"] == "pulling":
                return {
                    "message": f"Pull already in progress for {image_name}",
                    "status": "in_progress",
                    "started_at": current_status["started_at"],
                    "image_name": image_name
                }

        # Start the pull in a background thread
        threading.Thread(target=self._pull_image_with_tracking, args=(image_name,), daemon=True).start()

        return {
            "message": f"Pull started for {image_name}",
            "status": "started",
            "image_name": image_name
        }

    def _pull_image_with_tracking(self, image_name: str):
        """Background task to pull Docker image with status tracking"""
        try:
            self._pull_status[image_name] = {
                "status": "pulling",
                "started_at": time.time(),
                "progress": "Starting pull..."
            }

            # Use the synchronous pull method
            result = self.pull_image_sync(image_name)

            if result.get("success"):
                self._pull_status[image_name] = {
                    "status": "completed",
                    "started_at": self._pull_status[image_name]["started_at"],
                    "completed_at": time.time(),
                    "result": result
                }
            else:
                self._pull_status[image_name] = {
                    "status": "failed",
                    "started_at": self._pull_status[image_name]["started_at"],
                    "failed_at": time.time(),
                    "error": result.get("error", "Unknown error")
                }
        except Exception as e:
            self._pull_status[image_name] = {
                "status": "failed",
                "started_at": self._pull_status[image_name].get("started_at", time.time()),
                "failed_at": time.time(),
                "error": str(e)
            }

    def get_all_pull_status(self):
        """Get status of all pull operations"""
        operations = {}
        for image_name, status_info in self._pull_status.items():
            status_copy = status_info.copy()

            # Add duration for each operation
            start_time = status_copy.get("started_at")
            if start_time:
                if status_copy["status"] == "pulling":
                    status_copy["duration_seconds"] = round(time.time() - start_time, 2)
                elif "completed_at" in status_copy:
                    status_copy["duration_seconds"] = round(status_copy["completed_at"] - start_time, 2)
                elif "failed_at" in status_copy:
                    status_copy["duration_seconds"] = round(status_copy["failed_at"] - start_time, 2)

            operations[image_name] = status_copy

        return {
            "pull_operations": operations,
            "total_operations": len(operations)
        }

    def cleanup(self):
        """Clean up resources when shutting down"""
        self._stop_cleanup.set()
        if self._cleanup_thread:
            self._cleanup_thread.join(timeout=1)
