import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional

import docker

from config import settings
from database import AsyncDatabaseManager, ControllerPerformanceRepository
from utils.mqtt_manager import MQTTManager

logger = logging.getLogger(__name__)


def _to_canonical_bot_id(bot_id: str) -> str:
    """Normalize bot identifiers to the docker/container naming convention."""
    return bot_id.replace("_", "-")


class BotsOrchestrator:
    """Orchestrates Hummingbot instances using Docker and MQTT communication."""

    def __init__(self, broker_host, broker_port, broker_username, broker_password,
                 db_manager: AsyncDatabaseManager, performance_dump_interval: int = 5):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.broker_username = broker_username
        self.broker_password = broker_password

        self.docker_client = docker.from_env()
        self.mqtt_manager = MQTTManager(host=broker_host, port=broker_port, username=broker_username, password=broker_password)

        self.active_bots = {}
        self._update_bots_task: Optional[asyncio.Task] = None
        self.stopping_bots = set()

        self.performance_dump_interval = performance_dump_interval * 60
        self._performance_dump_task: Optional[asyncio.Task] = None

        # Shared manager injected from main.py
        self.db_manager = db_manager
        self._db_initialized = False

    @staticmethod
    def hummingbot_containers_fiter(container):
        """Filter for Hummingbot containers based on image name pattern."""
        try:
            image_name = container.image.tags[0] if container.image.tags else str(container.image)
            pattern = r'.+/hummingbot:'
            return bool(re.match(pattern, image_name))
        except Exception:
            return False

    async def get_active_containers(self):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._sync_get_active_containers)

    def _sync_get_active_containers(self):
        return [
            container.name
            for container in self.docker_client.containers.list()
            if container.status == "running" and self.hummingbot_containers_fiter(container)
        ]

    def start(self):
        """Start the loop that monitors active bots."""
        self._update_bots_task = asyncio.create_task(self._start_async())
        self._performance_dump_task = asyncio.create_task(self._performance_dump_loop())
        logger.info(f"Controller performance dump started ({self.performance_dump_interval}s interval)")

    async def _start_async(self):
        logger.info("Starting MQTT manager...")
        await self.mqtt_manager.start()
        await self.update_active_bots()

    async def stop(self):
        """Stop the active bots monitoring loop."""
        if self._update_bots_task:
            self._update_bots_task.cancel()
        self._update_bots_task = None
        if self._performance_dump_task:
            self._performance_dump_task.cancel()
        self._performance_dump_task = None
        await self.mqtt_manager.stop()

    async def update_active_bots(self, sleep_time=1.0):
        """Monitor and update active bots list using both Docker and MQTT discovery."""
        while True:
            try:
                docker_bots = await self.get_active_containers()
                mqtt_bots = self.mqtt_manager.get_discovered_bots(timeout_seconds=30)
                docker_bots_canonical = {_to_canonical_bot_id(bot) for bot in docker_bots}
                mqtt_bots_canonical = {_to_canonical_bot_id(bot) for bot in mqtt_bots}
                all_active_bots = set([bot for bot in docker_bots if not self.is_bot_stopping(bot)])

                for bot_name in list(self.active_bots):
                    if _to_canonical_bot_id(bot_name) not in docker_bots_canonical:
                        self.mqtt_manager.clear_bot_data(bot_name)
                        del self.active_bots[bot_name]

                for bot_name in all_active_bots:
                    canonical_bot_name = _to_canonical_bot_id(bot_name)
                    resolved_source = "docker+mqtt" if canonical_bot_name in mqtt_bots_canonical else "docker"
                    resolved_bot_name = self._resolve_bot_name(bot_name) or bot_name
                    if resolved_bot_name not in self.active_bots:
                        self.active_bots[resolved_bot_name] = {
                            "bot_name": bot_name,
                            "status": "connected",
                            "source": resolved_source,
                        }
                        await self.mqtt_manager.subscribe_to_bot(bot_name)
                    else:
                        self.active_bots[resolved_bot_name]["source"] = resolved_source

            except Exception as e:
                logger.error(f"Error in update_active_bots: {e}", exc_info=True)

            await asyncio.sleep(sleep_time)

    # ============================================
    # Bot control via MQTT
    # ============================================

    async def start_bot(self, bot_name, **kwargs):
        if bot_name not in self.active_bots:
            logger.warning(f"Bot {bot_name} not found in active bots")
            return {"success": False, "message": f"Bot {bot_name} not found"}
        data = {
            "log_level": kwargs.get("log_level"),
            "script": kwargs.get("script"),
            "conf": kwargs.get("conf"),
            "is_quickstart": kwargs.get("is_quickstart", False),
            "async_backend": kwargs.get("async_backend", True),
        }
        success = await self.mqtt_manager.publish_command(bot_name, "start", data)
        return {"success": success}

    async def stop_bot(self, bot_name, **kwargs):
        if bot_name not in self.active_bots:
            logger.warning(f"Bot {bot_name} not found in active bots")
            return {"success": False, "message": f"Bot {bot_name} not found"}
        data = {
            "skip_order_cancellation": kwargs.get("skip_order_cancellation", False),
            "async_backend": kwargs.get("async_backend", True),
        }
        success = await self.mqtt_manager.publish_command(bot_name, "stop", data)
        if success:
            self.mqtt_manager.clear_bot_controller_reports(bot_name)
        return {"success": success}

    async def import_strategy_for_bot(self, bot_name, strategy, **kwargs):
        if bot_name not in self.active_bots:
            return {"success": False, "message": f"Bot {bot_name} not found"}
        data = {"strategy": strategy}
        success = await self.mqtt_manager.publish_command(bot_name, "import_strategy", data)
        return {"success": success}

    async def configure_bot(self, bot_name, params, **kwargs):
        if bot_name not in self.active_bots:
            return {"success": False, "message": f"Bot {bot_name} not found"}
        data = {"params": params}
        success = await self.mqtt_manager.publish_command(bot_name, "config", data)
        return {"success": success}

    async def get_bot_history(self, bot_name, **kwargs):
        if bot_name not in self.active_bots:
            return {"success": False, "message": f"Bot {bot_name} not found"}
        data = {
            "days": kwargs.get("days", 0),
            "verbose": kwargs.get("verbose", False),
            "precision": kwargs.get("precision"),
            "async_backend": kwargs.get("async_backend", False),
        }
        timeout = kwargs.get("timeout", 30.0)
        response = await self.mqtt_manager.publish_command_and_wait(bot_name, "history", data, timeout=timeout)
        if response is None:
            return {"success": False, "message": f"No response from {bot_name} within {timeout}s", "timeout": True}
        return {"success": True, "data": response}

    # ============================================
    # Bot Run Tracking (stubs — no DB table yet)
    # ============================================

    async def create_bot_run(self, bot_name: str, instance_name: str = None,
                             strategy_type: str = None, strategy_name: str = None,
                             account_name: str = None, config_name: str = None,
                             image_version: str = None, deployment_config: dict = None) -> Optional[int]:
        """Track a new bot deployment. Stub: logs only, no DB table required."""
        logger.info(
            f"[bot_run] create | bot={bot_name} strategy_type={strategy_type} "
            f"strategy={strategy_name} account={account_name} image={image_version}"
        )
        return None

    async def mark_bot_run_stopped(self, bot_name: str, final_status: dict = None) -> bool:
        """Mark a bot run as stopped. Stub: logs only."""
        logger.info(f"[bot_run] stopped | bot={bot_name}")
        return True

    async def get_bot_runs(self, bot_name: str = None, account_name: str = None,
                           strategy_type: str = None, strategy_name: str = None,
                           run_status: str = None, deployment_status: str = None,
                           limit: int = 100, offset: int = 0) -> List[Dict]:
        """Return bot runs. Stub: returns empty list (no DB table)."""
        return []

    async def get_bot_run_stats(self) -> Dict:
        """Return bot run statistics. Stub: returns zeros."""
        return {"total": 0, "running": 0, "stopped": 0, "error": 0}

    async def get_bot_run_by_id(self, bot_run_id: int) -> Optional[Dict]:
        """Return a bot run by ID. Stub: always returns None."""
        return None

    async def delete_bot_run(self, bot_run_id: int) -> Optional[Dict]:
        """Delete a bot run record. Stub: always returns None."""
        return None

    # ============================================
    # Stop & Archive
    # ============================================

    async def stop_and_archive_bot(self, bot_name: str, container_name: str = None,
                                   bot_name_for_orchestrator: str = None,
                                   skip_order_cancellation: bool = True,
                                   archive_locally: bool = True, s3_bucket: str = None,
                                   docker_manager=None, bot_archiver=None):
        """Stop a bot and archive its data."""
        actual_bot = bot_name_for_orchestrator or bot_name
        container = container_name or bot_name
        self.set_bot_stopping(actual_bot)
        try:
            await self.stop_bot(actual_bot, skip_order_cancellation=skip_order_cancellation)
            await asyncio.sleep(15)
            if docker_manager:
                docker_manager.stop_container(container)
            if archive_locally and bot_archiver:
                bot_archiver.archive_locally(container)
            if s3_bucket and bot_archiver:
                bot_archiver.upload_to_s3(container, s3_bucket)
            if docker_manager:
                docker_manager.remove_container(container)
        except Exception as e:
            logger.error(f"Error stopping/archiving {bot_name}: {e}")
        finally:
            self.clear_bot_stopping(actual_bot)

    # ============================================
    # Status helpers
    # ============================================

    @staticmethod
    def determine_controller_performance(controller_reports):
        cleaned_data = {}
        for controller_id, report in controller_reports.items():
            try:
                if "performance" in report:
                    performance = report.get("performance", {})
                    custom_info = report.get("custom_info", {})
                else:
                    performance = report
                    custom_info = {}
                non_numeric_fields = ("positions_summary", "close_type_counts")
                _ = sum(
                    metric for key, metric in performance.items()
                    if key not in non_numeric_fields and isinstance(metric, (int, float))
                )
                cleaned_data[controller_id] = {"status": "running", "performance": performance, "custom_info": custom_info}
            except Exception as e:
                perf = report.get("performance", {}) if "performance" in report else report
                info = report.get("custom_info", {}) if "performance" in report else {}
                cleaned_data[controller_id] = {"status": "error", "error": str(e), "performance": perf, "custom_info": info}
        return cleaned_data

    def get_all_bots_status(self):
        all_bots_status = {}
        for bot in [b for b in self.active_bots if not self.is_bot_stopping(b)]:
            status = self.get_bot_status(bot)
            status.setdefault("source", self.active_bots[bot].get("source", "unknown"))
            all_bots_status[bot] = status
        return all_bots_status

    def _resolve_bot_name(self, bot_name: str) -> Optional[str]:
        if bot_name in self.active_bots:
            return bot_name
        canonical = _to_canonical_bot_id(bot_name)
        if canonical in self.active_bots:
            return canonical
        for active in self.active_bots:
            if _to_canonical_bot_id(active) == canonical:
                return active
        return None

    def get_bot_status(self, bot_name):
        resolved = self._resolve_bot_name(bot_name)
        if resolved is None:
            return {"status": "not_found", "error": f"Bot {bot_name} not found"}
        bot_name = resolved
        try:
            if bot_name in self.stopping_bots:
                return {"status": "stopping", "message": "Bot is being stopped",
                        "performance": {}, "error_logs": [], "general_logs": [], "recently_active": False}
            controller_reports = self.mqtt_manager.get_bot_controller_reports(bot_name)
            performance = self.determine_controller_performance(controller_reports)
            error_logs = self.mqtt_manager.get_bot_error_logs(bot_name)
            general_logs = self.mqtt_manager.get_bot_logs(bot_name)
            docker_bots = {_to_canonical_bot_id(name) for name in self._sync_get_active_containers()}
            discovered_bots = {
                _to_canonical_bot_id(b)
                for b in self.mqtt_manager.get_discovered_bots(timeout_seconds=30)
            }
            canonical = _to_canonical_bot_id(bot_name)
            docker_running = canonical in docker_bots
            recently_active = canonical in discovered_bots
            active_source = self.active_bots.get(bot_name, {}).get("source", "unknown")
            if docker_running:
                status = "running"
                source = "docker"
                connection_state = "healthy" if recently_active else "degraded"
            elif recently_active:
                status = "disconnected"
                source = "mqtt"
                connection_state = "mqtt_only"
            elif performance:
                status = "stopped"
                source = active_source
                connection_state = "stale"
            else:
                status, source = "stopped", active_source
                connection_state = "offline"
            return {"status": status, "source": source, "performance": performance,
                    "error_logs": error_logs, "general_logs": general_logs,
                    "recently_active": recently_active, "docker_running": docker_running,
                    "connection_state": connection_state}
        except Exception as e:
            logger.exception("Failed to get bot status for '%s'", bot_name)
            return {"status": "error", "error": str(e)}

    def set_bot_stopping(self, bot_name: str):
        self.stopping_bots.add(bot_name)

    def clear_bot_stopping(self, bot_name: str):
        self.stopping_bots.discard(bot_name)

    def is_bot_stopping(self, bot_name: str) -> bool:
        return bot_name in self.stopping_bots

    # ============================================
    # Controller Performance Snapshots
    # ============================================

    async def _ensure_db_initialized(self):
        if not self._db_initialized:
            await self.db_manager.create_tables()
            self._db_initialized = True

    async def _performance_dump_loop(self):
        while True:
            try:
                await self.dump_controller_performance()
            except Exception as e:
                logger.error(f"Error dumping controller performance: {e}")
            finally:
                await asyncio.sleep(self.performance_dump_interval)

    async def dump_controller_performance(self):
        await self._ensure_db_initialized()
        snapshot_timestamp = datetime.now(timezone.utc)
        saved_count = 0
        try:
            async with self.db_manager.get_session_context() as session:
                repo = ControllerPerformanceRepository(session)
                for bot_name in list(self.active_bots):
                    if self.is_bot_stopping(bot_name):
                        continue
                    controller_reports = self.mqtt_manager.get_bot_controller_reports(bot_name)
                    performance_data = self.determine_controller_performance(controller_reports)
                    for controller_id, data in performance_data.items():
                        await repo.save_controller_performance(
                            bot_name=bot_name, controller_id=controller_id,
                            status=data.get("status", "unknown"),
                            performance=data.get("performance", {}),
                            custom_info=data.get("custom_info", {}),
                            snapshot_timestamp=snapshot_timestamp,
                        )
                        saved_count += 1
            if saved_count > 0:
                logger.info(f"Dumped {saved_count} controller performance snapshots")
        except Exception as e:
            logger.error(f"Error saving controller performance: {e}")
            raise

    async def get_controller_performance_history(self, bot_name=None, controller_id=None,
                                                  limit=None, cursor=None, start_time=None,
                                                  end_time=None, interval="5m"):
        await self._ensure_db_initialized()
        try:
            async with self.db_manager.get_session_context() as session:
                repo = ControllerPerformanceRepository(session)
                return await repo.get_performance_history(
                    bot_name=bot_name, controller_id=controller_id, limit=limit,
                    cursor=cursor, start_time=start_time, end_time=end_time, interval=interval
                )
        except Exception as e:
            logger.error(f"Error getting performance history: {e}")
            return [], None, False

    async def get_latest_controller_performance(self, bot_name=None) -> List[Dict]:
        await self._ensure_db_initialized()
        try:
            async with self.db_manager.get_session_context() as session:
                repo = ControllerPerformanceRepository(session)
                return await repo.get_latest_performance(bot_name=bot_name)
        except Exception as e:
            logger.error(f"Error getting latest performance: {e}")
            return []
