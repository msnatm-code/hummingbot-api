from fastapi import APIRouter, Depends
from hummingbot.client.settings import AllConnectorSettings

from deps import (
    get_accounts_service,
    get_bots_orchestrator,
    get_connector_service,
    get_docker_service,
)
from services.accounts_service import AccountsService
from services.bots_orchestrator import BotsOrchestrator
from services.docker_service import DockerService
from services.unified_connector_service import UnifiedConnectorService

router = APIRouter(tags=["Health"], prefix="")


def _safe_bool(value) -> bool:
    return bool(value)


@router.get("/health")
async def healthz():
    """Basic liveness probe for the API process itself."""
    return {
        "status": "ok",
        "service": "hummingbot-api",
        "kind": "liveness",
    }


@router.get("/ready")
async def readyz(
    docker_service: DockerService = Depends(get_docker_service),
    bots_orchestrator: BotsOrchestrator = Depends(get_bots_orchestrator),
    accounts_service: AccountsService = Depends(get_accounts_service),
    connector_service: UnifiedConnectorService = Depends(get_connector_service),
):
    """Composite readiness endpoint for MiTAHbot/Hummingbot orchestration."""
    checks = {}

    docker_ok = False
    try:
        docker_ok = _safe_bool(docker_service.is_docker_running())
        checks["docker"] = {"ok": docker_ok}
    except Exception as exc:
        checks["docker"] = {"ok": False, "error": str(exc)}

    mqtt_ok = False
    try:
        mqtt_ok = _safe_bool(bots_orchestrator.mqtt_manager.is_connected)
        discovered_bots = bots_orchestrator.mqtt_manager.get_discovered_bots(timeout_seconds=30)
        checks["mqtt"] = {
            "ok": mqtt_ok,
            "discovered_bots": discovered_bots,
        }
    except Exception as exc:
        checks["mqtt"] = {"ok": False, "error": str(exc)}

    active_bots_ok = False
    try:
        active_bots = bots_orchestrator.get_all_bots_status()
        active_bots_ok = True
        checks["bot_orchestration"] = {
            "ok": True,
            "active_bot_count": len(active_bots) if isinstance(active_bots, dict) else 0,
            "active_bots": list(active_bots.keys()) if isinstance(active_bots, dict) else active_bots,
        }
    except Exception as exc:
        checks["bot_orchestration"] = {"ok": False, "error": str(exc)}

    accounts_ok = False
    try:
        accounts_state = accounts_service.get_accounts_state()
        accounts_ok = True
        checks["accounts"] = {
            "ok": True,
            "account_count": len(accounts_state) if isinstance(accounts_state, dict) else 0,
            "accounts": list(accounts_state.keys()) if isinstance(accounts_state, dict) else [],
        }
    except Exception as exc:
        checks["accounts"] = {"ok": False, "error": str(exc)}

    connectors_ok = False
    try:
        all_connectors = AllConnectorSettings.get_connector_settings().keys()
        available_connectors = [connector for connector in all_connectors if "/" not in connector]
        connectors_ok = True
        checks["connectors"] = {
            "ok": True,
            "count": len(available_connectors),
            "sample": available_connectors[:20],
        }
    except Exception as exc:
        checks["connectors"] = {"ok": False, "error": str(exc)}

    overall_ok = docker_ok and mqtt_ok and active_bots_ok and accounts_ok and connectors_ok

    return {
        "status": "ok" if overall_ok else "degraded",
        "service": "hummingbot-api",
        "kind": "readiness",
        "checks": checks,
    }