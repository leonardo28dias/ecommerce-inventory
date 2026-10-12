import base64
import urllib.error
import urllib.request

from fastapi import APIRouter

from .config import settings
from .database import check_db_health

router = APIRouter()


def check_rabbitmq_health() -> bool:
    """Query the RabbitMQ management API to confirm the broker is reachable.

    The management API (``/api/overview``) requires HTTP basic auth, so we send
    the configured credentials. Without auth the API returns 401/403 and the
    service would incorrectly report RabbitMQ as ``error`` while AMQP
    publish/consume still work. Credentials default to ``guest`` so a local
    setup without explicit env vars still reports health truthfully.
    """
    url = "http://{0}:{1}/api/overview".format(settings.rabbitmq_host, settings.rabbitmq_ui_port)
    user = settings.rabbitmq_user or "guest"
    passwd = settings.rabbitmq_password or "guest"
    token = base64.b64encode(f"{user}:{passwd}".encode()).decode()
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "Authorization": f"Basic {token}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=3) as response:
            return response.status == 200
    except (urllib.error.URLError, OSError):
        return False


@router.get("/health")
def health():
    """Report that the service is alive and include dependency health.

    HTTP 200 indicates the service itself is alive (it responded). The body
    reports whether each dependency (database, RabbitMQ) is reachable so
    operators can spot degradation without taking the service offline.
    """
    db_ok = check_db_health()
    rabbitmq_ok = check_rabbitmq_health()

    services = {
        "database": "ok" if db_ok else "error",
        "rabbitmq": "ok" if rabbitmq_ok else "error",
    }
    status = "healthy" if all(v == "ok" for v in services.values()) else "degraded"

    return {"status": status, "services": services}
