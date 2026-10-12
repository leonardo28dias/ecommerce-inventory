from contextlib import asynccontextmanager
import threading

from fastapi import FastAPI

from . import models  # noqa: F401  (registers the orders table)
from .config import settings
from .database import SessionLocal, init_db
from .health import router as health_router
from .logging_config import configure_logging
from .routes import router as orders_router
from .messaging import consume_stock_events


_consumer_thread: threading.Thread | None = None


def _start_consumer():
    """Start the RabbitMQ consumer in a background thread."""
    global _consumer_thread
    if _consumer_thread is not None and _consumer_thread.is_alive():
        return

    def run_consumer():
        try:
            consume_stock_events(SessionLocal)
        except Exception as exc:
            # Log but don't crash the main process
            import logging
            logging.getLogger(__name__).error("Consumer thread crashed: %s", exc)

    _consumer_thread = threading.Thread(target=run_consumer, daemon=True, name="rabbitmq-consumer")
    _consumer_thread.start()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup hook: configure logging, create tables, verify the database."""
    configure_logging(settings.log_level)
    init_db()
    _start_consumer()
    yield


app = FastAPI(
    title="Order Service",
    version="0.2.0",
    description=(
        "Order service for the BE-3 multi-tenant, event-driven e-commerce system."
    ),
    lifespan=lifespan,
)

app.include_router(health_router)
app.include_router(orders_router)
