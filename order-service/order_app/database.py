import logging

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base

from .config import settings

logger = logging.getLogger(__name__)

engine = create_engine(
    f"postgresql://{settings.order_db_user}:{settings.order_db_password}@{settings.order_db_host}:{settings.order_db_port}/{settings.order_db_name}",
    pool_pre_ping=True,
    pool_recycle=3600,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    """Dependency that yields a SQLAlchemy session per request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Create all tables registered on Base.metadata.

    A startup connection failure is logged but does not prevent the app from
    booting — the /health endpoint reports database status instead.
    """
    try:
        Base.metadata.create_all(bind=engine)
    except Exception:  # pragma: no cover - startup resilience
        logger.warning(
            "Database not reachable at startup; schema not initialised.", exc_info=True
        )


def check_db_health() -> bool:
    """Return True when the database accepts a lightweight query.

    Returns False (instead of raising) so /health can report a degraded
    status when the database is unreachable.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
