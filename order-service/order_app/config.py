from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application configuration loaded from .env or the environment."""

    order_service_port: int = 8001
    order_db_host: str = "order-db"
    order_db_port: int = 5432
    order_db_name: str = "orderdb"
    order_db_user: str = "postgres"
    order_db_password: str = ""  # secret; provided via environment (see .env.example)

    rabbitmq_host: str = "rabbitmq"
    rabbitmq_port: int = 5672
    rabbitmq_ui_port: int = 15672
    rabbitmq_user: str = "guest"
    rabbitmq_password: str = ""  # secret; provided via environment (see .env.example)

    log_level: str = "INFO"


settings = Settings()
