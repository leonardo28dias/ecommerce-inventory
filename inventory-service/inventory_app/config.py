from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application configuration loaded from .env or the environment."""

    inventory_service_port: int = 8002
    inventory_db_host: str = "inventory-db"
    inventory_db_port: int = 5432
    inventory_db_name: str = "inventorydb"
    inventory_db_user: str = "postgres"
    inventory_db_password: str = ""  # secret; provided via environment (see .env.example)

    rabbitmq_host: str = "rabbitmq"
    rabbitmq_port: int = 5672
    rabbitmq_ui_port: int = 15672
    rabbitmq_user: str = "guest"
    rabbitmq_password: str = ""  # secret; provided via environment (see .env.example)

    log_level: str = "INFO"


settings = Settings()
