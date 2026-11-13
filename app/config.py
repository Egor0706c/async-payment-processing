from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: str = "postgresql+asyncpg://payments:payments@localhost:55432/payments"
    rabbitmq_url: str = "amqp://payments:payments@localhost:5673/"
    api_key: str = Field(min_length=16)  # Required; never silently enable a public default.
    retry_base_seconds: float = Field(default=1, gt=0)
    gateway_min_seconds: float = Field(default=2, ge=0)
    gateway_max_seconds: float = Field(default=5, ge=0)


settings = Settings()
