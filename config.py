from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kafka_bootstrap: str = "localhost:9092"
    kafka_vitals_topic: str = "vitals-raw"
    kafka_triage_topic: str = "triage-investigation"
    kafka_dlq_topic: str = "triage-dead-letter"

    max_retries: int = 3

    redis_host: str = "localhost"
    redis_port: int = 6379

    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "clinical_alerts"
    mongo_collection: str = "alerts"

    ollama_host: str = "http://localhost:11434"
    ollama_model: str = "llama3"

    sqi_min: float = 0.6
    agent_thread_workers: int = 4

    class Config:
        env_file = ".env"


settings = Settings()
