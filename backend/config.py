from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    llm_model: str = "gpt-5.1"
    azure_openai_endpoint: str = ""
    azure_openai_deployment_name: str = ""
    azure_openai_api_key: str = ""
    azure_openai_api_version: str = ""
    max_script_retries: int = 3
    max_validation_retries: int = 2
    preview_row_count: int = 100
    default_row_count: int = 1000
    script_timeout_seconds: int = 120
    output_dir: str = "output"
    log_level: str = "INFO"
    log_file: str = "logs/app.log"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
