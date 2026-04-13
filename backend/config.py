from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    llm_model: str = "gpt-5.4"
    llm_base_url: str = "https://api.openai.com/v1"
    llm_api_key: str = ""
    max_script_retries: int = 3
    max_validation_retries: int = 2
    preview_row_count: int = 20
    default_row_count: int = 1000
    script_timeout_seconds: int = 120
    output_dir: str = "output"
    log_level: str = "INFO"
    log_file: str = "logs/app.log"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
