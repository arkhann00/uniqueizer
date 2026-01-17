from pydantic_settings import BaseSettings
from pathlib import Path


class Settings(BaseSettings):
    upload_dir: Path = Path("./uploads")
    output_dir: Path = Path("./outputs")
    max_file_size: int = 10 * 1024 * 1024 * 1024  # 10GB
    temp_file_cleanup_hours: int = 24  # Удалять файлы старше 24 часов
    
    backend_port: int = 8000
    frontend_port: int = 80
    class Config:
        env_file = ".env"


settings = Settings()

# Создаем директории если их нет
settings.upload_dir.mkdir(exist_ok=True)
settings.output_dir.mkdir(exist_ok=True)
