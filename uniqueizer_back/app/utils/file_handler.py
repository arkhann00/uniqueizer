import aiofiles
from pathlib import Path
from fastapi import UploadFile
import shutil


async def save_upload_file(upload_file: UploadFile, destination: Path) -> Path:
    """
    Сохраняет загруженный файл на диск с поддержкой потоковой записи
    для больших файлов
    """
    try:
        async with aiofiles.open(destination, 'wb') as out_file:
            while content := await upload_file.read(1024 * 1024):  # Читаем по 1MB
                await out_file.write(content)
        return destination
    except Exception as e:
        if destination.exists():
            destination.unlink()
        raise e


def cleanup_file(file_path: Path):
    """
    Удаляет файл если он существует
    """
    try:
        if file_path.exists():
            file_path.unlink()
    except Exception as e:
        print(f"Error cleaning up file {file_path}: {str(e)}")


def get_file_size_mb(file_path: Path) -> float:
    """
    Возвращает размер файла в MB
    """
    return file_path.stat().st_size / (1024 * 1024)
