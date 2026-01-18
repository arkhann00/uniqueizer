from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks, Form
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import uuid
from typing import Optional
import logging
import subprocess
import shutil

from app.config import settings
from app.models import ProcessStatus, ProcessResult
from app.services.video_processor import VideoProcessor
from app.utils.file_handler import save_upload_file, cleanup_file

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Video Uniquifier API",
    description="Сервис для создания технически уникальных копий видео",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Глобальный процессор
processor = VideoProcessor()


@app.on_event("startup")
async def startup_event():
    """
    Инициализация при запуске
    """
    logger.info("🚀 Video Uniquifier API started")
    logger.info(f"📁 Upload directory: {settings.upload_dir}")
    logger.info(f"📁 Output directory: {settings.output_dir}")
    logger.info(f"⏰ Auto-cleanup after: {settings.temp_file_cleanup_hours} hours")
    
    # Проверяем наличие FFmpeg
    import shutil as sh
    ffmpeg_path = sh.which('ffmpeg')
    if ffmpeg_path:
        logger.info(f"✅ FFmpeg found at: {ffmpeg_path}")
        try:
            result = subprocess.run(
                ['ffmpeg', '-version'],
                capture_output=True,
                text=True,
                timeout=5
            )
            version_line = result.stdout.split('\n')[0]
            logger.info(f"📹 {version_line}")
        except Exception as e:
            logger.warning(f"Could not check FFmpeg version: {e}")
    else:
        logger.error("❌ FFmpeg NOT FOUND! Please install FFmpeg")
    
    # Запускаем планировщик очистки
    await processor.start_cleanup_scheduler()
    logger.info("🧹 Cleanup scheduler started")


@app.on_event("shutdown")
async def shutdown_event():
    """
    Действия при остановке
    """
    logger.info("👋 Shutting down Video Uniquifier API")


@app.get("/")
async def root():
    """
    Проверка работы API
    """
    return {
        "message": "Video Uniquifier API",
        "version": "1.0.0",
        "status": "running"
    }


@app.post("/api/upload", response_model=ProcessStatus)
async def upload_video(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    copies_count: int = Form(..., ge=1, le=100),  # Добавлено ... для обязательного поля
    output_format: str = Form(default="mp4")
):
    """
    Загружает видео и запускает процесс уникализации
    """
    logger.info(f"Received upload request: {file.filename}, copies: {copies_count}")
    
    # Проверка формата файла
    allowed_extensions = {'.mp4', '.mov', '.avi', '.mkv', '.webm'}
    file_ext = Path(file.filename).suffix.lower()
    
    if file_ext not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail=f"Неподдерживаемый формат. Разрешены: {', '.join(allowed_extensions)}"
        )
    
    # Генерируем уникальное имя файла
    upload_id = str(uuid.uuid4())
    temp_file = settings.upload_dir / f"{upload_id}{file_ext}"
    
    try:
        logger.info(f"Saving uploaded file to: {temp_file}")
        await save_upload_file(file, temp_file)
        
        file_size = temp_file.stat().st_size
        logger.info(f"File saved successfully, size: {file_size} bytes")
        
        if not temp_file.exists():
            raise Exception("Uploaded file was not saved properly")
        
        # Запускаем обработку
        task_id = await processor.process_video(
            temp_file,
            copies_count,
            output_format
        )
        
        logger.info(f"Processing started with task_id: {task_id}")
        
        return ProcessStatus(
            task_id=task_id,
            status="processing",
            progress=0,
            total_copies=copies_count,
            message="Обработка началась"
        )
        
    except Exception as e:
        logger.error(f"Upload error: {str(e)}", exc_info=True)
        cleanup_file(temp_file)
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки: {str(e)}")


@app.get("/api/status/{task_id}", response_model=ProcessStatus)
async def get_status(task_id: str):
    """
    Получает статус обработки задачи
    """
    task = processor.get_task_status(task_id)
    
    # Если задачи нет в памяти, проверяем файловую систему
    if not task:
        logger.warning(f"⚠️  Task not in memory, checking filesystem: {task_id}")
        
        task_dir = settings.output_dir / task_id
        
        # Проверяем существование директории задачи
        if task_dir.exists():
            # Считаем сколько видео уже создано
            video_files = list(task_dir.glob("video_*.mp4"))
            zip_files = list(task_dir.glob("*.zip"))
            
            # Если есть архив - задача завершена
            if zip_files:
                logger.info(f"✅ Task completed (found on disk): {task_id}")
                return ProcessStatus(
                    task_id=task_id,
                    status="completed",
                    progress=100,
                    total_copies=len(video_files),
                    message="Обработка завершена"
                )
            
            # Если только видео без архива - еще обрабатывается
            if video_files:
                # Примерный прогресс по количеству файлов
                # Предполагаем что всего будет 10 копий (стандарт)
                estimated_total = 10  # можно брать из параметров запроса
                progress = int((len(video_files) / estimated_total) * 100)
                
                logger.info(f"📊 Task in progress (found {len(video_files)} videos): {task_id}")
                return ProcessStatus(
                    task_id=task_id,
                    status="processing",
                    progress=min(progress, 99),  # Не показываем 100% пока нет архива
                    total_copies=estimated_total,
                    message=f"Создано {len(video_files)} из {estimated_total} копий"
                )
        
        # Задача не найдена нигде
        logger.error(f"❌ Task not found: {task_id}")
        raise HTTPException(status_code=404, detail="Задача не найдена")
    
    # Задача есть в памяти - возвращаем ее статус
    return ProcessStatus(
        task_id=task_id,
        status=task['status'],
        progress=task['progress'],
        total_copies=task.get('total', 10),
        message=task.get('error')
    )



@app.get("/api/result/{task_id}", response_model=ProcessResult)
async def get_result(task_id: str):
    """
    Получает результат обработки
    """
    task = processor.get_task_status(task_id)
    
    if not task:
        raise HTTPException(status_code=404, detail="Задача не найдена")
    
    if task['status'] != 'completed':
        raise HTTPException(
            status_code=400, 
            detail=f"Задача еще не завершена. Статус: {task['status']}"
        )
    
    archive_url = None
    if task.get('archive'):
        archive_url = f"/api/download/archive/{task_id}"
    
    logger.info(f"Result for task {task_id}: {len(task['files'])} files, archive: {task.get('archive')}")
    
    return ProcessResult(
        task_id=task_id,
        status=task['status'],
        files=task['files'],
        archive_url=archive_url
    )


def cleanup_task_after_download(task_id: str):
    """
    Фоновая задача для удаления файлов после скачивания
    """
    try:
        task_dir = settings.output_dir / task_id
        if task_dir.exists():
            # Подсчитываем освобождаемое место
            dir_size = sum(f.stat().st_size for f in task_dir.rglob('*') if f.is_file())
            
            # Удаляем директорию
            shutil.rmtree(task_dir)
            
            logger.info(f"✅ Cleaned up task {task_id} after download, freed {dir_size / (1024*1024):.2f} MB")
        
        # Удаляем задачу из памяти
        if task_id in processor.active_tasks:
            del processor.active_tasks[task_id]
            logger.info(f"Removed task {task_id} from active tasks")
            
    except Exception as e:
        logger.error(f"Error cleaning up task {task_id}: {str(e)}", exc_info=True)


@app.get("/api/download/archive/{task_id}")
async def download_archive(task_id: str, background_tasks: BackgroundTasks):
    """
    Скачивает архив со всеми видео и автоматически удаляет файлы
    """
    logger.info(f"🔍 Archive download request for task: {task_id}")
    
    # Прямая проверка файловой системы вместо processor
    task_dir = settings.output_dir / task_id
    
    logger.info(f"📁 Checking task directory: {task_dir}")
    
    if not task_dir.exists():
        logger.error(f"❌ Task directory not found: {task_dir}")
        raise HTTPException(status_code=404, detail="Задача не найдена")
    
    # Ищем zip файлы в директории
    zip_files = list(task_dir.glob("*.zip"))
    
    logger.info(f"📦 Found {len(zip_files)} zip files")
    
    if not zip_files:
        logger.error(f"❌ No zip files in directory")
        all_files = list(task_dir.iterdir())
        logger.error(f"Directory contains: {[f.name for f in all_files]}")
        raise HTTPException(status_code=404, detail="Архив не найден")
    
    # Берем первый zip файл
    archive_path = zip_files[0]
    archive_size = archive_path.stat().st_size
    
    logger.info(f"✅ Found archive: {archive_path.name}, size: {archive_size} bytes")
    
    if archive_size == 0:
        logger.error(f"❌ Archive is empty")
        raise HTTPException(status_code=500, detail="Архив пустой")
    
    # Планируем удаление после скачивания
    background_tasks.add_task(cleanup_task_after_download, task_id)
    logger.info(f"🗑️  Scheduled cleanup after download")
    
    return FileResponse(
        path=str(archive_path),
        filename=f"unique_videos_{task_id}.zip",
        media_type='application/zip',
        headers={
            "Content-Disposition": f'attachment; filename="unique_videos_{task_id}.zip"'
        }
    )



@app.get("/api/download/{task_id}/{filename}")
async def download_file(task_id: str, filename: str):
    """
    Скачивает отдельный файл
    """
    logger.info(f"Download request: task={task_id}, file={filename}")
    
    task_dir = processor.get_task_files(task_id)
    
    if not task_dir:
        logger.error(f"Task directory not found: {task_id}")
        raise HTTPException(status_code=404, detail="Задача не найдена")
    
    file_path = task_dir / filename
    
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        raise HTTPException(status_code=404, detail="Файл не найден")
    
    logger.info(f"Serving file: {file_path}, size: {file_path.stat().st_size}")
    
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type='application/octet-stream'
    )


@app.delete("/api/task/{task_id}")
async def delete_task(task_id: str):
    """
    Удаляет задачу и все связанные файлы
    """
    task_dir = processor.get_task_files(task_id)
    
    freed_space = 0
    if task_dir and task_dir.exists():
        # Подсчитываем размер
        freed_space = sum(f.stat().st_size for f in task_dir.rglob('*') if f.is_file())
        
        shutil.rmtree(task_dir)
        logger.info(f"Deleted task directory: {task_dir}, freed {freed_space / (1024*1024):.2f} MB")
    
    if task_id in processor.active_tasks:
        del processor.active_tasks[task_id]
    
    return {
        "message": "Задача удалена",
        "freed_mb": round(freed_space / (1024 * 1024), 2)
    }


@app.post("/api/cleanup")
async def manual_cleanup(hours: int = 24):
    """
    Ручной запуск очистки старых файлов
    """
    logger.info(f"Manual cleanup triggered for files older than {hours} hours")
    cleaned_count, freed_space = await processor.cleanup_old_tasks(hours)
    
    return {
        "message": "Cleanup completed",
        "tasks_removed": cleaned_count,
        "space_freed_mb": round(freed_space / (1024 * 1024), 2),
        "space_freed_gb": round(freed_space / (1024 * 1024 * 1024), 2),
    }


@app.get("/api/storage")
async def get_storage_info():
    """
    Получает информацию о использовании дискового пространства
    """
    storage_info = await processor.get_storage_info()
    return storage_info


@app.get("/api/health")
async def health_check():
    """
    Проверка здоровья сервиса
    """
    storage_info = await processor.get_storage_info()
    
    return {
        "status": "healthy",
        "active_tasks": len(processor.active_tasks),
        "storage_used_mb": storage_info['total_size_mb'],
        "storage_used_gb": storage_info['total_size_gb'],
        "file_count": storage_info['file_count'],
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
