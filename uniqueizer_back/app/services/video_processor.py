import asyncio
import shutil
import zipfile
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import uuid
import logging

from app.services.uniquifier import VideoUniquifier
from app.config import settings
from app.utils.file_handler import cleanup_file

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VideoProcessor:
    """
    Основной процессор для обработки видео
    """
    
    def __init__(self):
        self.uniquifier = VideoUniquifier()
        self.active_tasks: Dict[str, Dict] = {}
        self.cleanup_task = None
    
    async def start_cleanup_scheduler(self):
        """
        Запускает фоновую задачу для периодической очистки
        """
        if self.cleanup_task is None:
            self.cleanup_task = asyncio.create_task(self._cleanup_scheduler())
            logger.info("Cleanup scheduler started")
    
    async def _cleanup_scheduler(self):
        """
        Периодически запускает очистку старых файлов
        """
        while True:
            try:
                # Ждем 1 час между проверками
                await asyncio.sleep(3600)
                
                logger.info("Running scheduled cleanup...")
                await self.cleanup_old_tasks(hours=settings.temp_file_cleanup_hours)
                
            except asyncio.CancelledError:
                logger.info("Cleanup scheduler cancelled")
                break
            except Exception as e:
                logger.error(f"Error in cleanup scheduler: {str(e)}", exc_info=True)
    
    async def process_video(
        self, 
        input_file: Path, 
        copies_count: int,
        output_format: str = "mp4"
    ) -> str:
        """
        Обрабатывает видео и создает N уникальных копий
        
        Returns:
            task_id для отслеживания прогресса
        """
        task_id = str(uuid.uuid4())
        
        # Создаем директорию для этой задачи
        task_dir = settings.output_dir / task_id
        task_dir.mkdir(exist_ok=True)
        
        logger.info(f"Created task {task_id}, output dir: {task_dir}")
        logger.info(f"Input file: {input_file}, exists: {input_file.exists()}")
        
        # Инициализируем задачу
        self.active_tasks[task_id] = {
            'status': 'processing',
            'progress': 0,
            'total': copies_count,
            'files': [],
            'created_at': datetime.now(),
            'last_accessed': datetime.now(),
            'task_dir': str(task_dir),
            'input_file': str(input_file),
        }
        
        # Запускаем обработку в фоне
        asyncio.create_task(
            self._process_task(task_id, input_file, copies_count, task_dir, output_format)
        )
        
        return task_id
    
    async def _process_task(
        self,
        task_id: str,
        input_file: Path,
        copies_count: int,
        task_dir: Path,
        output_format: str
    ):
        """
        Внутренний метод для обработки задачи
        """
        try:
            logger.info(f"Starting processing task {task_id}")
            logger.info(f"Input file exists: {input_file.exists()}, size: {input_file.stat().st_size if input_file.exists() else 0}")
            
            created_files = []
            
            for i in range(1, copies_count + 1):
                output_filename = f"video_{i:03d}.{output_format}"
                output_path = task_dir / output_filename
                
                logger.info(f"Creating unique copy {i}/{copies_count}: {output_path}")
                
                # Создаем уникальную копию
                success = await asyncio.to_thread(
                    self.uniquifier.create_unique_copy,
                    input_file,
                    output_path,
                    i,
                    copies_count
                )
                
                if success and output_path.exists():
                    created_files.append(output_filename)
                    logger.info(f"Successfully created {output_filename}, size: {output_path.stat().st_size} bytes")
                else:
                    logger.error(f"Failed to create {output_filename}")
                    
                # Обновляем прогресс
                self.active_tasks[task_id]['progress'] = i
                self.active_tasks[task_id]['files'] = created_files
                self.active_tasks[task_id]['last_accessed'] = datetime.now()
                
                # Небольшая пауза чтобы не перегружать систему
                await asyncio.sleep(0.1)
            
            logger.info(f"Task {task_id}: created {len(created_files)} files")
            
            # Создаем архив со всеми файлами
            if created_files:
                archive_path = await self._create_archive(task_id, task_dir, created_files)
                
                if archive_path and archive_path.exists():
                    logger.info(f"Archive created: {archive_path}, size: {archive_path.stat().st_size} bytes")
                    self.active_tasks[task_id]['archive'] = archive_path.name
                else:
                    logger.error(f"Failed to create archive for task {task_id}")
            
            # Обновляем статус
            self.active_tasks[task_id].update({
                'status': 'completed',
                'completed_at': datetime.now(),
                'last_accessed': datetime.now(),
            })
            
            logger.info(f"Task {task_id} completed successfully")
            
            # Удаляем входной файл только после завершения обработки
            if input_file.exists():
                logger.info(f"Cleaning up input file: {input_file}")
                cleanup_file(input_file)
            
        except Exception as e:
            logger.error(f"Task {task_id} failed: {str(e)}", exc_info=True)
            self.active_tasks[task_id].update({
                'status': 'failed',
                'error': str(e),
                'last_accessed': datetime.now(),
            })
            
            # Удаляем входной файл даже при ошибке
            if input_file.exists():
                cleanup_file(input_file)
    
    async def _create_archive(
        self, 
        task_id: str, 
        task_dir: Path, 
        files: List[str]
    ) -> Optional[Path]:
        """
        Создает ZIP архив со всеми файлами
        """
        try:
            archive_path = task_dir / f"videos_{task_id}.zip"
            
            logger.info(f"Creating archive: {archive_path}")
            logger.info(f"Files to archive: {files}")
            
            def create_zip():
                with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=5) as zipf:
                    for filename in files:
                        file_path = task_dir / filename
                        if file_path.exists():
                            file_size = file_path.stat().st_size
                            logger.info(f"Adding to archive: {filename} ({file_size} bytes)")
                            zipf.write(file_path, filename)
                        else:
                            logger.error(f"File not found for archiving: {file_path}")
                
                # Проверяем что архив создался и не пустой
                if archive_path.exists():
                    archive_size = archive_path.stat().st_size
                    logger.info(f"Archive created successfully, size: {archive_size} bytes")
                    
                    # Проверяем содержимое архива
                    with zipfile.ZipFile(archive_path, 'r') as zipf:
                        file_list = zipf.namelist()
                        logger.info(f"Archive contains {len(file_list)} files: {file_list}")
                else:
                    logger.error("Archive file was not created")
            
            await asyncio.to_thread(create_zip)
            
            return archive_path if archive_path.exists() else None
            
        except Exception as e:
            logger.error(f"Error creating archive: {str(e)}", exc_info=True)
            return None
    
    def get_task_status(self, task_id: str) -> Optional[Dict]:
        """
        Получает статус задачи и обновляет время последнего доступа
        """
        task = self.active_tasks.get(task_id)
        if task:
            task['last_accessed'] = datetime.now()
        return task
    
    def get_task_files(self, task_id: str) -> Optional[Path]:
        """
        Возвращает директорию с файлами задачи и обновляет время доступа
        """
        task = self.active_tasks.get(task_id)
        if task:
            task['last_accessed'] = datetime.now()
        
        task_dir = settings.output_dir / task_id
        if task_dir.exists():
            return task_dir
        return None
    
    async def cleanup_old_tasks(self, hours: int = 24):
        """
        Удаляет старые задачи
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        tasks_to_remove = []
        cleaned_count = 0
        freed_space = 0
        
        # Проверяем задачи в памяти
        for task_id, task_data in self.active_tasks.items():
            last_accessed = task_data.get('last_accessed', task_data['created_at'])
            
            if last_accessed < cutoff_time:
                tasks_to_remove.append(task_id)
                
                # Подсчитываем освобождаемое место
                task_dir = settings.output_dir / task_id
                if task_dir.exists():
                    try:
                        dir_size = sum(f.stat().st_size for f in task_dir.rglob('*') if f.is_file())
                        freed_space += dir_size
                        
                        shutil.rmtree(task_dir)
                        logger.info(f"Cleaned up old task: {task_id}, freed {dir_size / (1024*1024):.2f} MB")
                        cleaned_count += 1
                    except Exception as e:
                        logger.error(f"Error cleaning task {task_id}: {str(e)}")
        
        # Удаляем задачи из памяти
        for task_id in tasks_to_remove:
            del self.active_tasks[task_id]
        
        # Проверяем директории без задач в памяти (orphaned)
        if settings.output_dir.exists():
            for task_dir in settings.output_dir.iterdir():
                if task_dir.is_dir():
                    task_id = task_dir.name
                    
                    # Если задачи нет в памяти, проверяем по времени модификации
                    if task_id not in self.active_tasks:
                        try:
                            mtime = datetime.fromtimestamp(task_dir.stat().st_mtime)
                            if mtime < cutoff_time:
                                dir_size = sum(f.stat().st_size for f in task_dir.rglob('*') if f.is_file())
                                shutil.rmtree(task_dir)
                                freed_space += dir_size
                                cleaned_count += 1
                                logger.info(f"Cleaned up orphaned task directory: {task_id}, freed {dir_size / (1024*1024):.2f} MB")
                        except Exception as e:
                            logger.error(f"Error cleaning orphaned directory {task_id}: {str(e)}")
        
        # Очищаем старые загруженные файлы
        if settings.upload_dir.exists():
            for upload_file in settings.upload_dir.iterdir():
                if upload_file.is_file():
                    try:
                        mtime = datetime.fromtimestamp(upload_file.stat().st_mtime)
                        if mtime < cutoff_time:
                            file_size = upload_file.stat().st_size
                            upload_file.unlink()
                            freed_space += file_size
                            logger.info(f"Cleaned up old upload: {upload_file.name}")
                    except Exception as e:
                        logger.error(f"Error cleaning upload {upload_file}: {str(e)}")
        
        if cleaned_count > 0:
            logger.info(f"Cleanup completed: removed {cleaned_count} tasks, freed {freed_space / (1024*1024*1024):.2f} GB")
        else:
            logger.info("Cleanup completed: no old tasks found")
        
        return cleaned_count, freed_space
    
    async def get_storage_info(self) -> Dict:
        """
        Возвращает информацию о использовании дискового пространства
        """
        total_size = 0
        file_count = 0
        
        if settings.output_dir.exists():
            for task_dir in settings.output_dir.iterdir():
                if task_dir.is_dir():
                    for f in task_dir.rglob('*'):
                        if f.is_file():
                            total_size += f.stat().st_size
                            file_count += 1
        
        return {
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'total_size_gb': round(total_size / (1024 * 1024 * 1024), 2),
            'file_count': file_count,
            'task_count': len(self.active_tasks),
        }
