import subprocess
import random
import hashlib
from pathlib import Path
from typing import Dict, List
from datetime import datetime


class VideoUniquifier:
    """
    Класс для создания технически уникальных копий видео
    без визуальных изменений
    """
    
    def __init__(self):
        self.uniquification_strategies = [
            self._strategy_metadata,
            self._strategy_encoding_params,
            self._strategy_video_stream,
            self._strategy_audio_stream,
            self._strategy_timing,
        ]
    
    def create_unique_copy(
        self, 
        input_path: Path, 
        output_path: Path, 
        copy_number: int,
        total_copies: int
    ) -> bool:
        """
        Создает одну уникальную копию видео
        
        Args:
            input_path: путь к исходному файлу
            output_path: путь для сохранения копии
            copy_number: номер текущей копии
            total_copies: общее количество копий
            
        Returns:
            True если успешно, False при ошибке
        """
        try:
            params = self._generate_unique_params(copy_number, total_copies)
            command = self._build_ffmpeg_command(input_path, output_path, params)
            
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True
            )
            
            return output_path.exists()
            
        except subprocess.CalledProcessError as e:
            print(f"FFmpeg error: {e.stderr}")
            return False
        except Exception as e:
            print(f"Error creating unique copy: {str(e)}")
            return False
    
    def _generate_unique_params(self, copy_number: int, total_copies: int) -> Dict:
        """
        Генерирует уникальные параметры для каждой копии
        """
        # Используем номер копии как seed для воспроизводимости
        random.seed(copy_number)
        
        # Параметры FPS (микросдвиги)
        fps_variations = [23.976, 24, 25, 29.97, 30, 30.01, 50, 59.94, 60]
        fps_base = random.choice(fps_variations)
        fps_offset = random.uniform(-0.001, 0.001)
        
        # Параметры видео
        crf_variation = random.randint(17, 23)  # Высокое качество
        preset_options = ['slower', 'slow', 'medium']
        preset = random.choice(preset_options)
        
        # Параметры масштабирования (микроизменения)
        scale_factor = 1 + random.uniform(-0.005, 0.005)  # ±0.5%
        
        # Параметры GOP
        gop_size = random.randint(240, 260)
        
        # Параметры аудио
        audio_bitrate = random.choice(['192k', '256k', '320k'])
        audio_volume = 1.0 + random.uniform(-0.003, 0.003)  # ±0.3%
        
        # Метаданные
        timestamp = datetime.now().timestamp() + copy_number
        unique_id = hashlib.sha256(
            f"{copy_number}_{timestamp}_{random.random()}".encode()
        ).hexdigest()
        
        return {
            'fps': fps_base + fps_offset,
            'crf': crf_variation,
            'preset': preset,
            'scale_factor': scale_factor,
            'gop_size': gop_size,
            'audio_bitrate': audio_bitrate,
            'audio_volume': audio_volume,
            'creation_time': datetime.fromtimestamp(timestamp).isoformat(),
            'unique_id': unique_id,
            'copy_number': copy_number,
            'encoder_tag': f"UniqueEncoder_v{copy_number}",
            'pixel_format': random.choice(['yuv420p', 'yuv420p10le']),
            'b_frames': random.randint(2, 4),
            'ref_frames': random.randint(3, 5),
        }
    
    def _build_ffmpeg_command(
        self, 
        input_path: Path, 
        output_path: Path, 
        params: Dict
    ) -> List[str]:
        """
        Строит команду FFmpeg с параметрами уникализации
        """
        command = [
            'ffmpeg',
            '-i', str(input_path),
            '-y',  # Перезаписывать без подтверждения
        ]
        
        # Видео параметры
        video_filters = []
        
        # Микро-масштабирование (субпиксельное)
        if abs(params['scale_factor'] - 1.0) > 0.0001:
            video_filters.append(
                f"scale=iw*{params['scale_factor']}:ih*{params['scale_factor']}:flags=lanczos"
            )
        
        # Микро-сдвиг (субпиксельный)
        shift_x = random.uniform(-0.5, 0.5)
        shift_y = random.uniform(-0.5, 0.5)
        video_filters.append(f"crop=iw-1:ih-1:{shift_x}:{shift_y}")
        
        # Добавляем один пиксель обратно чтобы сохранить размер
        video_filters.append("pad=iw+1:ih+1:0:0")
        
        if video_filters:
            command.extend(['-vf', ','.join(video_filters)])
        
        # Кодек и параметры кодирования
        command.extend([
            '-c:v', 'libx264',
            '-preset', params['preset'],
            '-crf', str(params['crf']),
            '-g', str(params['gop_size']),  # GOP size
            '-bf', str(params['b_frames']),  # B-frames
            '-refs', str(params['ref_frames']),  # Reference frames
            '-pix_fmt', params['pixel_format'],
        ])
        
        # FPS
        command.extend(['-r', str(params['fps'])])
        
        # Аудио параметры
        command.extend([
            '-c:a', 'aac',
            '-b:a', params['audio_bitrate'],
            '-af', f"volume={params['audio_volume']}",
        ])
        
        # Метаданные
        command.extend([
            '-metadata', f"creation_time={params['creation_time']}",
            '-metadata', f"encoder={params['encoder_tag']}",
            '-metadata', f"comment=Unique_Copy_{params['copy_number']}",
            '-metadata', f"unique_id={params['unique_id']}",
            '-metadata', f"title=Video_{params['copy_number']:03d}",
        ])
        
        # Дополнительные параметры для уникальности
        command.extend([
            '-movflags', '+faststart',
            '-fflags', '+genpts',
        ])
        
        # Выходной файл
        command.append(str(output_path))
        
        return command
    
    def _strategy_metadata(self, params: Dict) -> Dict:
        """Стратегия уникализации через метаданные"""
        return params
    
    def _strategy_encoding_params(self, params: Dict) -> Dict:
        """Стратегия уникализации через параметры кодирования"""
        return params
    
    def _strategy_video_stream(self, params: Dict) -> Dict:
        """Стратегия уникализации видео потока"""
        return params
    
    def _strategy_audio_stream(self, params: Dict) -> Dict:
        """Стратегия уникализации аудио потока"""
        return params
    
    def _strategy_timing(self, params: Dict) -> Dict:
        """Стратегия уникализации тайминга"""
        return params
    
    def verify_uniqueness(self, file1: Path, file2: Path) -> bool:
        """
        Проверяет что два файла технически уникальны
        """
        hash1 = self._calculate_file_hash(file1)
        hash2 = self._calculate_file_hash(file2)
        return hash1 != hash2
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Вычисляет хеш файла"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
