from pydantic import BaseModel, Field
from typing import List, Optional
from enum import Enum


class VideoFormat(str, Enum):
    MP4 = "mp4"
    MOV = "mov"
    AVI = "avi"
    MKV = "mkv"


class ProcessRequest(BaseModel):
    copies_count: int = Field(ge=1, le=100, description="Количество копий от 1 до 100")
    output_format: VideoFormat = VideoFormat.MP4


class ProcessStatus(BaseModel):
    task_id: str
    status: str
    progress: int
    total_copies: int
    message: Optional[str] = None


class ProcessResult(BaseModel):
    task_id: str
    status: str
    files: List[str]
    archive_url: Optional[str] = None
