"""Resumable streaming upload service.

Handles chunked uploads to disk and retrieval of incomplete streams.
"""
import os
import re
import shutil
from pathlib import Path
from typing import AsyncIterable, Optional

import aiofiles
from fastapi import BackgroundTasks, HTTPException


# --- Configuration ---
STREAM_TEMP_DIR = Path(os.getenv("CHACC_STREAM_TEMP_DIR", "/tmp/chacc_streams"))
STREAM_TEMP_DIR.mkdir(parents=True, exist_ok=True)


def get_stream_temp_dir() -> Path:
    """Return the configured temporary directory for streaming chunks."""
    return STREAM_TEMP_DIR


# --- Sanitization ---
def sanitize_stream_id(stream_id: str) -> str:
    """Ensure stream ID is safe for directory names (alphanumeric, dash, underscore)."""
    clean_id = re.sub(r"[^a-zA-Z0-9_-]", "", stream_id)
    if not clean_id:
        raise HTTPException(status_code=400, detail="Invalid X-Stream-ID")
    return clean_id


def sanitize_filename(filename: str) -> str:
    """Extract only the basename to prevent directory traversal."""
    return Path(filename).name or "unnamed_file"


# --- Cleanup ---
async def cleanup_stream_dir(stream_id: str) -> None:
    """Remove a stream's temporary directory."""
    stream_dir = get_stream_temp_dir() / stream_id
    try:
        if stream_dir.exists():
            shutil.rmtree(stream_dir, ignore_errors=True)
    except Exception:
        pass


def startup_cleanup_orphaned_streams() -> None:
    """Remove any orphaned stream directories left over from previous runs."""
    temp_dir = get_stream_temp_dir()
    try:
        for child in temp_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
    except Exception:
        pass


class StreamService:
    """Service for managing resumable streaming uploads."""

    def __init__(self, file_service=None):
        # Lazy import to avoid circular dependency
        if file_service is None:
            from .service import FileService
            file_service = FileService()
        self.file_service = file_service

    def _stream_dir(self, stream_id: str) -> Path:
        return get_stream_temp_dir() / stream_id

    def _chunk_path(self, stream_id: str, chunk_number: int) -> Path:
        return self._stream_dir(stream_id) / f"chunk_{chunk_number:04d}.bin"

    async def write_chunk(
        self,
        stream_id: str,
        chunk_number: int,
        content_iterable: AsyncIterable[bytes],
    ) -> Path:
        """Stream request body to a chunk file. Returns the chunk path."""
        safe_stream_id = sanitize_stream_id(stream_id)
        stream_dir = self._stream_dir(safe_stream_id)
        stream_dir.mkdir(parents=True, exist_ok=True)

        chunk_path = self._chunk_path(safe_stream_id, chunk_number)
        try:
            async with aiofiles.open(chunk_path, "wb") as f:
                async for chunk in content_iterable:
                    await f.write(chunk)
        except Exception:
            if chunk_path.exists():
                try:
                    chunk_path.unlink()
                except Exception:
                    pass
            raise HTTPException(status_code=500, detail=f"Failed to write chunk: {chunk_number}")
        return chunk_path

    async def list_chunks(self, stream_id: str) -> list[int]:
        """Return sorted list of chunk numbers present in the stream."""
        safe_stream_id = sanitize_stream_id(stream_id)
        stream_dir = self._stream_dir(safe_stream_id)
        if not stream_dir.exists():
            return []
        chunks = []
        for p in stream_dir.glob("chunk_*.bin"):
            try:
                num = int(p.stem.split("_")[1])
                chunks.append(num)
            except (IndexError, ValueError):
                continue
        return sorted(chunks)

    async def chunk_iterator(self, stream_id: str) -> AsyncIterable[bytes]:
        """Yield stored chunks in numeric order."""
        safe_stream_id = sanitize_stream_id(stream_id)
        stream_dir = self._stream_dir(safe_stream_id)
        chunk_files = sorted(stream_dir.glob("chunk_*.bin"))
        for chunk_path in chunk_files:
            async with aiofiles.open(chunk_path, "rb") as cf:
                while True:
                    data = await cf.read(1024 * 1024)  # 1 MiB buffer
                    if not data:
                        break
                    yield data

    async def finalize_stream(
        self,
        stream_id: str,
        file_name: str,
        content_type: str,
        created_by_module: str,
        channel: Optional[str],
        duplicate_policy: str,
        db_session,
        total_chunks: Optional[int] = None,
        final_chunk_num: Optional[int] = None,
    ):
        """Verify chunks, assemble via FileService, and cleanup.

        Returns the FileRecord on success.
        Raises HTTPException on validation/storage errors.
        """
        safe_stream_id = sanitize_stream_id(stream_id)
        safe_file_name = sanitize_filename(file_name)

        expected_total = total_chunks if total_chunks is not None else final_chunk_num
        if expected_total is None:
            raise HTTPException(status_code=400, detail="Cannot finalize without chunk count")

        present = await self.list_chunks(safe_stream_id)
        present_set = set(present)
        missing = [i for i in range(1, expected_total + 1) if i not in present_set]
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"Missing chunks: {missing}",
            )

        async def _chunk_iter():
            async for data in self.chunk_iterator(safe_stream_id):
                yield data

        try:
            record = await self.file_service.save_file(
                file=_chunk_iter(),
                filename=safe_file_name,
                content_type=content_type,
                created_by_module=created_by_module,
                channel=channel,
                db_session=db_session,
                duplicate_policy=duplicate_policy,
            )
            return record
        except Exception:
            raise

    async def retrieve_stream(self, stream_id: str):
        """Return (filename, content_type, chunk_iterator) for an in-progress stream.

        Raises HTTPException(404) if stream not found or empty.
        """
        safe_stream_id = sanitize_stream_id(stream_id)
        stream_dir = self._stream_dir(safe_stream_id)
        if not stream_dir.exists() or not stream_dir.is_dir():
            raise HTTPException(status_code=404, detail="Stream not found")

        chunk_files = sorted(stream_dir.glob("chunk_*.bin"))
        if not chunk_files:
            raise HTTPException(status_code=404, detail="Stream contains no chunks")

        return safe_stream_id, self.chunk_iterator(safe_stream_id)