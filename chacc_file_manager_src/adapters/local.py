import aiofiles
import mimetypes
import os
import shutil
from pathlib import Path
from typing import AsyncIterable, Optional, Union
from fastapi import Request, HTTPException

from .base import BaseAdapter


class LocalAdapter(BaseAdapter):
    name = "local"

    def __init__(self, storage_dir: str):
        self.storage_dir = Path(storage_dir).resolve()
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    def _get_storage_path(self, storage_key: str) -> Path:
        full_path = (self.storage_dir / storage_key).resolve()
        try:
            full_path.relative_to(self.storage_dir)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid file path")
        return full_path

    async def save(
        self,
        file_uuid: str,
        content: Union[bytes, Path],
        content_type: str,
    ) -> dict:
        full_path = self._get_storage_path(file_uuid)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = full_path.parent / f".tmp_{full_path.name}"

        try:
            if isinstance(content, bytes):
                async with aiofiles.open(temp_path, "wb") as f:
                    await f.write(content)
            else:
                # Use shutil.move instead of Path.rename to support cross-device moves
                # (e.g. /tmp -> /home). Falls back to copy+delete if rename fails.
                shutil.move(str(content), str(temp_path))
            os.rename(str(temp_path), str(full_path))
            return {"storage_key": file_uuid, "adapter": self.name}
        except Exception:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass
            raise
        
    async def delete(self, storage_key: str) -> bool:
        full_path = self._get_storage_path(storage_key)
        if os.path.exists(full_path):
            os.remove(full_path)
            return True
        return False

    async def exists(self, storage_key: str) -> bool:
        full_path = self._get_storage_path(storage_key)
        return os.path.exists(full_path)

    async def get_size(self, storage_key: str) -> int:
        full_path = self._get_storage_path(storage_key)
        if not os.path.exists(full_path):
            raise HTTPException(status_code=404, detail="File not found")
        return os.path.getsize(full_path)

    async def get_url(self, storage_key: str, request: Request) -> str:
        url = request.url_for("serve_file", uuid=storage_key)
        return str(url)

    def _detect_mime_type(self, filename: str) -> str:
        mime_type, _ = mimetypes.guess_type(filename)
        return mime_type or "application/octet-stream"

    async def read_stream(self, storage_key: str, start: int = 0, end: Optional[int] = None):
        full_path = self._get_storage_path(storage_key)
        async with aiofiles.open(full_path, "rb") as f:
            if start > 0:
                await f.seek(start)
            remaining = (end - start + 1) if end is not None else None
            while True:
                if remaining is not None and remaining <= 0:
                    break
                chunk_size = min(8192, remaining) if remaining is not None else 8192
                data = await f.read(chunk_size)
                if not data:
                    break
                yield data
                if remaining is not None:
                    remaining -= len(data)