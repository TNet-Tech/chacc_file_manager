import aiofiles
import uuid_utils
import hashlib
import asyncio
import tempfile
import os
import json
from pathlib import Path
from typing import Optional, Union, AsyncIterable, List, Callable, Awaitable, Literal
from abc import ABC, abstractmethod
from fastapi import Request, UploadFile, HTTPException
from starlette.datastructures import UploadFile as StarletteUploadFile
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from .context_factory import get_module_context
from .models import FileRecord, ModuleAdapterMapping
from .adapters.base import AdapterRegistry, BaseAdapter
from .exceptions import (
    FileNotFoundError as FileNotFound,
    InvalidContentTypeError,
    FileTooLargeError,
    DuplicateFileError,
)


DuplicatePolicy = Literal["reject", "share", "allow"]


MODULE_META_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "module_meta.json")


def get_base_url_prefix() -> str:
    """Read base_path_prefix from module_meta.json. Sync, usable anywhere."""
    try:
        with open(MODULE_META_PATH, "r") as f:
            meta = json.load(f)
        return meta.get("base_path_prefix", "/files")
    except (FileNotFoundError, json.JSONDecodeError):
        return "/files"


def get_content_url(file_uuid: str) -> str:
    """Construct content URL from UUID. Sync, no DB needed."""
    base_prefix = get_base_url_prefix()
    return f"{base_prefix}/{file_uuid}/content"


def generate_checksum(content: bytes, module: str = "", include_module: bool = True) -> str:
    if include_module and module:
        return hashlib.sha256(module.encode() + content).hexdigest()
    return hashlib.sha256(content).hexdigest()


def sanitize_filename(filename: str) -> str:
    safe = "".join(c for c in filename if c.isalnum() or c in "._-")
    return safe or f"file_{uuid_utils.uuid7().hex}"


class BaseFileService(ABC):
    @abstractmethod
    async def save_file(
        self,
        file: Union[UploadFile, bytes, AsyncIterable[bytes]],
        filename: str,
        content_type: str,
        created_by_module: str,
        channel: Optional[str] = None,
        db_session: AsyncSession = None,
        adapter_name: Optional[str] = None,
        created_by_user_id: Optional[int] = None,
        validation_hooks: Optional[List[Callable[[Union[bytes, Path], dict, bool], Awaitable[None]]]] = None,
        duplicate_policy: DuplicatePolicy = "reject",
        include_checksum_module: bool = True,
    ) -> FileRecord:
        pass

    @abstractmethod
    async def get_file(self, file_uuid: str, db_session: AsyncSession) -> FileRecord:
        pass

    @abstractmethod
    async def delete_file(self, file_uuid: str, db_session: AsyncSession) -> bool:
        pass

    @abstractmethod
    async def get_download_url(
        self,
        file_uuid: str,
        request: Request,
        db_session: AsyncSession,
        download: bool = False,
    ) -> str:
        pass

    @abstractmethod
    async def register_adapter(self, adapter: BaseAdapter, name: str, set_default: bool = False):
        pass


class FileService(BaseFileService):
    def __init__(self):
        pass

    def get_base_url_prefix(self) -> str:
        """Return the module's base path prefix for URL construction."""
        return get_base_url_prefix()

    def get_content_url(self, file_uuid: str) -> str:
        """Construct content URL from UUID. Sync, no DB needed."""
        base_prefix = get_base_url_prefix()
        return f"{base_prefix}/{file_uuid}/content"

    async def _resolve_adapter(self, module_name: str, adapter_name: Optional[str], db_session: AsyncSession):
        if adapter_name and adapter_name in AdapterRegistry._adapters:
            return AdapterRegistry.get(adapter_name), None

        if db_session:
            result = await db_session.execute(
                select(ModuleAdapterMapping).where(ModuleAdapterMapping.module_name == module_name)
            )
            mapping = result.scalar_one_or_none()
        else:
            mapping = None

        if mapping and mapping.adapter_name in AdapterRegistry._adapters:
            return AdapterRegistry.get(mapping.adapter_name), mapping

        adapter = AdapterRegistry.get_default()
        if not adapter:
            raise ValueError(f"No adapter registered for module '{module_name}'")
        return adapter, None

    async def _process_bytes(
        self,
        content: bytes,
        max_file_size: int,
        module: str = "",
        include_checksum_module: bool = True,
    ) -> tuple[str, int, bytes]:
        if len(content) > max_file_size:
            raise FileTooLargeError(f"File exceeds {max_file_size} bytes limit")

        checksum = generate_checksum(content, module, include_checksum_module)
        return checksum, len(content), content

    async def _process_stream(
        self,
        stream: AsyncIterable[bytes],
        max_file_size: int,
        module: str = "",
        include_checksum_module: bool = True,
    ) -> tuple[str, int, Path]:
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".upload") as tmp:
                temp_path = Path(tmp.name)

            sha = hashlib.sha256()
            if include_checksum_module and module:
                sha.update(module.encode())
            file_size = 0
            async with aiofiles.open(temp_path, "wb") as af:
                async for chunk in stream:
                    if not isinstance(chunk, bytes):
                        raise TypeError("Stream must yield bytes")
                    file_size += len(chunk)
                    if file_size > max_file_size:
                        raise FileTooLargeError(f"File exceeds {max_file_size} bytes limit")
                    sha.update(chunk)
                    await af.write(chunk)

            checksum = sha.hexdigest()
            return checksum, file_size, temp_path
        except Exception:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except Exception:
                    pass
            raise

    async def _check_duplicate(
        self, db_session: AsyncSession, checksum: str, created_by_module: str, include_module: bool = True
    ) -> Optional[FileRecord]:
        stmt = select(FileRecord).where(FileRecord.checksum == checksum)
        if not include_module:
            stmt = stmt.where(FileRecord.created_by_module == created_by_module)
        result = await db_session.execute(stmt)
        return result.scalars().first()

    def _get_config(self) -> dict:
        context = get_module_context()
        if context is None:
            return {
                "max_file_size": 10 * 1024 * 1024,
                "stream_threshold": 10 * 1024 * 1024,
                "chunk_size": 8192,
                "allowed_content_types": set(),
            }

        max_file_size = context.get_module_config(
            "MAX_FILE_SIZE", "chacc_file_manager", default=10 * 1024 * 1024
        )
        stream_threshold = context.get_module_config(
            "STREAM_THRESHOLD", "chacc_file_manager", default=10 * 1024 * 1024
        )
        chunk_size = context.get_module_config(
            "UPLOAD_CHUNK_SIZE", "chacc_file_manager", default=8192
        )
        allowed_types_str = context.get_module_config(
            "ALLOWED_CONTENT_TYPES", "chacc_file_manager", default=""
        )
        allowed_types = (
            {t.strip() for t in allowed_types_str.split(",") if t.strip()}
            if allowed_types_str
            else set()
        )
        return {
            "max_file_size": max_file_size,
            "stream_threshold": stream_threshold,
            "chunk_size": chunk_size,
            "allowed_content_types": allowed_types,
        }

    async def save_file(
        self,
        file: Union[UploadFile, bytes, AsyncIterable[bytes]],
        filename: str,
        content_type: str,
        created_by_module: str,
        channel: Optional[str] = None,
        db_session: AsyncSession = None,
        adapter_name: Optional[str] = None,
        created_by_user_id: Optional[int] = None,
        validation_hooks: Optional[List[Callable[[Union[bytes, Path], dict, bool], Awaitable[None]]]] = None,
        duplicate_policy: DuplicatePolicy = "reject",
        include_checksum_module: bool = True,
    ) -> FileRecord:
        if db_session is None:
            raise ValueError("Database session is required")

        config = self._get_config()
        max_file_size = config["max_file_size"]
        stream_threshold = config["stream_threshold"]
        chunk_size = config["chunk_size"]
        allowed_content_types = config["allowed_content_types"]

        if allowed_content_types and content_type not in allowed_content_types:
            raise InvalidContentTypeError(
                f"Content type '{content_type}' is not allowed"
            )

        adapter, mapping = await self._resolve_adapter(
            created_by_module, adapter_name, db_session
        )
        file_uuid = str(uuid_utils.uuid7())
        safe_filename = sanitize_filename(filename)

        use_module_dir = bool(mapping and mapping.use_module_dir)
        storage_key = file_uuid
        if use_module_dir:
            storage_key = f"{created_by_module}/{storage_key}"
            if channel:
                storage_key = f"{created_by_module}/{channel}/{storage_key}"

        checksum = None
        file_size = None
        validation_payload = None
        validation_is_path = False
        temp_path = None

        if isinstance(file, StarletteUploadFile):
            if file.size is not None and file.size <= stream_threshold:
                file_size = file.size
                content_bytes = await file.read()
                checksum, file_size, content_bytes = await self._process_bytes(
                    content_bytes, max_file_size, created_by_module, include_checksum_module
                )
                validation_payload = content_bytes
                validation_is_path = False
            else:
                async def _stream_reader():
                    remaining = None if file.size is None else file.size
                    while True:
                        chunk = await file.read(chunk_size)
                        if not chunk:
                            break
                        if remaining is not None:
                            remaining -= len(chunk)
                            if remaining < 0:
                                raise FileTooLargeError(f"File exceeds {max_file_size} bytes limit")
                        yield chunk

                checksum, file_size, temp_path = await self._process_stream(
                    _stream_reader(), max_file_size, created_by_module, include_checksum_module
                )
                validation_payload = temp_path
                validation_is_path = True
        elif isinstance(file, bytes):
            checksum, file_size, content_bytes = await self._process_bytes(
                file, max_file_size, created_by_module, include_checksum_module
            )
            validation_payload = content_bytes
            validation_is_path = False
        else:
            checksum, file_size, temp_path = await self._process_stream(
                file, max_file_size, created_by_module, include_checksum_module
            )
            validation_payload = temp_path
            validation_is_path = True

        for hook in (validation_hooks or []):
            await hook(validation_payload, {}, validation_is_path)

        existing = await self._check_duplicate(db_session, checksum, created_by_module, include_checksum_module)
        if existing:
            if duplicate_policy == "reject":
                raise DuplicateFileError("File already exists", existing_record=existing)

            if duplicate_policy == "share":
                if temp_path and os.path.exists(temp_path):
                    try:
                        os.unlink(temp_path)
                    except Exception:
                        pass
                temp_path = None

                shared_record = FileRecord(
                    uuid=str(uuid_utils.uuid7()),
                    adapter_name=existing.adapter_name,
                    module_dir=existing.module_dir,
                    channel=channel,
                    filename=safe_filename,
                    content_type=content_type,
                    size=existing.size,
                    storage_key=existing.storage_key,
                    created_by_module=created_by_module,
                    checksum=existing.checksum,
                )
                if hasattr(shared_record, "created_by_id"):
                    shared_record.created_by_id = created_by_user_id
                db_session.add(shared_record)
                await db_session.flush()
                await db_session.refresh(shared_record)
                return shared_record

        if temp_path and os.path.exists(temp_path):
            metadata = await adapter.save(storage_key, temp_path, content_type)
        else:
            metadata = await adapter.save(storage_key, validation_payload, content_type)

        if temp_path and os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except Exception:
                pass

        record = FileRecord(
            uuid=file_uuid,
            adapter_name=adapter.name,
            module_dir=created_by_module if use_module_dir else None,
            channel=channel if use_module_dir else None,
            filename=safe_filename,
            content_type=content_type,
            size=file_size,
            storage_key=storage_key,
            created_by_module=created_by_module,
            checksum=checksum,
        )
        if hasattr(record, "created_by_id"):
            record.created_by_id = created_by_user_id

        db_session.add(record)
        await db_session.flush()
        await db_session.refresh(record)
        return record

    async def get_file(self, file_uuid: str, db_session: AsyncSession) -> FileRecord:
        result = await db_session.execute(
            select(FileRecord).where(FileRecord.uuid == file_uuid)
        )
        record = result.scalar_one_or_none()
        if record is None:
            raise FileNotFound(f"File {file_uuid} not found")
        return record

    async def delete_file(self, file_uuid: str, db_session: AsyncSession) -> bool:
        result = await db_session.execute(
            select(FileRecord).where(FileRecord.uuid == file_uuid)
        )
        record = result.scalar_one_or_none()
        if record is None:
            return False

        result = await db_session.execute(
            select(func.count()).where(
                FileRecord.storage_key == record.storage_key,
                FileRecord.uuid != file_uuid,
            )
        )
        ref_count = result.scalar() or 0

        if ref_count == 0:
            try:
                await AdapterRegistry.get(record.adapter_name).delete(str(record.storage_key))
            except Exception:
                pass

        result = await db_session.execute(
            select(FileRecord).where(FileRecord.uuid == file_uuid)
        )
        target = result.scalar_one_or_none()
        if target:
            db_session.delete(target)
            await db_session.flush()
        return True

    async def get_download_url(
        self,
        file_uuid: str,
        request: Request,
        db_session: AsyncSession,
        download: bool = False,
    ) -> str:
        record = await self.get_file(file_uuid, db_session)
        adapter = AdapterRegistry.get(record.adapter_name)
        return await adapter.get_url(record.storage_key, request)

    async def register_adapter(self, adapter: BaseAdapter, name: str, set_default: bool = False):
        AdapterRegistry.register(adapter, name=name, set_default=set_default)
