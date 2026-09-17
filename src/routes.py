from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .context_factory import get_async_db, get_module_context
from .models import FileRecord, ModuleAdapterMapping
from .service import FileService
from .adapters.base import AdapterRegistry
from .exceptions import (
    DuplicateFileError,
    FileTooLargeError,
    InvalidContentTypeError,
)
from .stream_service import StreamService, cleanup_stream_dir, sanitize_filename


class ModuleMappingCreate(BaseModel):
    module_name: str
    adapter_name: str
    use_module_dir: bool = False
    description: Optional[str] = None


router = APIRouter()


@router.get("/adapters")
async def list_adapters():
    """List all registered adapters."""
    return {"adapters": list(AdapterRegistry._adapters.keys())}


@router.get("/adapters/{name}")
async def get_adapter(name: str):
    """Get adapter info by name."""
    if name not in AdapterRegistry._adapters:
        raise HTTPException(status_code=404, detail="Adapter not found")
    return {"name": name, "status": "registered"}


@router.get("/module-mappings", response_model=List[dict])
async def list_module_mappings(db=Depends(get_async_db)):
    """List all module-to-adapter mappings."""
    from sqlalchemy import select
    result = await db.execute(select(ModuleAdapterMapping))
    mappings = result.scalars().all()
    return [{"module_name": m.module_name, "adapter_name": m.adapter_name, "use_module_dir": m.use_module_dir} for m in mappings]


@router.post("/module-mappings", status_code=status.HTTP_201_CREATED)
async def create_module_mapping(mapping: ModuleMappingCreate, db=Depends(get_async_db)):
    """Create module-to-adapter mapping."""
    if mapping.adapter_name not in AdapterRegistry._adapters:
        raise HTTPException(status_code=400, detail="Adapter not registered")
    db_mapping = ModuleAdapterMapping(
        module_name=mapping.module_name,
        adapter_name=mapping.adapter_name,
        use_module_dir=mapping.use_module_dir,
        description=mapping.description,
    )
    db.add(db_mapping)
    await db.commit()
    return {"module_name": db_mapping.module_name, "adapter_name": db_mapping.adapter_name, "use_module_dir": db_mapping.use_module_dir}


@router.delete("/module-mappings/{module_name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_module_mapping(module_name: str, db=Depends(get_async_db)):
    """Delete module-to-adapter mapping."""
    from sqlalchemy import select
    result = await db.execute(select(ModuleAdapterMapping).where(ModuleAdapterMapping.module_name == module_name))
    mapping = result.scalar_one_or_none()
    if mapping:
        db.delete(mapping)
        await db.commit()


@router.get("/{uuid}/content")
async def serve_file(
    uuid: str,
    request: Request,
    download: bool = False,
    db=Depends(get_async_db),
):
    service = FileService()
    record = await service.get_file(uuid, db)

    adapter = AdapterRegistry.get(record.adapter_name)
    storage_key = str(record.storage_key)

    try:
        size = await adapter.get_size(storage_key)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=404, detail="File not found")

    context = get_module_context()
    cache_max_age = (
        context.get_module_config("FILE_CACHE_MAX_AGE", "chacc_file_manager", default=300)
        if context
        else 3600
    )

    headers = {
        "Content-Type": record.content_type,
        "Content-Disposition": f'inline; filename="{record.filename}"' if not download else f'attachment; filename="{record.filename}"',
        "Cache-Control": f"public, max-age={cache_max_age}",
        "ETag": f'"{record.checksum}"' if record.checksum else None,
    }

    range_header = request.headers.get("range")
    if range_header:
        start, end = 0, size - 1
        if range_header.startswith("bytes="):
            parts = range_header[6:].split("-")
            if len(parts) == 2:
                start = int(parts[0]) if parts[0] else 0
                end = int(parts[1]) if parts[1] else end

        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
        headers["Accept-Ranges"] = "bytes"

        return StreamingResponse(
            adapter.read_stream(storage_key, start=start, end=end),
            status_code=206,
            headers=headers,
            media_type=record.content_type,
        )

    return StreamingResponse(
        adapter.read_stream(storage_key),
        headers=headers,
        media_type=record.content_type,
    )


@router.post("/", status_code=status.HTTP_201_CREATED)
async def upload_file(
    request: Request,
    db=Depends(get_async_db),
):
    service = FileService()
    form = await request.form()
    file = form.get("file")
    if not file:
        raise HTTPException(status_code=400, detail="No file provided")

    content_type = file.content_type or "application/octet-stream"

    try:
        record = await service.save_file(
            file=file,
            filename=file.filename,
            content_type=content_type,
            created_by_module="chacc_file_manager",
            channel=form.get("channel"),
            db_session=db,
        )
        await db.commit()
        return {"uuid": record.uuid, "filename": record.filename, "size": record.size, "storage_key": record.storage_key}
    except (FileTooLargeError, InvalidContentTypeError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except DuplicateFileError as e:
        existing = e.existing_record
        raise HTTPException(
            status_code=409,
            detail="File already exists",
            headers={"X-Existing-File-Uuid": existing.uuid} if existing else None,
        )


@router.delete("/{uuid}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_file(
    uuid: str,
    db=Depends(get_async_db),
):
    service = FileService()
    deleted = await service.delete_file(uuid, db)
    if not deleted:
        raise HTTPException(status_code=404, detail="File not found")
    await db.commit()


# ---------------------------------------------------------------------------
# Resumable streaming upload endpoints (delegated to StreamService)
# ---------------------------------------------------------------------------

class StreamFinalizeResponse(BaseModel):
    status: str
    file_uuid: Optional[str] = None
    filename: Optional[str] = None
    size: Optional[int] = None
    storage_key: Optional[str] = None
    message: Optional[str] = None
    missing_chunks: Optional[list] = None


@router.post("/stream-upload", response_model=StreamFinalizeResponse)
async def stream_upload(
    request: Request,
    background_tasks: BackgroundTasks,
    x_stream_id: str = Header(..., description="Unique identifier for the upload session"),
    x_file_name: str = Header(..., description="Desired final filename"),
    x_chunk_number: int = Header(..., ge=1, description="Current chunk number (1-indexed)"),
    x_final_chunk: bool = Header(..., description="True if this is the last chunk"),
    x_total_chunks: Optional[int] = Header(default=None, description="Optional: Total expected chunks"),
    x_content_type: str = Header(default="application/octet-stream", description="MIME type of the file"),
    x_created_by_module: str = Header(default="chacc_file_manager", description="Module that owns the file"),
    x_channel: Optional[str] = Header(default=None, description="Optional channel for module-scoped storage"),
    x_duplicate_policy: str = Header(default="reject", description="reject | share | allow"),
    db=Depends(get_async_db),
):
    """Resumable streaming upload endpoint.

    Each request uploads a single chunk. When ``X-Final-Chunk: true`` is set,
    all chunks are verified, concatenated, and passed to ``FileService.save_file``
    for checksum computation, duplicate detection, and final storage.
    """
    stream_service = StreamService()

    try:
        await stream_service.write_chunk(
            stream_id=x_stream_id,
            chunk_number=x_chunk_number,
            content_iterable=request.stream(),
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to write chunk: {str(e)}")

    if not x_final_chunk:
        return StreamFinalizeResponse(status="receiving", message=f"Chunk {x_chunk_number} received")

    try:
        record = await stream_service.finalize_stream(
            stream_id=x_stream_id,
            file_name=x_file_name,
            content_type=x_content_type,
            created_by_module=x_created_by_module,
            channel=x_channel,
            duplicate_policy=x_duplicate_policy,
            db_session=db,
            total_chunks=x_total_chunks,
            final_chunk_num=x_chunk_number,
        )
    except DuplicateFileError as e:
        background_tasks.add_task(cleanup_stream_dir, x_stream_id)
        try:
            await db.commit()
        except Exception:
            pass
        existing = e.existing_record
        raise HTTPException(
            status_code=409,
            detail="File already exists",
            headers={"X-Existing-File-Uuid": existing.uuid} if existing else None,
        )
    except HTTPException as e:
        background_tasks.add_task(cleanup_stream_dir, x_stream_id)
        raise

    try:
        await db.commit()
    except Exception:
        pass

    background_tasks.add_task(cleanup_stream_dir, x_stream_id)

    return StreamFinalizeResponse(
        status="completed",
        file_uuid=str(record.uuid),
        filename=record.filename,
        size=record.size,
        storage_key=str(record.storage_key),
        message=f"Assembled {x_total_chunks or x_chunk_number} chunk(s)",
    )


# ---------------------------------------------------------------------------
# Retrieve an incomplete / in-progress stream by stream ID
# ---------------------------------------------------------------------------

@router.get("/stream/{stream_id}")
async def retrieve_stream(
    stream_id: str,
    x_file_name: Optional[str] = Header(default=None, description="Optional filename for Content-Disposition"),
    x_content_type: str = Header(default="application/octet-stream", description="MIME type of the stream"),
):
    """Stream back the contents of an in-progress or incomplete upload.

    Useful for retrieving partially uploaded data (e.g. NDJSON) that may be
    corrupt but still readable. Chunks are concatenated in numeric order based
    on their zero-padded filenames.
    """
    stream_service = StreamService()
    safe_stream_id, chunk_iter = await stream_service.retrieve_stream(stream_id)

    safe_filename = sanitize_filename(x_file_name) if x_file_name else f"{safe_stream_id}.bin"

    headers = {
        "Content-Type": x_content_type,
        "Content-Disposition": f'inline; filename="{safe_filename}"',
        "Accept-Ranges": "bytes",
    }

    return StreamingResponse(
        chunk_iter,
        headers=headers,
        media_type=x_content_type,
    )
