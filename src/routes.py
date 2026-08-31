from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List
import os

from .context_factory import get_async_db, get_module_context
from .models import FileRecord, ModuleAdapterMapping
from .service import FileService
from .adapters.base import AdapterRegistry
from .exceptions import FileTooLargeError, InvalidContentTypeError, DuplicateFileError


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
