from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from chacc_api import BackboneContext
from typing import Optional
import os

from .routes import router as chacc_file_manager_router
from .context_factory import get_context, set_module_context, get_module_context
from .adapters.local import LocalAdapter
from .adapters.base import AdapterRegistry, BaseAdapter
from .service import FileService


health_router = APIRouter()


@health_router.get("/health")
async def health_check():
    """Health check endpoint for the module."""
    context = get_module_context()
    if context is not None:
        storage_path = context.get_module_config("STORAGE_DIR", "chacc_file_manager", default="/tmp/chacc_file_storage")
    else:
        storage_path = "/tmp/chacc_file_storage"

    if not os.path.exists(storage_path):
        return JSONResponse(
            {"status": "warning", "message": "Storage directory not created"},
            status_code=200,
        )

    if not os.access(storage_path, os.W_OK):
        return JSONResponse(
            {"status": "error", "message": "Storage directory not writable"},
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    return {"status": "healthy", "adapter": "local"}


def setup_plugin(context: Optional[BackboneContext] = None):
    """
    This function is called by the ChaCC API backbone to initialize your module.
    It can also be called in development mode without a context.
    """
    _module_context = get_context(context)
    set_module_context(_module_context)

    _module_context.logger.info("chacc_file_manager: Setup initiated!")

    storage_dir = _module_context.get_module_config(
        "STORAGE_DIR", "chacc_file_manager", default="uploads/"
    )
    try:
        local_adapter = LocalAdapter(storage_dir=storage_dir)
        AdapterRegistry.register(local_adapter, name="local", set_default=True)
        _module_context.logger.info("chacc_file_manager: Local adapter registered")
    except Exception as e:
        _module_context.logger.warning(f"chacc_file_manager: Failed to init storage: {e}")

    _module_context.register_service("file_service", FileService())
    _module_context.register_service("register_file_adapter", register_file_adapter)
    _module_context.register_service("file_base_adapter", BaseAdapter)

    try:
        from .stream_service import startup_cleanup_orphaned_streams
        startup_cleanup_orphaned_streams()
        _module_context.logger.info("chacc_file_manager: Cleaned up orphaned stream directories")
    except Exception as e:
        _module_context.logger.warning(f"chacc_file_manager: Failed to cleanup orphaned streams: {e}")

    chacc_file_manager_router.include_router(health_router)
    return chacc_file_manager_router


def register_file_adapter(adapter: BaseAdapter, name=None, description=None):
    adapter_name = name or getattr(adapter, "name", None)
    if not adapter_name:
        raise ValueError("Adapter must have a name")
    _validate_adapter(adapter)
    AdapterRegistry.register(adapter, name=adapter_name, set_default=False)
    return adapter_name


def _validate_adapter(adapter):
    required_methods = [
        "save", "delete", "exists", "get_size", "get_url", "read_stream", "health_check"
    ]
    missing = [m for m in required_methods if not hasattr(adapter, m)]
    if missing:
        raise TypeError(f"Adapter missing required methods: {', '.join(missing)}")

    for method_name in required_methods:
        method = getattr(adapter, method_name)
        if not callable(method):
            raise TypeError(f"Adapter.{method_name} is not callable")


def get_plugin_info():
    """
    Provides essential information about this module to the ChaCC API backbone.
    """
    return {
        "name": "chacc_file_manager",
        "display_name": "ChaccFileManager Module",
        "version": "0.1.0",
        "author": "Your Name/Organization",
        "description": "A new ChaCC API module for chacc_file_manager functionality.",
        "status": "enabled"
    }
