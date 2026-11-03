from .context_factory import get_module_context


async def get_file_service():
    return get_module_context().get_service("file_service") if get_module_context() else None