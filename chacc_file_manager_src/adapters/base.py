from abc import ABC, abstractmethod
from typing import Optional, Union
from pathlib import Path
from fastapi import Request


class BaseAdapter(ABC):
    name: str = "base"

    @abstractmethod
    async def save(
        self,
        file_uuid: str,
        content: Union[bytes, Path],
        content_type: str,
    ) -> dict:
        """Save content to storage. Returns metadata dict."""
        pass

    @abstractmethod
    async def delete(self, storage_key: str) -> bool:
        """Delete file from storage. Returns True if successful."""
        pass

    @abstractmethod
    async def exists(self, storage_key: str) -> bool:
        """Check if file exists."""
        pass

    @abstractmethod
    async def get_size(self, storage_key: str) -> int:
        """Get file size in bytes."""
        pass

    @abstractmethod
    async def get_url(self, storage_key: str, request: Request) -> str:
        """Generate URL for file access."""
        pass

    async def health_check(self) -> bool:
        """Check adapter health."""
        return True

    @abstractmethod
    async def read_stream(self, storage_key: str, start: int = 0, end: Optional[int] = None):
        """Return an async iterator over file bytes, optionally bounded by start/end."""
        pass


class AdapterRegistry:
    _adapters: dict = {}
    _default: Optional[str] = None

    @classmethod
    def register(cls, adapter: BaseAdapter, name: Optional[str] = None, set_default: bool = False):
        """Register an adapter with the registry."""
        adapter_name = name or adapter.name
        cls._adapters[adapter_name] = adapter
        if set_default or cls._default is None:
            cls._default = adapter_name

    @classmethod
    def get(cls, name: Optional[str] = None) -> BaseAdapter:
        """Get adapter by name or default."""
        adapter_name = name or cls._default
        if adapter_name not in cls._adapters:
            raise ValueError(f"Adapter '{adapter_name}' not registered")
        return cls._adapters[adapter_name]

    @classmethod
    def get_default(cls) -> Optional[BaseAdapter]:
        """Get the default adapter."""
        if cls._default is None:
            return None
        return cls._adapters.get(cls._default)
