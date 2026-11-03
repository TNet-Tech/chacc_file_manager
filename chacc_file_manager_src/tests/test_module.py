import pytest
import os
import sys
import tempfile
from pathlib import Path

from ..models import FileRecord, ModuleAdapterMapping
from ..exceptions import (
    FileTooLargeError,
    InvalidContentTypeError,
    FileNotFoundError,
)


def test_models_import():
    assert FileRecord is not None
    assert ModuleAdapterMapping is not None


def test_exceptions_exist():
    assert FileTooLargeError is not None
    assert InvalidContentTypeError is not None
    assert FileNotFoundError is not None


def test_file_record_creation():
    record = FileRecord(
        uuid="test-uuid-123",
        adapter_name="local",
        module_dir="menu",
        channel="images",
        filename="test.txt",
        content_type="text/plain",
        size=100,
        storage_key="menu/images/test-uuid-123",
        created_by_module="menu",
        checksum="abc123",
    )
    assert record.uuid == "test-uuid-123"
    assert record.adapter_name == "local"
    assert record.module_dir == "menu"
    assert record.channel == "images"
    assert record.storage_key == "menu/images/test-uuid-123"


def test_module_adapter_mapping_creation():
    mapping = ModuleAdapterMapping(
        module_name="menu",
        adapter_name="local",
        use_module_dir=True,
        description="Menu module",
    )
    assert mapping.module_name == "menu"
    assert mapping.adapter_name == "local"
    assert mapping.use_module_dir is True


def test_sanitize_filename():
    from ..service import sanitize_filename
    assert sanitize_filename("test file.txt") == "testfile.txt"
    assert sanitize_filename("../../../etc/passwd") == "......etcpasswd"
    assert sanitize_filename("file<>.pdf") == "file.pdf"
    assert sanitize_filename("safe_file.txt") == "safe_file.txt"


def test_checksum_generation():
    from ..service import generate_checksum
    checksum = generate_checksum(b"test content")
    assert len(checksum) == 64


def test_local_adapter():
    from ..adapters.local import LocalAdapter
    with tempfile.TemporaryDirectory() as tmpdir:
        adapter = LocalAdapter(storage_dir=tmpdir)
        assert adapter.name == "local"


@pytest.mark.asyncio
async def test_adapter_save_and_delete():
    from ..adapters.local import LocalAdapter
    with tempfile.TemporaryDirectory() as tmpdir:
        adapter = LocalAdapter(storage_dir=tmpdir)
        storage_key = "test-uuid-123"
        await adapter.save(storage_key, b"test content", "text/plain")
        assert await adapter.exists(storage_key)
        assert await adapter.get_size(storage_key) == 12
        assert await adapter.delete(storage_key)
        assert not await adapter.exists(storage_key)


@pytest.mark.asyncio
async def test_adapter_save_with_module_dir():
    from ..adapters.local import LocalAdapter
    with tempfile.TemporaryDirectory() as tmpdir:
        adapter = LocalAdapter(storage_dir=tmpdir)
        storage_key = "mymodule/test-uuid-123"
        await adapter.save(storage_key, b"test content", "text/plain")
        assert await adapter.exists(storage_key)
        assert await adapter.get_size(storage_key) == 12


def test_router_exists():
    from ..routes import router
    assert router is not None
    assert hasattr(router, "routes")


def test_router_has_no_baked_in_prefix():
    from ..routes import router
    assert router.prefix == ""


@pytest.mark.asyncio
async def test_save_file_reject_policy_raises_duplicate_error():
    from ..service import FileService
    from ..exceptions import DuplicateFileError
    from ..adapters.local import LocalAdapter
    from ..adapters.base import AdapterRegistry

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter = LocalAdapter(storage_dir=tmpdir)
        AdapterRegistry._adapters["local"] = adapter
        AdapterRegistry._default = "local"

        service = FileService()

        existing = FileRecord(
            uuid="existing-uuid",
            adapter_name="local",
            module_dir="menu",
            channel="images",
            filename="x.png",
            content_type="image/png",
            size=4,
            storage_key="menu/images/existing-uuid",
            created_by_module="menu",
            checksum="known",
        )

        async def fake_check(db, checksum, module, include_module=True):
            return existing

        service._check_duplicate = fake_check

        fake_db = object()

        with pytest.raises(DuplicateFileError) as exc:
            await service.save_file(
                file=b"data",
                filename="dup.png",
                content_type="image/png",
                created_by_module="menu",
                channel="images",
                db_session=fake_db,
                duplicate_policy="reject",
            )
        assert exc.value.existing_record is existing


@pytest.mark.asyncio
async def test_save_file_share_policy_reuses_storage_key():
    from ..service import FileService
    from ..adapters.local import LocalAdapter
    from ..adapters.base import AdapterRegistry

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter = LocalAdapter(storage_dir=tmpdir)
        AdapterRegistry._adapters["local"] = adapter
        AdapterRegistry._default = "local"

        service = FileService()

        existing = FileRecord(
            uuid="existing-uuid",
            adapter_name="local",
            module_dir="menu",
            channel="images",
            filename="x.png",
            content_type="image/png",
            size=4,
            storage_key="menu/images/existing-uuid",
            created_by_module="menu",
            checksum="known",
        )

        async def fake_check(db, checksum, module, include_module=True):
            return existing

        service._check_duplicate = fake_check

        added_records = []

        class FakeSession:
            async def add(self, record):
                added_records.append(record)

            async def flush(self):
                pass

            async def refresh(self, record):
                pass

        record = await service.save_file(
            file=b"data",
            filename="dup.png",
            content_type="image/png",
            created_by_module="menu",
            channel="images",
            db_session=FakeSession(),
            duplicate_policy="share",
        )

        assert record.uuid != existing.uuid
        assert record.storage_key == existing.storage_key
        assert record.checksum == existing.checksum
        assert len(added_records) == 1
        assert added_records[0].storage_key == existing.storage_key
        assert not await adapter.exists(record.uuid)
        assert await adapter.exists(existing.storage_key)


@pytest.mark.asyncio
async def test_save_file_allow_policy_stores_separate_bytes():
    from ..service import FileService
    from ..adapters.local import LocalAdapter
    from ..adapters.base import AdapterRegistry

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter = LocalAdapter(storage_dir=tmpdir)
        AdapterRegistry._adapters["local"] = adapter
        AdapterRegistry._default = "local"

        service = FileService()

        existing = FileRecord(
            uuid="existing-uuid",
            adapter_name="local",
            module_dir=None,
            channel=None,
            filename="x.png",
            content_type="image/png",
            size=4,
            storage_key="existing-uuid",
            created_by_module="menu",
            checksum="known",
        )

        async def fake_check(db, checksum, module, include_module=True):
            return existing

        service._check_duplicate = fake_check

        added_records = []

        class FakeSession:
            async def add(self, record):
                added_records.append(record)

            async def flush(self):
                pass

            async def refresh(self, record):
                pass

        record = await service.save_file(
            file=b"data",
            filename="dup.png",
            content_type="image/png",
            created_by_module="menu",
            db_session=FakeSession(),
            duplicate_policy="allow",
        )

        assert record.storage_key != existing.storage_key
        assert len(added_records) == 1
        assert await adapter.exists(record.storage_key)
        assert await adapter.exists(existing.storage_key)


@pytest.mark.asyncio
async def test_local_adapter_get_url_resolves():
    from fastapi import FastAPI, Request
    from starlette.testclient import TestClient
    from ..routes import router
    from ..adapters.local import LocalAdapter

    app = FastAPI()
    app.include_router(router, prefix="/files")

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter = LocalAdapter(storage_dir=tmpdir)

        @app.get("/test-get-url")
        async def _probe(request: Request):
            return {"url": await adapter.get_url("some-uuid", request)}

        with TestClient(app) as client:
            resp = client.get("/test-get-url")
            assert resp.status_code == 200
            assert resp.json()["url"].endswith("/files/some-uuid/content")


def run_module_tests():
    import subprocess
    tests_dir = Path(__file__).resolve().parent
    plugin_root = tests_dir.parent.parent
    venv_python = plugin_root.parent / "venv" / "bin" / "python"
    python = str(venv_python if venv_python.exists() else sys.executable)
    result = subprocess.run(
        [python, "-m", "pytest", str(tests_dir), "-v", "--tb=short"],
        cwd=str(tests_dir),
        env={**os.environ, "PYTHONPATH": str(plugin_root)},
    )
    if result.returncode == 0:
        return {"status": "passed", "message": "All tests passed"}
    return {"status": "failed", "message": "Tests failed", "details": result.stdout + result.stderr}