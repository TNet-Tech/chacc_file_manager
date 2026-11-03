from chacc_api import ChaCCBaseModel, register_model
from sqlalchemy import Column, String, Integer, Boolean, Enum as SQLAEnum
from typing import Optional
from enum import Enum


class FileStatus(str, Enum):
    ACTIVE = "ACTIVE"
    DELETED = "DELETED"
    QUARANTINED = "QUARANTINED"


@register_model
class FileRecord(ChaCCBaseModel):
    __tablename__ = "file_records"

    adapter_name = Column(String(100), nullable=False)
    module_dir = Column(String(100), nullable=True)
    channel = Column(String(100), nullable=True)
    filename = Column(String(255), nullable=False)
    content_type = Column(String(100), nullable=False)
    size = Column(Integer, nullable=False)
    storage_key = Column(String, nullable=False)
    created_by_module = Column(String(100), nullable=False)
    checksum = Column(String(64), nullable=True)
    status = Column(SQLAEnum(FileStatus), nullable=False, default=FileStatus.ACTIVE)


@register_model
class ModuleAdapterMapping(ChaCCBaseModel):
    __tablename__ = "file_module_adapter_mappings"

    module_name = Column(String(100), unique=True, nullable=False)
    adapter_name = Column(String(100), nullable=False)
    use_module_dir = Column(Boolean, default=False, nullable=False)
    description = Column(String(500), nullable=True)