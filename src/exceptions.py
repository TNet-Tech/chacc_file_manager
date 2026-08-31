class FileStorageError(Exception):
    """Base exception for file storage errors."""
    pass


class FileNotFoundError(FileStorageError):
    """File not found in storage."""
    pass


class InvalidPathError(FileStorageError):
    """Invalid or unsafe file path detected."""
    pass


class QuotaExceededError(FileStorageError):
    """Storage quota exceeded."""
    pass


class InvalidContentTypeError(FileStorageError):
    """Invalid content type."""
    pass


class FileTooLargeError(FileStorageError):
    """File size exceeds limit."""
    pass


class DuplicateFileError(FileStorageError):
    """Raised when a duplicate file is detected and the policy forbids it."""
    def __init__(self, message: str, existing_record=None):
        super().__init__(message)
        self.existing_record = existing_record