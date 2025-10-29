import json
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar, Union
from uuid import UUID

F = TypeVar("F", bound=Callable[..., Any])

class StorageBackend(ABC):
    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        pass

    @abstractmethod
    async def put(self, key: str, value: Any) -> None:
        pass


def sanitize_key(key: str, max_length: int = 200) -> str:
    safe = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", key)
    return safe[:max_length] if len(safe) > max_length else safe


def compose_event_key(event_id: UUID, base_key: str) -> str:
    """Compose event-scoped key: {event_id}/{base_key}"""
    return f"{event_id}/{base_key}"


def parse_event_key(key: str) -> tuple[str, str]:
    """Split event-scoped key into (event_id, base_key)"""
    if "/" not in key:
        raise ValueError(f"Invalid event-scoped key format: {key}. Expected: {{event_id}}/{{base_key}}")
    parts = key.split("/", 1)
    return parts[0], parts[1]


@dataclass(frozen=True)
class FileStorageConfig:
    cache_dir: Path = field(default_factory=lambda: Path(".cache"))
    encoding: str = "utf-8"
    indent: int = 2

class FileStorage(StorageBackend):
    """File-based event-scoped cache: .cache/{event_id}/key.json"""

    def __init__(self, config: Optional[FileStorageConfig] = None):
        self.config = config or FileStorageConfig()
        if isinstance(self.config.cache_dir, str):
            object.__setattr__(self.config, "cache_dir", Path(self.config.cache_dir))
        self.config.cache_dir.mkdir(parents=True, exist_ok=True)

    def _resolve_file_path(self, key: str) -> Path:
        event_id, base_key = parse_event_key(key)
        event_dir = self.config.cache_dir / sanitize_key(event_id)
        event_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{sanitize_key(base_key)}.json"
        return event_dir / filename

    async def get(self, key: str) -> Optional[Any]:
        file_path = self._resolve_file_path(key)
        
        if not file_path.exists():
            return None
        
        try:
            with open(file_path, "r", encoding=self.config.encoding) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None

    async def put(self, key: str, value: Any) -> None:
        file_path = self._resolve_file_path(key)
        
        try:
            with open(file_path, "w", encoding=self.config.encoding) as f:
                json.dump(value, f, indent=self.config.indent, ensure_ascii=False)
        except (TypeError, IOError):
            pass  # Cache is best-effort

class MemoryStorage(StorageBackend):
    """In-memory cache for testing"""

    def __init__(self):
        self._cache: dict[str, Any] = {}

    async def get(self, key: str) -> Optional[Any]:
        return self._cache.get(key)

    async def put(self, key: str, value: Any) -> None:
        self._cache[key] = value


_default_storage: StorageBackend = FileStorage()


def get_default_storage() -> StorageBackend:
    return _default_storage


def set_default_storage(storage: StorageBackend) -> None:
    global _default_storage
    _default_storage = storage

def cached(
    key: Union[str, Callable[..., str]],
    storage: Optional[StorageBackend] = None,
    event_id: Optional[UUID] = None,
    cache_enabled: bool = True,
) -> Callable[[F], F]:
    """Cache decorator with event scoping.
    
    Args:
        key: Cache key or key generator function
        storage: Optional custom storage backend
        event_id: Required event ID for scoped caching
        cache_enabled: If False, skip cache; if True, use cache-aside pattern
    """
    if event_id is None:
        raise ValueError("event_id is required for event-scoped caching")
    
    backend = storage or get_default_storage()

    def decorator(func: F) -> F:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if not cache_enabled:
                return await func(*args, **kwargs)
            
            base_key = key if isinstance(key, str) else key(*args, **kwargs)
            cache_key = compose_event_key(event_id, base_key)
            
            cached_result = await backend.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            result = await func(*args, **kwargs)
            await backend.put(cache_key, result)
            return result

        return wrapper  # type: ignore

    return decorator
