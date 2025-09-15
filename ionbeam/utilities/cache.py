import json
import re
from abc import ABC, abstractmethod
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar, Union

F = TypeVar("F", bound=Callable[..., Any])


class StorageBackend(ABC):
    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        pass

    @abstractmethod
    async def put(self, key: str, value: Any) -> None:
        pass


class FileStorage(StorageBackend):
    def __init__(self, cache_dir: Union[str, Path] = ".cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_fs_safe_key(self, key: str) -> Path:
        safe_filename = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", key)
        # Limit filename length to avoid filesystem limits
        if len(safe_filename) > 200:
            safe_filename = safe_filename[:200]
        return safe_filename

    def _get_file_path(self, key: str) -> Path:
        safe_filename = self._get_fs_safe_key(key)
        filename = safe_filename + ".json"
        return self.cache_dir / filename

    async def get(self, key: str) -> Optional[Any]:
        file_path = self._get_file_path(key)
        # print(file_path)
        if not file_path.exists():
            return None
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    async def put(self, key: str, value: Any) -> None:
        file_path = self._get_file_path(key)
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(value, f, indent=2, ensure_ascii=False)
        except Exception:
            pass  # Fail silently


# Global storage instance
_storage = FileStorage()


def cached(
    key: Union[str, Callable[..., str]],
    cache_only: bool = False,
    storage: Optional[StorageBackend] = None,
) -> Callable[[F], F]:
    """Lightweight cache decorator."""
    backend = storage or _storage

    def decorator(func: F) -> F:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            cache_key = key if isinstance(key, str) else key(*args, **kwargs)

            # Try cache first
            cached_result = await backend.get(cache_key)
            if cached_result is not None:
                return cached_result

            # If cache_only is True and no cached data, raise exception
            if cache_only:
                raise Exception(f"No cached data found for key: {cache_key}")

            # Execute function and cache result
            result = await func(*args, **kwargs)
            await backend.put(cache_key, result)
            return result

        return wrapper

    return decorator


# Direct cache access
async def get_cached(key: str, storage: Optional[StorageBackend] = None) -> Optional[Any]:
    backend = storage or _storage
    return await backend.get(key)


async def set_cached(key: str, value: Any, storage: Optional[StorageBackend] = None) -> None:
    backend = storage or _storage
    await backend.put(key, value)
