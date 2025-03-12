from collections import OrderedDict
from typing import Any, Callable


class ActiveLRUCache:
    __slots__ = ["max_size", "cache", "current_access"]

    def __init__(self, max_size: int):
        self.max_size: int = max_size
        self.cache: OrderedDict[bytes, Any] = OrderedDict()
        self.current_access: set[Any] = set()

    def get(self, key: bytes, load_func: Callable[..., Any]) -> Any:
        """Retrieve item from cache or load it using load_func."""
        if key in self.cache:
            self.current_access.add(key)
            self.cache.move_to_end(key)  # Mark as recently used
            return self.cache[key]

        value = load_func()
        if value is not None:
            self._add(key, value)
        return value

    def add(self, key: bytes, value: Any) -> None:
        self._add(key, value)

    def drop(self) -> None:
        self.cache = OrderedDict()
        self.current_access = set()

    def _add(self, key: bytes, value: Any) -> None:
        if len(self.cache) >= self.max_size:
            self._evict()
        self.cache[key] = value
        self.current_access.add(key)

    def _evict(self) -> None:
        """Evicts least recently used items that were not accessed in the last cycle."""
        to_remove = [k for k in self.cache.keys() if k not in self.current_access]
        for k in to_remove:
            del self.cache[k]
        while (
            len(self.cache) >= self.max_size
        ):  # In case everything was accessed, evict normally
            self.cache.popitem(last=False)

    def new_cycle(self) -> None:
        """Call this at the start of a new access cycle."""
        self.current_access.clear()
