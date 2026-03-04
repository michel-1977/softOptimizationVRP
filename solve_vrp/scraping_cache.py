from __future__ import annotations

import json
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple


class ScrapingCache(ABC):
    @abstractmethod
    def get(self, key: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def set(self, key: str, value: Dict[str, Any], ttl_sec: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def backend_name(self) -> str:
        raise NotImplementedError


class MemoryTTLCache(ScrapingCache):
    def __init__(self, max_entries: int = 5000) -> None:
        self.max_entries = max(100, int(max_entries))
        self._lock = threading.Lock()
        self._store: Dict[str, Tuple[float, str]] = {}

    def _prune_expired_locked(self, now: float) -> None:
        expired = [key for key, (expires_at, _) in self._store.items() if expires_at <= now]
        for key in expired:
            self._store.pop(key, None)

    def _evict_one_locked(self) -> None:
        if not self._store:
            return
        oldest_key = min(self._store.keys(), key=lambda key: self._store[key][0])
        self._store.pop(oldest_key, None)

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        now = time.time()
        with self._lock:
            row = self._store.get(str(key))
            if not row:
                return None
            expires_at, raw_json = row
            if expires_at <= now:
                self._store.pop(str(key), None)
                return None

        try:
            payload = json.loads(raw_json)
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None

    def set(self, key: str, value: Dict[str, Any], ttl_sec: int) -> None:
        safe_ttl = max(1, int(ttl_sec))
        expires_at = time.time() + safe_ttl
        raw_json = json.dumps(value, ensure_ascii=False)

        with self._lock:
            self._prune_expired_locked(time.time())
            if len(self._store) >= self.max_entries:
                self._evict_one_locked()
            self._store[str(key)] = (expires_at, raw_json)

    def backend_name(self) -> str:
        return "memory"


class RedisScrapingCache(ScrapingCache):
    def __init__(self, redis_url: str, timeout_sec: int = 2) -> None:
        if not str(redis_url or "").strip():
            raise RuntimeError("SCRAPING_CACHE_REDIS_URL is required for redis backend.")

        try:
            import redis  # type: ignore
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"redis package is not available: {exc}") from exc

        timeout = max(1, int(timeout_sec))
        self._client = redis.from_url(
            redis_url.strip(),
            decode_responses=True,
            socket_timeout=timeout,
            socket_connect_timeout=timeout,
        )
        # Fail fast when redis endpoint is unreachable.
        self._client.ping()

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        raw_json = self._client.get(str(key))
        if not raw_json:
            return None
        try:
            payload = json.loads(raw_json)
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None

    def set(self, key: str, value: Dict[str, Any], ttl_sec: int) -> None:
        safe_ttl = max(1, int(ttl_sec))
        raw_json = json.dumps(value, ensure_ascii=False)
        self._client.setex(str(key), safe_ttl, raw_json)

    def backend_name(self) -> str:
        return "redis"
