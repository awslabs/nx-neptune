# Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

"""A small, dependency-free, thread-safe cache with per-entry TTL eviction.

The standard library has no TTL dict (``functools.lru_cache`` evicts by size,
not time), so this provides a minimal ``get``/``set`` interface for values that
should expire after a configurable period.

* **Time:** each entry expires ``ttl`` seconds after it was set; checked lazily
  on ``get`` (an expired entry found on read is deleted).
* **Size:** at most ``maxsize`` entries are kept. When full, inserting a new
  key evicts one arbitrary existing entry.

Monotonic time is used so the TTL is unaffected by wall-clock adjustments.
"""

from __future__ import annotations

import threading
import time
from typing import Dict, Generic, Hashable, Optional, Tuple, TypeVar

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")

DEFAULT_MAXSIZE = 128


class TTLCache(Generic[K, V]):
    """Thread-safe mapping whose entries expire ``ttl`` seconds after insertion.

    Args:
        ttl: Time-to-live in seconds for each entry. A value <= 0 disables
            caching: ``get`` always misses and ``set`` is a no-op.
        maxsize: Maximum number of entries. When exceeded, one arbitrary entry
            is evicted. Must be positive.
    """

    def __init__(self, ttl: float, maxsize: int = DEFAULT_MAXSIZE):
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        self._ttl = ttl
        self._maxsize = maxsize
        # key -> (expiry_monotonic, value)
        self._store: Dict[K, Tuple[float, V]] = {}
        self._lock = threading.Lock()

    def get(self, key: K) -> Optional[V]:
        """Return the cached value for ``key``, or ``None`` if absent/expired."""
        if self._ttl <= 0:
            return None
        now = time.monotonic()
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            expiry, value = entry
            if expiry <= now:
                del self._store[key]
                return None
            return value

    def set(self, key: K, value: V) -> None:
        """Cache ``value`` under ``key`` for ``ttl`` seconds (no-op if disabled).

        If at capacity, one arbitrary entry is evicted to make room.
        """
        if self._ttl <= 0:
            return
        expiry = time.monotonic() + self._ttl
        with self._lock:
            if key not in self._store and len(self._store) >= self._maxsize:
                # Evict one arbitrary entry.
                self._store.pop(next(iter(self._store)))
            self._store[key] = (expiry, value)

    def clear(self) -> None:
        """Remove all entries (useful in tests)."""
        with self._lock:
            self._store.clear()
