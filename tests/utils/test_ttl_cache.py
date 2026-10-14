# Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
from unittest.mock import patch

import pytest

from nx_neptune.utils.ttl_cache import TTLCache


def test_get_returns_set_value_within_ttl():
    cache: TTLCache[str, int] = TTLCache(ttl=60)
    cache.set("k", 1)
    assert cache.get("k") == 1


def test_get_missing_key_returns_none():
    cache: TTLCache[str, int] = TTLCache(ttl=60)
    assert cache.get("absent") is None


def test_entry_expires_after_ttl():
    # Drive the clock via time.monotonic so the test is deterministic.
    with patch("nx_neptune.utils.ttl_cache.time.monotonic") as clock:
        clock.return_value = 1000.0
        cache: TTLCache[str, int] = TTLCache(ttl=60)
        cache.set("k", 1)

        clock.return_value = 1059.9  # just before expiry
        assert cache.get("k") == 1

        clock.return_value = 1060.1  # just after expiry
        assert cache.get("k") is None


def test_set_overwrites_and_refreshes_expiry():
    with patch("nx_neptune.utils.ttl_cache.time.monotonic") as clock:
        clock.return_value = 0.0
        cache: TTLCache[str, int] = TTLCache(ttl=10)
        cache.set("k", 1)
        clock.return_value = 9.0
        cache.set("k", 2)  # refreshes expiry to 19.0
        clock.return_value = 18.0
        assert cache.get("k") == 2  # still alive thanks to the refresh


def test_ttl_zero_disables_caching():
    cache: TTLCache[str, int] = TTLCache(ttl=0)
    cache.set("k", 1)
    assert cache.get("k") is None


def test_clear_removes_all_entries():
    cache: TTLCache[str, int] = TTLCache(ttl=60)
    cache.set("a", 1)
    cache.set("b", 2)
    cache.clear()
    assert cache.get("a") is None
    assert cache.get("b") is None


def test_exceeding_maxsize_evicts_something():
    cache: TTLCache[str, int] = TTLCache(ttl=60, maxsize=2)
    cache.set("a", 1)
    cache.set("b", 2)
    cache.set("c", 3)  # over capacity -> one arbitrary entry evicted
    assert len(cache._store) == 2
    assert cache.get("c") == 3  # the just-inserted key is retained
    # Exactly one of the earlier keys survives.
    survivors = [k for k in ("a", "b") if cache.get(k) is not None]
    assert len(survivors) == 1


def test_overwrite_does_not_evict():
    cache: TTLCache[str, int] = TTLCache(ttl=60, maxsize=2)
    cache.set("a", 1)
    cache.set("b", 2)
    cache.set("a", 10)  # overwrite existing key -> no eviction
    assert cache.get("a") == 10
    assert cache.get("b") == 2


def test_maxsize_must_be_positive():
    with pytest.raises(ValueError):
        TTLCache(ttl=60, maxsize=0)
