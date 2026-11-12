# Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
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

"""Helpers for code that has to work both inside and outside an event loop.

The library's graph operations are ``async``, while some of the hooks that have
to run them are not - a context manager's ``__exit__``, a ``__del__``, a
synchronous public method. These three helpers answer the questions that come
up at every such boundary: is there already a loop running, how do I run a
coroutine from here, and is the `CancelledError` I am looking at a cancellation
of my own caller or just a value handed back by ``gather``.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor

__all__ = [
    "has_running_loop",
    "run_sync",
    "caller_is_being_cancelled",
]


def has_running_loop() -> bool:
    """Whether the calling thread is inside a running event loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def caller_is_being_cancelled() -> bool:
    """Whether the running task itself has been asked to cancel.

    This separates the two `CancelledError`s that can reach an ``__aexit__``.
    One is a cancellation *of the caller* - ``asyncio.timeout``, a
    ``TaskGroup`` unwinding, a plain ``task.cancel()`` - which must propagate,
    because ``timeout`` and ``TaskGroup`` rely on seeing it to convert it into
    ``TimeoutError`` or to finish unwinding. The other is the `CancelledError`
    that ``gather(return_exceptions=True)`` hands back *as a value* for a
    cancelled child operation; that one is a failure of the work, is reported
    as such, and must not be mistaken for the caller going away.

    ``Task.cancelling()`` counts outstanding cancellation requests against the
    current task, which is non-zero only in the first case.

    Outside a running loop there is no task to cancel, so the answer is False -
    ``asyncio.current_task`` raises there rather than returning ``None``.
    """
    try:
        task = asyncio.current_task()
    except RuntimeError:
        return False
    return task is not None and task.cancelling() > 0


def run_sync(coro):
    """Run ``coro`` to completion from synchronous code.

    ``asyncio.run`` raises when a loop is already running, so in that case the
    coroutine runs on its own loop in a worker thread. That bridge blocks the
    caller's loop until the coroutine finishes, which also holds Ctrl-C, so
    async callers should await instead of coming through here - a caller that
    has an async path should say so when it has to fall back to this one.
    """
    if not has_running_loop():
        return asyncio.run(coro)

    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()
