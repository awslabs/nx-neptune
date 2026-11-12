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
import asyncio
import threading

import pytest

from nx_neptune.utils.async_utils import (
    caller_is_being_cancelled,
    has_running_loop,
    run_sync,
)


class TestHasRunningLoop:
    def test_false_outside_a_loop(self):
        assert has_running_loop() is False

    @pytest.mark.asyncio
    async def test_true_inside_a_loop(self):
        assert has_running_loop() is True


class TestRunSync:
    def test_runs_on_its_own_loop_when_none_is_running(self):
        async def work():
            await asyncio.sleep(0)
            return "done"

        assert run_sync(work()) == "done"

    @pytest.mark.asyncio
    async def test_bridges_to_a_worker_thread_inside_a_running_loop(self):
        """asyncio.run would raise here, so the coroutine gets its own thread."""
        caller_thread = threading.current_thread().ident

        async def work():
            return threading.current_thread().ident

        worker_thread = run_sync(work())

        assert worker_thread != caller_thread

    def test_propagates_the_coroutine_s_exception(self):
        async def work():
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            run_sync(work())


class TestCallerIsBeingCancelled:
    def test_false_with_no_current_task(self):
        assert caller_is_being_cancelled() is False

    @pytest.mark.asyncio
    async def test_false_in_a_task_that_was_not_cancelled(self):
        assert caller_is_being_cancelled() is False

    @pytest.mark.asyncio
    async def test_true_once_the_running_task_has_been_cancelled(self):
        """The case that must propagate out of an __aexit__.

        A cancellation *of the caller* - asyncio.timeout, a TaskGroup
        unwinding, a plain task.cancel() - has to reach the caller, unlike a
        CancelledError that gather(return_exceptions=True) merely hands back as
        a value for a child.
        """
        seen = []

        async def body():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                seen.append(caller_is_being_cancelled())
                raise

        task = asyncio.create_task(body())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert seen == [True]
