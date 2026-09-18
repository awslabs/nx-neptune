# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared base class for the assistant agents (spec §4.4.2 / §9.9).

Ported/trimmed from the sample's ``agents/baseclass.py``: wraps a Strands
``Agent`` and provides retry with exponential backoff on transient Bedrock /
event-loop errors, token/latency metrics access, and a JSON-extraction helper.

Strands is never imported here — retryable Strands exceptions are matched by
class name — so the module stays importable without ``strands-agents`` present.
Subclasses build their own ``strands.Agent`` (see the sample's per-agent
modules) and pass it in, then implement :meth:`_execute_agent`.
"""

import logging
import time
from typing import Optional, Type, TypeVar

from pydantic import BaseModel

from nx_neptune_proxy.assistant.debug_trace import log_invocation, log_result
from nx_neptune_proxy.assistant.json_utils import extract_json

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

# botocore ClientError codes worth retrying (throttling / transient service).
_RETRYABLE_CLIENT_CODES = frozenset(
    {
        "ThrottlingException",
        "TooManyRequestsException",
        "ServiceUnavailableException",
        "ModelTimeoutException",
        "ModelNotReadyException",
    }
)

# Strands / botocore exception class names worth retrying, matched by name so we
# don't import strands here.
_RETRYABLE_EXC_NAMES = frozenset(
    {
        "EventLoopException",
        "EventStreamError",
        "ModelThrottledException",
        "ThrottlingException",
        "ServiceUnavailableException",
    }
)


class BaseAgent:
    """Wraps a Strands ``Agent`` with retry, metrics, and JSON extraction."""

    max_retries: int = 5
    base_delay: float = 1.0
    max_delay: float = 30.0

    def __init__(self, name: str, agent=None):
        self._name = name
        self._agent = agent
        self._result = None

    # --- Accessors --------------------------------------------------------

    def get_agent(self):
        return self._agent

    def get_statistics(self) -> dict:
        """Token/tool/cycle metrics from the last run (empty before any run)."""
        metrics = getattr(self._result, "metrics", None)
        if metrics is None:
            return {}
        return {
            "cycles": getattr(metrics, "cycle_count", None),
            "tools": getattr(metrics, "tool_metrics", None),
            "usage": getattr(metrics, "accumulated_usage", None),
            "accumulated_metrics": getattr(metrics, "accumulated_metrics", None),
        }

    def get_metrics_summary(self) -> dict:
        metrics = getattr(self._result, "metrics", None)
        if metrics is None or not hasattr(metrics, "get_summary"):
            return {}
        return metrics.get_summary()

    # --- Execution --------------------------------------------------------

    def execute_task(self, **kwargs) -> str:
        """Run the agent, retrying transient errors with exponential backoff.

        Returns the string form of the agent result; subclasses typically feed
        it through :meth:`extract_json` to get a structured contract.
        """
        attempt = 0
        log_invocation(self._name, self._safe_prompt(**kwargs))
        while True:
            try:
                self._result = self._execute_agent(**kwargs)
                result_text = str(self._result)
                log_result(self._name, result_text, self.get_statistics())
                return result_text
            except Exception as exc:  # noqa: BLE001 (retry then re-raise)
                if attempt < self.max_retries and self._is_retryable(exc):
                    delay = min(self.max_delay, self.base_delay * (2**attempt))
                    attempt += 1
                    logger.warning(
                        "Retryable error in %s (attempt %d/%d): %s; sleeping %.1fs",
                        self._name,
                        attempt,
                        self.max_retries,
                        exc,
                        delay,
                    )
                    self._sleep(delay)
                    continue
                logger.error("Error in %s task: %s", self._name, exc)
                raise

    def _execute_agent(self, **kwargs):
        """Invoke the wrapped agent. Subclasses must implement this."""
        raise NotImplementedError("Subclasses must implement '_execute_agent'.")

    def _safe_prompt(self, **kwargs) -> str:
        """Best-effort render of the formatted prompt for debug tracing.

        Never raises: if a subclass has no ``_format_prompt`` or it errors, fall
        back to the raw kwargs so tracing can't break a real run.
        """
        fmt = getattr(self, "_format_prompt", None)
        if callable(fmt):
            try:
                return fmt(**kwargs)
            except Exception:  # noqa: BLE001 (tracing must not fail the run)
                pass
        return repr(kwargs)

    # --- Helpers ----------------------------------------------------------

    @staticmethod
    def extract_json(text: str, model_cls: Optional[Type[T]] = None):
        """Recover the JSON contract from a model reply (see json_utils)."""
        return extract_json(text, model_cls)

    def _is_retryable(self, exc: BaseException) -> bool:
        response = getattr(exc, "response", None)
        if isinstance(response, dict):
            code = response.get("Error", {}).get("Code")
            if code in _RETRYABLE_CLIENT_CODES:
                return True
        return any(
            klass.__name__ in _RETRYABLE_EXC_NAMES for klass in type(exc).__mro__
        )

    def _sleep(self, seconds: float) -> None:
        # Isolated so tests can patch out the backoff sleep.
        time.sleep(seconds)
