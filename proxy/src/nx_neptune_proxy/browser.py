# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Open the launch URL in the local browser once the server accepts connections.

The URL carries the per-run token, so it is not passed on the browser's
command line, where other local users could read it from the process list.
Instead a redirect page readable only by the current user is written to a
temp file and that file is opened, the same approach Jupyter uses. The file
is deleted shortly after the browser is launched, or at exit if that fails.
"""

import atexit
import html
import logging
import os
import socket
import tempfile
import threading
import time
import webbrowser
from pathlib import Path

logger = logging.getLogger("nx_neptune_proxy")

_WAIT_SECONDS = 15.0
_POLL_SECONDS = 0.2
# How long the redirect page (which contains the token) stays on disk after
# the browser is launched. Long enough for a cold browser start.
_CLEANUP_SECONDS = 30.0


def _wait_until_listening(host: str, port: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=_POLL_SECONDS):
                return True
        except OSError:
            time.sleep(_POLL_SECONDS)
    return False


def _write_redirect_page(url: str) -> Path:
    fd, name = tempfile.mkstemp(prefix="nx-neptune-proxy-", suffix=".html")
    escaped = html.escape(url, quote=True)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(
            "<!DOCTYPE html><html><head><meta charset=\"utf-8\">"
            f"<meta http-equiv=\"refresh\" content=\"0;url={escaped}\">"
            "<title>nx-neptune proxy</title></head>"
            f"<body><a href=\"{escaped}\">Open the nx-neptune proxy UI</a></body></html>"
        )
    path = Path(name)
    atexit.register(path.unlink, missing_ok=True)
    return path


def _remove(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _schedule_removal(path: Path, delay: float) -> None:
    timer = threading.Timer(delay, _remove, args=(path,))
    timer.daemon = True
    timer.start()


def _open_when_ready(url: str, host: str, port: int) -> None:
    if not _wait_until_listening(host, port, _WAIT_SECONDS):
        logger.info("Proxy not reachable yet; open the launch URL printed above.")
        return
    page = None
    try:
        page = _write_redirect_page(url)
        if webbrowser.open(page.as_uri()):
            # webbrowser.open returns before the browser reads the file, so
            # keep it briefly, then remove it so the token isn't left on disk
            # for the rest of the run. atexit remains as a fallback.
            _schedule_removal(page, _CLEANUP_SECONDS)
            return
        logger.info("No browser available; open the launch URL printed above.")
    except Exception as exc:
        logger.info(f"Could not open a browser ({exc!r}); open the launch URL printed above.")
    if page is not None:
        _remove(page)


def open_launch_url(url: str, host: str, port: int) -> None:
    """Open ``url`` in a background thread once ``host:port`` accepts connections."""
    threading.Thread(
        target=_open_when_ready, args=(url, host, port), daemon=True
    ).start()
