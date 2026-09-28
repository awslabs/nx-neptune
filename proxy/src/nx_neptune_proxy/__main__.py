# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Process entrypoint: validate the bind address, then launch uvicorn.

Run as ``python -m nx_neptune_proxy``. This wraps the uvicorn launch so the
bind address is validated *before* the socket is opened. Because the same
value is both checked here and passed to ``uvicorn.run``, the address that
gets validated is guaranteed to be the address that gets bound.

"""

import sys

import uvicorn

from nx_neptune_proxy.config import Settings

# A request addressed to any of these never leaves the local machine.
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})


def check_bind_allowed(host: str, allow_non_loopback: bool) -> str | None:
    """Return an error message if binding to ``host`` is not allowed, else None.

    Binding to loopback is always allowed. Binding to any other address is
    allowed only when explicitly acknowledged via ``allow_non_loopback``.
    """
    if host in _LOOPBACK_HOSTS:
        return None
    if allow_non_loopback:
        return None
    return (
        f"Refusing to start: HOST={host!r} is not a loopback address and "
        "ALLOW_NON_LOOPBACK_BIND is not set. This would expose the proxy on "
        "the network. Bind to 127.0.0.1, or set ALLOW_NON_LOOPBACK_BIND=1 to "
        "expose it deliberately (e.g. inside a container whose published "
        "ports are restricted)."
    )


def main() -> None:
    settings = Settings.from_env()
    error = check_bind_allowed(settings.host, settings.allow_non_loopback_bind)
    if error:
        sys.exit(error)
    uvicorn.run(
        "nx_neptune_proxy.app:app",
        host=settings.host,
        port=settings.port,
    )


if __name__ == "__main__":
    main()
