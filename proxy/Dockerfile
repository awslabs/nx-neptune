FROM python:3.11-slim
WORKDIR /app
COPY nx_neptune/ ./nx_neptune_src/nx_neptune/
COPY nx_plugin/ ./nx_neptune_src/nx_plugin/
COPY pyproject.toml ./nx_neptune_src/
COPY proxy/ ./proxy/
RUN pip install --no-cache-dir ./nx_neptune_src ./proxy && useradd --create-home appuser
USER appuser
EXPOSE 8080
# The container must bind 0.0.0.0 so Docker's published-port forwarding can
# reach it (container-internal 127.0.0.1 is unreachable from the host). Real
# network exposure is controlled by how the port is published (see the run
# target's -p 127.0.0.1:8080:8080). ALLOW_NON_LOOPBACK_BIND acknowledges this
# deliberate bind so the startup guard permits it.
# PYTHONUNBUFFERED=1: flush stdout immediately so the startup token launch URL
# (printed once at startup) reaches the container log driver / `finch logs`.
ENV HOST=0.0.0.0 ALLOW_NON_LOOPBACK_BIND=1 PYTHONUNBUFFERED=1
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD wget -qO /dev/null http://localhost:8080/health
CMD ["python", "-m", "nx_neptune_proxy"]
