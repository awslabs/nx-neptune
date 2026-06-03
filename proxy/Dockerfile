FROM python:3.11-slim
WORKDIR /app
COPY nx_neptune/ ./nx_neptune_src/nx_neptune/
COPY nx_plugin/ ./nx_neptune_src/nx_plugin/
COPY pyproject.toml ./nx_neptune_src/
COPY proxy/ ./proxy/
RUN pip install --no-cache-dir ./nx_neptune_src ./proxy && useradd --create-home appuser
USER appuser
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD wget -qO /dev/null http://localhost:8080/health
CMD ["uvicorn", "nx_neptune_proxy.app:app", "--host", "0.0.0.0", "--port", "8080"]
