FROM python:3.11-slim AS builder
WORKDIR /build
COPY proxy/pyproject.toml proxy/src ./proxy/
RUN pip install --no-cache-dir --prefix=/install ./proxy

FROM python:3.11-slim
WORKDIR /app
COPY --from=builder /install /usr/local
RUN useradd --create-home appuser
USER appuser
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"
CMD ["uvicorn", "nx_neptune_proxy.app:app", "--host", "0.0.0.0", "--port", "8080"]
