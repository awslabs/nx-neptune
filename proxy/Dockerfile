FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir ./proxy
EXPOSE 8080
CMD ["uvicorn", "nx_neptune_proxy.main:app", "--host", "0.0.0.0", "--port", "8080"]
