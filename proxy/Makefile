.PHONY: dev install docker-build docker-run docker-stop docker-clean

dev:              ## Run the proxy in development mode with auto-reload.
	uvicorn nx_neptune_proxy.main:app --reload --port 8080

install:          ## Install the proxy in editable mode.
	pip install -e .

docker-build:     ## Build the container image.
	finch build -t nx-neptune-proxy -f Dockerfile ..

docker-run:       ## Run the container (uses AWS credentials from current shell).
	finch run --name nx-neptune-proxy -p 8080:8080 \
		-e AWS_ACCESS_KEY_ID \
		-e AWS_SECRET_ACCESS_KEY \
		-e AWS_SESSION_TOKEN \
		-e AWS_REGION \
		-e AWS_DEFAULT_REGION \
		nx-neptune-proxy

docker-stop:      ## Stop the running container.
	@finch stop nx-neptune-proxy >/dev/null 2>&1 || true

docker-clean:     ## Remove container and image.
	@finch rm nx-neptune-proxy >/dev/null 2>&1 || true
	@finch rmi nx-neptune-proxy >/dev/null 2>&1 || true
