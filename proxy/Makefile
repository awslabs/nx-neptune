.PHONY: dev install test build run stop clean ui

RUNTIME ?= finch

ui:               ## Build frontend into ui/ directory.
	cd ui-src && npm install && npm run build

dev:              ## Run with auto-reload for development.
	uvicorn nx_neptune_proxy.app:app --reload --port 8080

install:          ## Install proxy in editable mode with test deps.
	pip install -e ".[test]"

test:             ## Run tests.
	pytest tests/ -v

build:            ## Build container image.
	$(RUNTIME) build -t nx-neptune-proxy -f Dockerfile ..

run:              ## Run container with AWS credentials from environment.
	$(RUNTIME) run --rm --name nx-neptune-proxy -p 8080:8080 \
		-e AWS_ACCESS_KEY_ID \
		-e AWS_SECRET_ACCESS_KEY \
		-e AWS_SESSION_TOKEN \
		-e AWS_DEFAULT_REGION \
		-e CORS_ALLOWED_ORIGINS \
		-e LOG_LEVEL \
		nx-neptune-proxy

stop:             ## Stop running container.
	@$(RUNTIME) stop nx-neptune-proxy 2>/dev/null || true

clean:            ## Remove container and image.
	@$(RUNTIME) rm nx-neptune-proxy 2>/dev/null || true
	@$(RUNTIME) rmi nx-neptune-proxy 2>/dev/null || true

up:               ## Start proxy + Graph Explorer with compose.
	$(RUNTIME) compose up --build --force-recreate -d

down:             ## Stop compose services.
	$(RUNTIME) compose down

logs:             ## Tail compose logs.
	$(RUNTIME) compose logs -f
