.PHONY: dev install

dev:              ## Run the proxy in development mode with auto-reload.
	uvicorn nx_neptune_proxy.main:app --reload --port 8080

install:          ## Install the proxy in editable mode.
	pip install -e .
