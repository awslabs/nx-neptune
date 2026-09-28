.PHONY: start stop build relock

start:            ## Start the docs/blog dev server (http://localhost:4321/nx-neptune).
	npm run dev

stop:             ## Stop the dev server.
	@lsof -ti tcp:4321 | xargs kill 2>/dev/null || echo "not running"

build:            ## Build the static site into dist/.
	npm run build

relock:           ## Rebuild the lockfile from scratch (fixes stale transitive deps).
	rm -rf node_modules package-lock.json
	npm install
