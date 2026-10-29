.PHONY: start build relock

start:            ## Start the docs/blog dev server (http://localhost:4321/nx-neptune).
	npm run dev

build:            ## Build the static site into dist/.
	npm run build

relock:           ## Rebuild the lockfile from scratch and print a clean version diff.
	@jq -r '.packages | to_entries[] | select(.key != "") | "\(.key | sub("^node_modules/";""))@\(.value.version)"' package-lock.json 2>/dev/null | sort > /tmp/docs-npm-before.txt || true
	rm -rf node_modules package-lock.json
	npm install
	@jq -r '.packages | to_entries[] | select(.key != "") | "\(.key | sub("^node_modules/";""))@\(.value.version)"' package-lock.json 2>/dev/null | sort > /tmp/docs-npm-after.txt || true
	@echo "=== dependency version changes ==="; diff /tmp/docs-npm-before.txt /tmp/docs-npm-after.txt || true
