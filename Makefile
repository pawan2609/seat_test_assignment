.PHONY: up down test burst burst-local

TEST_DATABASE_URL ?= postgres://seats:seats@127.0.0.1:5439/seats?sslmode=disable

## up: build and run the service + Postgres locally on :8080
up:
	docker compose up --build -d app
	@echo "waiting for readiness..."; until curl -sf localhost:$${APP_PORT:-8080}/readyz >/dev/null; do sleep 1; done; echo ready

down:
	docker compose down -v

## test: concurrency integration tests against the compose Postgres
test:
	docker compose up -d db
	@until docker compose exec -T db pg_isready -U seats -d seats >/dev/null 2>&1; do sleep 1; done
	TEST_DATABASE_URL='$(TEST_DATABASE_URL)' go test -race -count=1 ./...

## burst: storm a deployment, e.g. make burst BASE_URL=https://13-127-237-241.sslip.io ADMIN_TOKEN=...
burst:
	./burst.sh $(BASE_URL) $(ADMIN_TOKEN)

## burst-local: storm the local compose stack
burst-local: up
	docker compose run --rm burst
