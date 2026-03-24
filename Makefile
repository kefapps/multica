.PHONY: dev daemon cli build test migrate-up migrate-down sqlc seed clean setup start stop check worktree-env setup-main start-main stop-main check-main setup-worktree start-worktree stop-worktree check-worktree

MAIN_ENV_FILE ?= .env
WORKTREE_ENV_FILE ?= .env.worktree
ENV_FILE ?= $(if $(wildcard $(MAIN_ENV_FILE)),$(MAIN_ENV_FILE),$(if $(wildcard $(WORKTREE_ENV_FILE)),$(WORKTREE_ENV_FILE),$(MAIN_ENV_FILE)))

ifneq ($(wildcard $(ENV_FILE)),)
include $(ENV_FILE)
endif

POSTGRES_DB ?= multica
POSTGRES_USER ?= multica
POSTGRES_PASSWORD ?= multica
POSTGRES_PORT ?= 5432
PORT ?= 8080
FRONTEND_PORT ?= 3000
FRONTEND_ORIGIN ?= http://localhost:$(FRONTEND_PORT)
DATABASE_URL ?= postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@localhost:$(POSTGRES_PORT)/$(POSTGRES_DB)?sslmode=disable
NEXT_PUBLIC_API_URL ?= http://localhost:$(PORT)
NEXT_PUBLIC_WS_URL ?= ws://localhost:$(PORT)/ws
GOOGLE_REDIRECT_URI ?= $(FRONTEND_ORIGIN)/auth/callback
MULTICA_SERVER_URL ?= ws://localhost:$(PORT)/ws
COMPOSE_PROJECT_NAME ?= super_multica

export

COMPOSE := docker compose --env-file $(ENV_FILE)

# ---------- One-click commands ----------

# First-time setup: install deps, start DB, run migrations
setup:
	@echo "==> Using env file: $(ENV_FILE)"
	@echo "==> Installing dependencies..."
	pnpm install
	@echo "==> Starting PostgreSQL..."
	@if pg_isready -h localhost -p $(POSTGRES_PORT) -U $(POSTGRES_USER) -d $(POSTGRES_DB) > /dev/null 2>&1; then \
		echo "    PostgreSQL already running, skipping docker compose up."; \
	else \
		$(COMPOSE) up -d; \
		echo "==> Waiting for PostgreSQL to be ready..."; \
		until $(COMPOSE) exec -T postgres pg_isready -U $(POSTGRES_USER) -d $(POSTGRES_DB) > /dev/null 2>&1; do \
			sleep 1; \
		done; \
	fi
	@echo "==> Running migrations..."
	cd server && go run ./cmd/migrate up
	@echo ""
	@echo "✓ Setup complete! Run 'make seed' if you want example data, then 'make start' to launch the app."

# Start all services (backend + frontend)
start:
	@echo "Using env file: $(ENV_FILE)"
	@echo "Backend: http://localhost:$(PORT)"
	@echo "Frontend: http://localhost:$(FRONTEND_PORT)"
	@if pg_isready -h localhost -p $(POSTGRES_PORT) -U $(POSTGRES_USER) -d $(POSTGRES_DB) > /dev/null 2>&1; then \
		echo "PostgreSQL already running, skipping docker compose up."; \
	else \
		$(COMPOSE) up -d; \
		until $(COMPOSE) exec -T postgres pg_isready -U $(POSTGRES_USER) -d $(POSTGRES_DB) > /dev/null 2>&1; do \
			sleep 1; \
		done; \
	fi
	@echo "Starting backend and frontend..."
	@trap 'kill 0' EXIT; \
		(cd server && go run ./cmd/server) & \
		pnpm dev:web & \
		wait

# Stop all services
stop:
	@echo "Stopping services..."
	@-lsof -ti:$(PORT) | xargs kill -9 2>/dev/null
	@-lsof -ti:$(FRONTEND_PORT) | xargs kill -9 2>/dev/null
	$(COMPOSE) down
	@echo "✓ All services stopped."

# Full verification: typecheck + unit tests + Go tests + E2E
check:
	@bash scripts/check.sh

worktree-env:
	@bash scripts/init-worktree-env.sh .env.worktree

setup-main:
	@$(MAKE) setup ENV_FILE=$(MAIN_ENV_FILE)

start-main:
	@$(MAKE) start ENV_FILE=$(MAIN_ENV_FILE)

stop-main:
	@$(MAKE) stop ENV_FILE=$(MAIN_ENV_FILE)

check-main:
	@ENV_FILE=$(MAIN_ENV_FILE) bash scripts/check.sh

setup-worktree:
	@$(MAKE) setup ENV_FILE=$(WORKTREE_ENV_FILE)

start-worktree:
	@$(MAKE) start ENV_FILE=$(WORKTREE_ENV_FILE)

stop-worktree:
	@$(MAKE) stop ENV_FILE=$(WORKTREE_ENV_FILE)

check-worktree:
	@ENV_FILE=$(WORKTREE_ENV_FILE) bash scripts/check.sh

# ---------- Individual commands ----------

# Go server
dev:
	cd server && go run ./cmd/server

daemon:
	cd server && MULTICA_REPOS_ROOT="${MULTICA_REPOS_ROOT:-$(abspath .)}" go run ./cmd/multica daemon

cli:
	cd server && go run ./cmd/multica $(ARGS)

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)

build:
	cd server && go build -o bin/server ./cmd/server
	cd server && go build -ldflags "-X main.version=$(VERSION) -X main.commit=$(COMMIT)" -o bin/multica-cli ./cmd/multica

test:
	cd server && go test ./...

# Database
migrate-up:
	cd server && go run ./cmd/migrate up

migrate-down:
	cd server && go run ./cmd/migrate down

sqlc:
	cd server && sqlc generate

seed:
	cd server && go run ./cmd/seed

# Cleanup
clean:
	rm -rf server/bin server/tmp
