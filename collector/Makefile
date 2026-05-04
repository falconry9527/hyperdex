.PHONY: up down migrate run build tidy fmt test sync-fills

DB_DSN ?= postgres://thor:thor1239@127.0.0.1:5432/seabond?sslmode=disable
MIGRATE ?= migrate

up:
	docker compose -f deploy/docker-compose.yml up -d

down:
	docker compose -f deploy/docker-compose.yml down

migrate:
	$(MIGRATE) -path migrations -database "$(DB_DSN)" up

migrate-down:
	$(MIGRATE) -path migrations -database "$(DB_DSN)" down 1

run:
	go run ./cmd/collector --config configs/config.toml

build:
	CGO_ENABLED=0 go build -o bin/collector ./cmd/collector

tidy:
	go mod tidy

fmt:
	go fmt ./...

test:
	go test ./...

# On-demand: refresh user_fills for every addr in [userfills].addrs.
# Pass ADDR=0x... or DAYS=N to override config:
#   make sync-fills ADDR=0xabc DAYS=30
sync-fills:
	go run ./cmd/sync-fills --config configs/config.toml \
		$(if $(ADDR),--addr $(ADDR)) \
		$(if $(DAYS),--days $(DAYS))
