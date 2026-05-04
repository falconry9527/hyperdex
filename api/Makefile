.PHONY: run build tidy fmt test

run:
	go run ./cmd/api --config configs/config.toml

build:
	CGO_ENABLED=0 go build -o bin/api ./cmd/api

tidy:
	go mod tidy

fmt:
	go fmt ./...

test:
	go test ./...
