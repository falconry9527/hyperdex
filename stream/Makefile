.PHONY: run build tidy fmt test

run:
	go run ./cmd/stream --config configs/config.toml

build:
	CGO_ENABLED=0 go build -o bin/stream ./cmd/stream

tidy:
	go mod tidy

fmt:
	go fmt ./...

test:
	go test ./...
