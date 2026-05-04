FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/api ./cmd/api

FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/api /usr/local/bin/api
COPY configs/config.toml /app/config.toml
USER app
ENTRYPOINT ["/usr/local/bin/api", "--config", "/app/config.toml"]
