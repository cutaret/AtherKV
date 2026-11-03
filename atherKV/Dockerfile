FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /aether-server ./cmd/aether-server
FROM alpine:3.20
WORKDIR /data
COPY --from=builder /aether-server /usr/local/bin/aether-server
EXPOSE 8080
CMD ["aether-server", "-port", "8080"]
