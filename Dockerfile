FROM golang:1.24.1-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/clickhouse-mcp .

FROM alpine:3.21

WORKDIR /app

COPY --from=builder /out/clickhouse-mcp ./clickhouse-mcp

ENV CLICKHOUSE_MCP_TRANSPORT=sse
ENV CLICKHOUSE_MCP_PORT=8082
EXPOSE 8082

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=5 CMD wget -qO- "http://127.0.0.1:${CLICKHOUSE_MCP_PORT:-8082}/healthz" | grep -q "ok" || exit 1

ENTRYPOINT ["sh", "-c", "exec ./clickhouse-mcp -transport=${CLICKHOUSE_MCP_TRANSPORT:-sse} -port=${CLICKHOUSE_MCP_PORT:-8082} -public-base-url=${CLICKHOUSE_MCP_PUBLIC_BASE_URL:-http://localhost:${CLICKHOUSE_MCP_PORT:-8082}} -url=${CLICKHOUSE_MCP_URL:-clickhouse:9000/default} -user=${CLICKHOUSE_MCP_USER:-default} -password=${CLICKHOUSE_MCP_PASSWORD:-} -db=${CLICKHOUSE_MCP_DB:-} -secure=${CLICKHOUSE_MCP_SECURE:-false} -insecure-skip-verify=${CLICKHOUSE_MCP_INSECURE_SKIP_VERIFY:-false} -allow-write=${CLICKHOUSE_MCP_ALLOW_WRITE:-false} -default-query-limit=${CLICKHOUSE_MCP_DEFAULT_LIMIT:-100} -max-query-limit=${CLICKHOUSE_MCP_MAX_LIMIT:-10000}"]
