# ClickHouse MCP Server

[![Go Version](https://img.shields.io/github/go-mod/go-version/Headcrab/clickhouse-mcp)](https://go.dev)
[![License](https://img.shields.io/github/license/Headcrab/clickhouse-mcp)](LICENSE)

Нормальный MCP-сервер для ClickHouse с безопасными дефолтами, `stdio` и `SSE`, Docker-запуском и read-only режимом по умолчанию.

## Что умеет

- показать список баз данных;
- показать таблицы в базе;
- показать схему таблицы;
- выполнить SQL через MCP-инструмент `query`;
- работать через `stdio` и `SSE`;
- запускаться локально, в Docker и в `docker compose`.

## Что важно про безопасность

- По умолчанию сервер работает в read-only режиме.
- `INSERT`, `ALTER`, `CREATE`, `DROP`, `TRUNCATE`, `RENAME`, `OPTIMIZE`, `SYSTEM`, `GRANT`, `REVOKE` и другие write/admin запросы запрещены, пока не включен `--allow-write`.
- Для `SELECT` без `LIMIT` сервер сам добавляет лимит по умолчанию.
- `--secure` теперь реально включает TLS с проверкой сертификата.
- Если нужен небезопасный TLS для dev/self-host, включайте отдельно `--insecure-skip-verify`.

## Быстрый старт

### Сборка

```bash
go build -o clickhouse-mcp
```

### Запуск через stdio

```bash
./clickhouse-mcp \
  -transport stdio \
  -url localhost:9000/default \
  -user default \
  -password clickhouse
```

### Запуск через SSE

```bash
./clickhouse-mcp \
  -transport sse \
  -port 8082 \
  -public-base-url http://localhost:8082 \
  -url localhost:9000/default \
  -user default \
  -password clickhouse
```

### Тестовый режим

```bash
./clickhouse-mcp -test
```

Он печатает реальные примеры `tools/call`, которые можно слать MCP-клиенту.

## Docker

### Один контейнер

```bash
docker build -t clickhouse-mcp .

docker run --rm -p 8082:8082 \
  -e CLICKHOUSE_MCP_TRANSPORT=sse \
  -e CLICKHOUSE_MCP_PORT=8082 \
  -e CLICKHOUSE_MCP_PUBLIC_BASE_URL=http://localhost:8082 \
  -e CLICKHOUSE_MCP_URL=host.docker.internal:9000/default \
  -e CLICKHOUSE_MCP_USER=default \
  -e CLICKHOUSE_MCP_PASSWORD=clickhouse \
  clickhouse-mcp
```

Если ClickHouse стоит на хосте Linux, добавьте `--add-host=host.docker.internal:host-gateway`.
Для Docker-сценариев у `default` должен быть задан пароль: без него официальный образ ClickHouse режет сетевой доступ к этому пользователю.

### Docker Compose

```bash
docker compose up -d
```

По умолчанию `compose` поднимает:

- `clickhouse` на `9000` и `8123`;
- `clickhouse-mcp` на `8082`.
- логин ClickHouse: `default`
- пароль ClickHouse: `clickhouse`

Healthcheck MCP:

```text
http://localhost:8082/healthz
```

## Переменные окружения

Основные:

- `CLICKHOUSE_MCP_TRANSPORT` — `stdio` или `sse`
- `CLICKHOUSE_MCP_PORT` — порт SSE сервера
- `CLICKHOUSE_MCP_PUBLIC_BASE_URL` — публичный базовый URL для advertised SSE endpoint
- `CLICKHOUSE_MCP_URL` — `host:port/database`
- `CLICKHOUSE_MCP_USER`
- `CLICKHOUSE_MCP_PASSWORD`
- `CLICKHOUSE_MCP_DB` — переопределяет базу из URL
- `CLICKHOUSE_MCP_SECURE`
- `CLICKHOUSE_MCP_INSECURE_SKIP_VERIFY`
- `CLICKHOUSE_MCP_ALLOW_WRITE`
- `CLICKHOUSE_MCP_DEFAULT_LIMIT`
- `CLICKHOUSE_MCP_MAX_LIMIT`

Для обратной совместимости сервер понимает и старые переменные:

- `CLICKHOUSE_URL`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_DB`
- `CLICKHOUSE_SECURE`
- `PORT`

## Флаги

- `-transport`, `-t` — `stdio` или `sse`
- `-test` — печатает рабочие MCP-примеры
- `-url` — `host:port/database`
- `-user`
- `-password`
- `-db`
- `-secure`
- `-insecure-skip-verify`
- `-allow-write`
- `-port`
- `-public-base-url`
- `-default-query-limit`
- `-max-query-limit`

## Примеры MCP-запросов

### Список баз данных

```json
{
  "jsonrpc": "2.0",
  "id": "1",
  "method": "tools/call",
  "params": {
    "name": "get_databases",
    "arguments": {}
  }
}
```

### Список таблиц

```json
{
  "jsonrpc": "2.0",
  "id": "2",
  "method": "tools/call",
  "params": {
    "name": "get_tables",
    "arguments": {
      "database": "default"
    }
  }
}
```

### Схема таблицы

```json
{
  "jsonrpc": "2.0",
  "id": "3",
  "method": "tools/call",
  "params": {
    "name": "get_schema",
    "arguments": {
      "database": "default",
      "table": "my_table"
    }
  }
}
```

### SQL запрос

```json
{
  "jsonrpc": "2.0",
  "id": "4",
  "method": "tools/call",
  "params": {
    "name": "query",
    "arguments": {
      "query": "SELECT * FROM default.my_table",
      "limit": 100
    }
  }
}
```

## Настройка MCP клиента

### Stdio

```json
{
  "mcpServers": {
    "clickhouse": {
      "command": "/path/to/clickhouse-mcp",
      "args": [
        "-transport",
        "stdio",
        "-url",
        "localhost:9000/default",
        "-user",
        "default",
        "-password",
        "clickhouse"
      ]
    }
  }
}
```

### SSE

```json
{
  "mcpServers": {
    "clickhouse": {
      "url": "http://localhost:8082/sse"
    }
  }
}
```

Если сервер стоит за proxy или доступен не по `localhost`, обязательно задайте `-public-base-url` или `CLICKHOUSE_MCP_PUBLIC_BASE_URL`.

## Ограничения первой версии

- Нет auth-слоя для SSE.
- Нет метрик и отдельного `/ready` кроме простого `/healthz`.
- SQL-политика намеренно строгая: всё нераспознанное в read-only режиме режется.
- Парсер SQL не пытается быть полноценным SQL parser; он решает продуктовую задачу безопасного ограничения запросов.

## Разработка

```bash
go vet ./...
go test ./...
docker build -t clickhouse-mcp:test .
```

Интеграционные тесты включаются так:

```bash
CLICKHOUSE_MCP_INTEGRATION=1 \
CLICKHOUSE_MCP_TEST_URL=localhost:9000/default \
CLICKHOUSE_MCP_TEST_USER=default \
CLICKHOUSE_MCP_TEST_PASSWORD=clickhouse \
go test ./...
```

## Лицензия

MIT. Подробности в [LICENSE](LICENSE).
