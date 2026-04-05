package main

import (
	"flag"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"clickhouse-mcp/app"
)

func main() {
	// Определение флагов командной строки
	var (
		transport          string
		testMode           bool
		clickhouseURL      string
		username           string
		password           string
		database           string
		secure             bool
		insecureSkipVerify bool
		port               int
		publicBaseURL      string
		allowWrite         bool
		defaultQueryLimit  int
		maxQueryLimit      int
	)

	// Настройки транспорта и тестового режима
	flag.StringVar(&transport, "t", envString(app.DefaultTransport, "CLICKHOUSE_MCP_TRANSPORT"), "Transport type (stdio or sse)")
	flag.StringVar(&transport, "transport", envString(app.DefaultTransport, "CLICKHOUSE_MCP_TRANSPORT"), "Transport type (stdio or sse)")
	flag.BoolVar(&testMode, "test", envBool(false, "CLICKHOUSE_MCP_TEST"), "Run in test mode")
	flag.IntVar(&port, "port", envInt(app.DefaultSSEPort, "CLICKHOUSE_MCP_PORT", "PORT"), "Port for SSE server")
	flag.StringVar(&publicBaseURL, "public-base-url", envString("", "CLICKHOUSE_MCP_PUBLIC_BASE_URL"), "Public base URL for advertised SSE endpoint")

	// Настройки подключения к ClickHouse
	flag.StringVar(&clickhouseURL, "url", envString("localhost:9000/default", "CLICKHOUSE_MCP_URL", "CLICKHOUSE_URL"), "ClickHouse URL (format: host:port/database)")
	flag.StringVar(&username, "user", envString(app.DefaultUsername, "CLICKHOUSE_MCP_USER", "CLICKHOUSE_USER"), "ClickHouse username")
	flag.StringVar(&password, "password", envString("", "CLICKHOUSE_MCP_PASSWORD", "CLICKHOUSE_PASSWORD"), "ClickHouse password")
	flag.StringVar(&database, "db", envString("", "CLICKHOUSE_MCP_DB", "CLICKHOUSE_DB"), "ClickHouse database (overrides database in URL)")
	flag.BoolVar(&secure, "secure", envBool(false, "CLICKHOUSE_MCP_SECURE", "CLICKHOUSE_SECURE"), "Use TLS connection")
	flag.BoolVar(&insecureSkipVerify, "insecure-skip-verify", envBool(false, "CLICKHOUSE_MCP_INSECURE_SKIP_VERIFY"), "Disable TLS certificate verification")
	flag.BoolVar(&allowWrite, "allow-write", envBool(false, "CLICKHOUSE_MCP_ALLOW_WRITE"), "Allow write/admin SQL queries")
	flag.IntVar(&defaultQueryLimit, "default-query-limit", envInt(app.DefaultQueryLimit, "CLICKHOUSE_MCP_DEFAULT_LIMIT"), "Default row limit for read queries without LIMIT")
	flag.IntVar(&maxQueryLimit, "max-query-limit", envInt(app.DefaultMaxQueryLimit, "CLICKHOUSE_MCP_MAX_LIMIT"), "Maximum allowed row limit for read queries")

	flag.Parse()

	// Настраиваем текстовый логгер
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	config, err := app.ResolveConfig(app.ServerInputConfig{
		Transport:          strings.TrimSpace(transport),
		TestMode:           testMode,
		ClickhouseURL:      strings.TrimSpace(clickhouseURL),
		Username:           strings.TrimSpace(username),
		Password:           password,
		Database:           strings.TrimSpace(database),
		Secure:             secure,
		InsecureSkipVerify: insecureSkipVerify,
		Port:               port,
		PublicBaseURL:      strings.TrimSpace(publicBaseURL),
		AllowWrite:         allowWrite,
		DefaultQueryLimit:  defaultQueryLimit,
		MaxQueryLimit:      maxQueryLimit,
	})
	if err != nil {
		slog.Error("Ошибка конфигурации", "err", err)
		os.Exit(1)
	}

	// Создаем и запускаем сервер
	server, err := app.NewServer(config)
	if err != nil {
		slog.Error("Ошибка создания сервера", "err", err)
		os.Exit(1)
	}
	defer server.Close()

	if err := server.Start(); err != nil {
		slog.Error("Ошибка сервера", "err", err)
		os.Exit(1)
	}
}

func envString(fallback string, keys ...string) string {
	for _, key := range keys {
		if value, ok := os.LookupEnv(key); ok {
			return value
		}
	}
	return fallback
}

func envBool(fallback bool, keys ...string) bool {
	for _, key := range keys {
		if value, ok := os.LookupEnv(key); ok {
			parsed, err := strconv.ParseBool(strings.TrimSpace(value))
			if err == nil {
				return parsed
			}
		}
	}
	return fallback
}

func envInt(fallback int, keys ...string) int {
	for _, key := range keys {
		if value, ok := os.LookupEnv(key); ok {
			parsed, err := strconv.Atoi(strings.TrimSpace(value))
			if err == nil {
				return parsed
			}
		}
	}
	return fallback
}
