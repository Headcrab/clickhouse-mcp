package app

import (
	"fmt"
	"net/url"
	"strings"

	"clickhouse-mcp/clickhouse"
)

const (
	DefaultTransport     = "stdio"
	DefaultSSEPort       = 8082
	DefaultCHPort        = 9000
	DefaultCHDatabase    = "default"
	DefaultUsername      = "default"
	DefaultQueryLimit    = 100
	DefaultMaxQueryLimit = 10000
)

// ServerInputConfig содержит сырые входные параметры из флагов и env.
type ServerInputConfig struct {
	Transport          string
	TestMode           bool
	ClickhouseURL      string
	Username           string
	Password           string
	Database           string
	Secure             bool
	InsecureSkipVerify bool
	Port               int
	PublicBaseURL      string
	AllowWrite         bool
	DefaultQueryLimit  int
	MaxQueryLimit      int
}

// ServerConfig содержит валидированную конфигурацию сервера.
type ServerConfig struct {
	Transport          string
	TestMode           bool
	ClickhouseHost     string
	ClickhousePort     int
	Username           string
	Password           string
	Database           string
	Secure             bool
	InsecureSkipVerify bool
	Port               int
	PublicBaseURL      string
	AllowWrite         bool
	DefaultQueryLimit  int
	MaxQueryLimit      int
}

// ParseClickhouseURL разбирает адрес подключения к ClickHouse.
func ParseClickhouseURL(raw string) (string, int, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", 0, "", fmt.Errorf("URL ClickHouse не указан")
	}

	if !strings.Contains(raw, "://") {
		raw = "clickhouse://" + raw
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return "", 0, "", fmt.Errorf("неверный формат URL ClickHouse: %w", err)
	}

	host := parsed.Hostname()
	if host == "" {
		return "", 0, "", fmt.Errorf("неверный формат URL ClickHouse: отсутствует хост")
	}

	port := DefaultCHPort
	if parsed.Port() != "" {
		fmtPort := 0
		if _, err := fmt.Sscanf(parsed.Port(), "%d", &fmtPort); err != nil || fmtPort <= 0 || fmtPort > 65535 {
			return "", 0, "", fmt.Errorf("неверный порт ClickHouse: %q", parsed.Port())
		}
		port = fmtPort
	}

	database := strings.Trim(strings.TrimSpace(parsed.Path), "/")
	if database == "" {
		database = DefaultCHDatabase
	}

	if strings.Contains(database, "/") {
		return "", 0, "", fmt.Errorf("неверный формат URL ClickHouse: база данных должна быть одной частью пути")
	}

	return host, port, database, nil
}

// ResolveConfig собирает и валидирует итоговую конфигурацию сервера.
func ResolveConfig(input ServerInputConfig) (ServerConfig, error) {
	host, port, database, err := ParseClickhouseURL(input.ClickhouseURL)
	if err != nil {
		return ServerConfig{}, err
	}

	if input.Database != "" {
		database = input.Database
	}

	cfg := ServerConfig{
		Transport:          strings.ToLower(strings.TrimSpace(input.Transport)),
		TestMode:           input.TestMode,
		ClickhouseHost:     host,
		ClickhousePort:     port,
		Username:           strings.TrimSpace(input.Username),
		Password:           input.Password,
		Database:           database,
		Secure:             input.Secure,
		InsecureSkipVerify: input.InsecureSkipVerify,
		Port:               input.Port,
		PublicBaseURL:      strings.TrimSpace(input.PublicBaseURL),
		AllowWrite:         input.AllowWrite,
		DefaultQueryLimit:  input.DefaultQueryLimit,
		MaxQueryLimit:      input.MaxQueryLimit,
	}

	if cfg.Username == "" {
		cfg.Username = DefaultUsername
	}

	if cfg.Transport == "" {
		cfg.Transport = DefaultTransport
	}

	if err := cfg.Validate(); err != nil {
		return ServerConfig{}, err
	}

	if cfg.PublicBaseURL != "" {
		cfg.PublicBaseURL = strings.TrimRight(cfg.PublicBaseURL, "/")
	}

	return cfg, nil
}

// Validate проверяет согласованность конфигурации.
func (c ServerConfig) Validate() error {
	switch c.Transport {
	case "stdio", "sse":
	default:
		return fmt.Errorf("неподдерживаемый transport %q: используйте stdio или sse", c.Transport)
	}

	if c.ClickhouseHost == "" {
		return fmt.Errorf("хост ClickHouse не указан")
	}

	if c.ClickhousePort <= 0 || c.ClickhousePort > 65535 {
		return fmt.Errorf("порт ClickHouse должен быть в диапазоне 1..65535")
	}

	if c.Database == "" {
		return fmt.Errorf("база данных ClickHouse не указана")
	}

	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("порт SSE должен быть в диапазоне 1..65535")
	}

	if c.DefaultQueryLimit <= 0 {
		return fmt.Errorf("default query limit должен быть больше 0")
	}

	if c.MaxQueryLimit <= 0 {
		return fmt.Errorf("max query limit должен быть больше 0")
	}

	if c.DefaultQueryLimit > c.MaxQueryLimit {
		return fmt.Errorf("default query limit не может быть больше max query limit")
	}

	if c.InsecureSkipVerify && !c.Secure {
		return fmt.Errorf("insecure-skip-verify можно использовать только вместе с secure=true")
	}

	if c.PublicBaseURL != "" {
		parsed, err := url.Parse(c.PublicBaseURL)
		if err != nil {
			return fmt.Errorf("public-base-url имеет неверный формат: %w", err)
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return fmt.Errorf("public-base-url должен начинаться с http:// или https://")
		}
		if parsed.Host == "" {
			return fmt.Errorf("public-base-url должен содержать хост")
		}
	}

	return nil
}

// ClickHouseConfig возвращает конфиг клиента ClickHouse.
func (c ServerConfig) ClickHouseConfig() clickhouse.Config {
	return clickhouse.Config{
		Host:               c.ClickhouseHost,
		Port:               c.ClickhousePort,
		Database:           c.Database,
		Username:           c.Username,
		Password:           c.Password,
		Secure:             c.Secure,
		InsecureSkipVerify: c.InsecureSkipVerify,
	}
}

// QueryPolicy возвращает политику выполнения SQL запросов.
func (c ServerConfig) QueryPolicy() clickhouse.QueryPolicy {
	return clickhouse.QueryPolicy{
		AllowWrite:   c.AllowWrite,
		DefaultLimit: c.DefaultQueryLimit,
		MaxLimit:     c.MaxQueryLimit,
	}
}

// SSEBaseURL возвращает base URL для advertised SSE endpoint.
func (c ServerConfig) SSEBaseURL() string {
	if c.PublicBaseURL != "" {
		return c.PublicBaseURL
	}

	return fmt.Sprintf("http://127.0.0.1:%d", c.Port)
}
