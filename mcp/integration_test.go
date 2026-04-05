package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	ch "clickhouse-mcp/clickhouse"

	mcpsdk "github.com/mark3labs/mcp-go/mcp"
)

func TestIntegrationQueryAndMetadata(t *testing.T) {
	if os.Getenv("CLICKHOUSE_MCP_INTEGRATION") != "1" {
		t.Skip("integration tests are disabled")
	}

	client := mustIntegrationClient(t)
	defer client.Close()

	tableName := "mcp_integration_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	ctx := context.Background()

	conn := client.GetConnection()
	if err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	defer func() {
		_ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+tableName)
	}()

	if err := conn.Exec(ctx, "CREATE TABLE "+tableName+" (number UInt64, title String) ENGINE = Memory"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := conn.Exec(ctx, "INSERT INTO "+tableName+" VALUES (1, 'one'), (2, 'two')"); err != nil {
		t.Fatalf("seed data: %v", err)
	}

	readOnlyHandler := NewToolHandler(client, ch.QueryPolicy{
		AllowWrite:   false,
		DefaultLimit: 1,
		MaxLimit:     100,
	})

	databasesResult, err := readOnlyHandler.HandleGetDatabasesTool(ctx, mcpsdk.CallToolRequest{})
	if err != nil {
		t.Fatalf("HandleGetDatabasesTool() error = %v", err)
	}
	if databasesResult.IsError || getText(databasesResult) == "" {
		t.Fatalf("expected databases list, got %+v", databasesResult)
	}

	tablesRequest := mcpsdk.CallToolRequest{}
	tablesRequest.Params.Arguments = map[string]any{"database": "default"}
	tablesResult, err := readOnlyHandler.HandleGetTablesTool(ctx, tablesRequest)
	if err != nil {
		t.Fatalf("HandleGetTablesTool() error = %v", err)
	}
	if tablesResult.IsError || !containsText(getText(tablesResult), tableName) {
		t.Fatalf("expected table %q in result: %s", tableName, getText(tablesResult))
	}

	schemaRequest := mcpsdk.CallToolRequest{}
	schemaRequest.Params.Arguments = map[string]any{
		"database": "default",
		"table":    tableName,
	}
	schemaResult, err := readOnlyHandler.HandleGetTableSchemaTool(ctx, schemaRequest)
	if err != nil {
		t.Fatalf("HandleGetTableSchemaTool() error = %v", err)
	}
	if schemaResult.IsError || !containsText(getText(schemaResult), "number") || !containsText(getText(schemaResult), "title") {
		t.Fatalf("unexpected schema result: %s", getText(schemaResult))
	}

	queryRequest := mcpsdk.CallToolRequest{}
	queryRequest.Params.Arguments = map[string]any{
		"query": "SELECT number, title FROM default." + tableName + " ORDER BY number",
	}
	queryResult, err := readOnlyHandler.HandleQueryTool(ctx, queryRequest)
	if err != nil {
		t.Fatalf("HandleQueryTool() error = %v", err)
	}
	if queryResult.IsError {
		t.Fatalf("expected successful query, got: %s", getText(queryResult))
	}

	var parsed ch.QueryResult
	if err := json.Unmarshal([]byte(getText(queryResult)), &parsed); err != nil {
		t.Fatalf("unmarshal query result: %v", err)
	}
	if len(parsed.Rows) != 1 {
		t.Fatalf("expected default limit to return 1 row, got %d", len(parsed.Rows))
	}

	insertRequest := mcpsdk.CallToolRequest{}
	insertRequest.Params.Arguments = map[string]any{
		"query": "INSERT INTO default." + tableName + " VALUES (3, 'three')",
	}
	insertBlocked, err := readOnlyHandler.HandleQueryTool(ctx, insertRequest)
	if err != nil {
		t.Fatalf("HandleQueryTool() readonly insert error = %v", err)
	}
	if !insertBlocked.IsError {
		t.Fatalf("expected readonly insert to fail")
	}

	writeHandler := NewToolHandler(client, ch.QueryPolicy{
		AllowWrite:   true,
		DefaultLimit: 100,
		MaxLimit:     100,
	})
	insertAllowed, err := writeHandler.HandleQueryTool(ctx, insertRequest)
	if err != nil {
		t.Fatalf("HandleQueryTool() write insert error = %v", err)
	}
	if insertAllowed.IsError {
		t.Fatalf("expected write-enabled insert to succeed: %s", getText(insertAllowed))
	}
}

func mustIntegrationClient(t *testing.T) ch.Client {
	t.Helper()

	url := envOr("CLICKHOUSE_MCP_TEST_URL", envOr("CLICKHOUSE_MCP_URL", "localhost:9000/default"))
	user := envOr(
		"CLICKHOUSE_MCP_TEST_USER",
		envOr("CLICKHOUSE_MCP_USER", envOr("CLICKHOUSE_USER", "mcp")),
	)
	password := envOr(
		"CLICKHOUSE_MCP_TEST_PASSWORD",
		envOr("CLICKHOUSE_MCP_PASSWORD", envOr("CLICKHOUSE_PASSWORD", "clickhouse")),
	)
	secure := envBoolOr("CLICKHOUSE_MCP_TEST_SECURE", false)
	insecureSkipVerify := envBoolOr("CLICKHOUSE_MCP_TEST_INSECURE_SKIP_VERIFY", false)

	host, port, database, err := parseClickHouseURL(url)
	if err != nil {
		t.Fatalf("invalid test url: %v", err)
	}

	client, err := ch.NewClient(ch.Config{
		Host:               host,
		Port:               port,
		Database:           database,
		Username:           user,
		Password:           password,
		Secure:             secure,
		InsecureSkipVerify: insecureSkipVerify,
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	return client
}

func envOr(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func envBoolOr(key string, fallback bool) bool {
	if value, ok := os.LookupEnv(key); ok {
		parsed, err := strconv.ParseBool(value)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func parseClickHouseURL(raw string) (string, int, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", 0, "", fmt.Errorf("empty url")
	}
	if !strings.Contains(raw, "://") {
		raw = "clickhouse://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", 0, "", err
	}

	port := 9000
	if parsed.Port() != "" {
		port, err = strconv.Atoi(parsed.Port())
		if err != nil {
			return "", 0, "", err
		}
	}

	database := strings.Trim(strings.TrimSpace(parsed.Path), "/")
	if database == "" {
		database = "default"
	}

	return parsed.Hostname(), port, database, nil
}

func containsText(text, needle string) bool {
	return strings.Contains(text, needle)
}
