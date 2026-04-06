package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"clickhouse-mcp/clickhouse"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// ToolHandler определяет интерфейс для обработчика инструментов MCP
type ToolHandler interface {
	// HandleGetDatabasesTool обрабатывает запрос на получение списка баз данных
	HandleGetDatabasesTool(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error)

	// HandleGetTablesTool обрабатывает запрос на получение списка таблиц
	HandleGetTablesTool(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error)

	// HandleGetTableSchemaTool обрабатывает запрос на получение схемы таблицы
	HandleGetTableSchemaTool(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error)

	// HandleQueryTool обрабатывает запрос на выполнение SQL запроса
	HandleQueryTool(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error)
}

// DefaultToolHandler - стандартная реализация обработчика инструментов
type DefaultToolHandler struct {
	client clickhouse.Client
	policy clickhouse.QueryPolicy
}

// NewToolHandler создает новый экземпляр обработчика инструментов
func NewToolHandler(client clickhouse.Client, policy clickhouse.QueryPolicy) ToolHandler {
	return &DefaultToolHandler{
		client: client,
		policy: policy,
	}
}

// HandleGetDatabasesTool обрабатывает запрос на получение списка баз данных
func (h *DefaultToolHandler) HandleGetDatabasesTool(
	ctx context.Context,
	request mcp.CallToolRequest,
) (*mcp.CallToolResult, error) {
	databases, err := h.client.GetDatabases(ctx)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Ошибка получения баз данных: %s", err)), nil
	}

	// Форматируем результат в текстовый вид
	result := "Базы данных в ClickHouse:\n\n"
	if len(databases) == 0 {
		result += "Базы данных не найдены."
		return mcp.NewToolResultText(result), nil
	}
	for i, db := range databases {
		result += fmt.Sprintf("%d. %s\n", i+1, db)
	}

	// Возвращаем результат
	return mcp.NewToolResultText(result), nil
}

// HandleGetTablesTool обрабатывает запрос на получение списка таблиц
func (h *DefaultToolHandler) HandleGetTablesTool(
	ctx context.Context,
	request mcp.CallToolRequest,
) (*mcp.CallToolResult, error) {
	arguments := request.Params.Arguments
	database, ok := arguments["database"].(string)
	if !ok {
		return mcp.NewToolResultError("Необходимо указать параметр 'database'"), nil
	}

	tables, err := h.client.GetTables(ctx, database)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Ошибка получения таблиц: %s", err)), nil
	}

	// Форматируем результат в текстовый вид
	result := fmt.Sprintf("Таблицы в базе данных '%s':\n\n", database)
	if len(tables) == 0 {
		result += "Таблицы не найдены."
	} else {
		for i, table := range tables {
			result += fmt.Sprintf("%d. %s\n", i+1, table)
		}
	}

	// Возвращаем результат
	return mcp.NewToolResultText(result), nil
}

// HandleGetTableSchemaTool обрабатывает запрос на получение схемы таблицы
func (h *DefaultToolHandler) HandleGetTableSchemaTool(
	ctx context.Context,
	request mcp.CallToolRequest,
) (*mcp.CallToolResult, error) {
	arguments := request.Params.Arguments
	database, ok1 := arguments["database"].(string)
	table, ok2 := arguments["table"].(string)

	if !ok1 || !ok2 {
		return mcp.NewToolResultError("Необходимо указать параметры 'database' и 'table'"), nil
	}

	columns, err := h.client.GetTableSchema(ctx, database, table)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Ошибка получения схемы таблицы: %s", err)), nil
	}

	// Форматируем результат в текстовый вид
	result := fmt.Sprintf("Схема таблицы '%s.%s':\n\n", database, table)
	if len(columns) == 0 {
		result += "Колонки не найдены."
	} else {
		result += fmt.Sprintf("%-20s | %-30s | %s\n", "КОЛОНКА", "ТИП", "ПОЗИЦИЯ")
		result += strings.Repeat("-", 70) + "\n"
		for _, col := range columns {
			result += fmt.Sprintf("%-20s | %-30s | %d\n", col.Name, col.Type, col.Position)
		}
	}

	// Возвращаем результат
	return mcp.NewToolResultText(result), nil
}

// HandleQueryTool обрабатывает запрос на выполнение SQL запроса
func (h *DefaultToolHandler) HandleQueryTool(
	ctx context.Context,
	request mcp.CallToolRequest,
) (*mcp.CallToolResult, error) {
	arguments := request.Params.Arguments
	query, ok1 := arguments["query"].(string)
	limit := 100 // Значение по умолчанию

	if !ok1 {
		return mcp.NewToolResultError("Необходимо указать параметр 'query'"), nil
	}

	// Извлекаем лимит, если он задан
	switch limitVal := arguments["limit"].(type) {
	case float64:
		limit = int(limitVal)
	case int:
		limit = limitVal
	}

	prepared, err := clickhouse.PrepareQuery(query, limit, h.policy)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Ошибка валидации запроса: %s", err)), nil
	}

	if prepared.Kind == clickhouse.QueryKindWrite {
		if err := h.client.Execute(ctx, prepared.Query); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Ошибка выполнения запроса: %s", err)), nil
		}
		return mcp.NewToolResultText("Запрос выполнен, результатов нет."), nil
	}

	results, err := h.client.QueryData(ctx, prepared.Query)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Ошибка выполнения запроса: %s", err)), nil
	}

	// Прекращаем дальнейшую обработку, если нет колонок
	if len(results.Columns) == 0 {
		return mcp.NewToolResultText("Запрос выполнен, результатов нет."), nil
	}

	// Преобразуем результаты для JSON
	jsonBytes, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Ошибка форматирования результатов: %s", err)), nil
	}

	// Возвращаем результат в текстовом виде (поскольку mcp-go не имеет метода NewToolResultJSON)
	return mcp.NewToolResultText(string(jsonBytes)), nil
}

// RegisterTools регистрирует инструменты MCP
func RegisterTools(mcpServer *server.MCPServer, handler ToolHandler) {
	// Инструмент для получения списка баз данных
	mcpServer.AddTool(mcp.NewTool("get_databases",
		mcp.WithDescription("Получить список баз данных в ClickHouse"),
	), handler.HandleGetDatabasesTool)

	// Инструмент для получения списка таблиц
	mcpServer.AddTool(mcp.NewTool("get_tables",
		mcp.WithDescription("Получить список таблиц в выбранной базе данных"),
		mcp.WithString("database",
			mcp.Description("Имя базы данных"),
			mcp.Required(),
		),
	), handler.HandleGetTablesTool)

	// Инструмент для получения схемы таблицы
	mcpServer.AddTool(mcp.NewTool("get_schema",
		mcp.WithDescription("Получить схему выбранной таблицы"),
		mcp.WithString("database",
			mcp.Description("Имя базы данных"),
			mcp.Required(),
		),
		mcp.WithString("table",
			mcp.Description("Имя таблицы"),
			mcp.Required(),
		),
	), handler.HandleGetTableSchemaTool)

	// Инструмент для выполнения SQL запроса
	mcpServer.AddTool(mcp.NewTool("query",
		mcp.WithDescription("Выполнить SQL запрос в ClickHouse"),
		mcp.WithString("query",
			mcp.Description("SQL запрос для выполнения"),
			mcp.Required(),
		),
		mcp.WithNumber("limit",
			mcp.Description("Максимальное количество возвращаемых строк (по умолчанию 100)"),
		),
	), handler.HandleQueryTool)
}
