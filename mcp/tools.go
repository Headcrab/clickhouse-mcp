package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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
	logger *slog.Logger
	client clickhouse.Client
	policy clickhouse.QueryPolicy
}

// NewToolHandler создает новый экземпляр обработчика инструментов
func NewToolHandler(client clickhouse.Client, policy clickhouse.QueryPolicy, logger *slog.Logger) ToolHandler {
	if logger == nil {
		logger = slog.Default().With("module", "mcp.tools")
	}

	return &DefaultToolHandler{
		logger: logger,
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
		h.logger.Error("failed to list databases", slog.Any("err", err))
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

	h.logger.Info("listed databases", slog.Int("database_count", len(databases)))

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
		h.logger.Warn("get_tables rejected", slog.String("reason", "missing_database"))
		return mcp.NewToolResultError("Необходимо указать параметр 'database'"), nil
	}

	tables, err := h.client.GetTables(ctx, database)
	if err != nil {
		h.logger.Error("failed to list tables",
			slog.String("database", database),
			slog.Any("err", err),
		)
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

	h.logger.Info("listed tables",
		slog.String("database", database),
		slog.Int("table_count", len(tables)),
	)

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
		h.logger.Warn("get_schema rejected",
			slog.Bool("has_database", ok1),
			slog.Bool("has_table", ok2),
		)
		return mcp.NewToolResultError("Необходимо указать параметры 'database' и 'table'"), nil
	}

	columns, err := h.client.GetTableSchema(ctx, database, table)
	if err != nil {
		h.logger.Error("failed to describe table",
			slog.String("database", database),
			slog.String("table", table),
			slog.Any("err", err),
		)
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

	h.logger.Info("described table",
		slog.String("database", database),
		slog.String("table", table),
		slog.Int("column_count", len(columns)),
	)

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
		h.logger.Warn("query rejected", slog.String("reason", "missing_query"))
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
		h.logger.Warn("query rejected",
			slog.String("query_preview", previewQuery(query)),
			slog.Int("requested_limit", limit),
			slog.Bool("allow_write", h.policy.AllowWrite),
			slog.Any("err", err),
		)
		return mcp.NewToolResultError(fmt.Sprintf("Ошибка валидации запроса: %s", err)), nil
	}

	if prepared.Kind == clickhouse.QueryKindWrite {
		if err := h.client.Execute(ctx, prepared.Query); err != nil {
			h.logger.Error("failed to execute write query",
				slog.String("query_kind", string(prepared.Kind)),
				slog.String("query_preview", previewQuery(prepared.Query)),
				slog.Any("err", err),
			)
			return mcp.NewToolResultError(fmt.Sprintf("Ошибка выполнения запроса: %s", err)), nil
		}

		h.logger.Warn("write query executed",
			slog.String("query_kind", string(prepared.Kind)),
			slog.String("query_preview", previewQuery(prepared.Query)),
		)
		return mcp.NewToolResultText("Запрос выполнен, результатов нет."), nil
	}

	results, err := h.client.QueryData(ctx, prepared.Query)
	if err != nil {
		h.logger.Error("failed to execute read query",
			slog.String("query_kind", string(prepared.Kind)),
			slog.String("query_preview", previewQuery(prepared.Query)),
			slog.Int("effective_limit", prepared.EffectiveLimit),
			slog.Any("err", err),
		)
		return mcp.NewToolResultError(fmt.Sprintf("Ошибка выполнения запроса: %s", err)), nil
	}

	// Прекращаем дальнейшую обработку, если нет колонок
	if len(results.Columns) == 0 {
		h.logger.Info("query executed",
			slog.String("query_kind", string(prepared.Kind)),
			slog.String("query_preview", previewQuery(prepared.Query)),
			slog.Int("effective_limit", prepared.EffectiveLimit),
			slog.Bool("limit_applied", prepared.LimitApplied),
			slog.Int("row_count", 0),
		)
		return mcp.NewToolResultText("Запрос выполнен, результатов нет."), nil
	}

	// Преобразуем результаты для JSON
	jsonBytes, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		h.logger.Error("failed to format query result",
			slog.String("query_kind", string(prepared.Kind)),
			slog.String("query_preview", previewQuery(prepared.Query)),
			slog.Any("err", err),
		)
		return mcp.NewToolResultError(fmt.Sprintf("Ошибка форматирования результатов: %s", err)), nil
	}

	h.logger.Info("query executed",
		slog.String("query_kind", string(prepared.Kind)),
		slog.String("query_preview", previewQuery(prepared.Query)),
		slog.Int("effective_limit", prepared.EffectiveLimit),
		slog.Bool("limit_applied", prepared.LimitApplied),
		slog.Int("row_count", len(results.Rows)),
		slog.Int("column_count", len(results.Columns)),
	)

	// Возвращаем результат в текстовом виде (поскольку mcp-go не имеет метода NewToolResultJSON)
	return mcp.NewToolResultText(string(jsonBytes)), nil
}

func previewQuery(query string) string {
	normalized := strings.Join(strings.Fields(strings.TrimSpace(query)), " ")
	if len(normalized) <= 160 {
		return normalized
	}

	return normalized[:157] + "..."
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
