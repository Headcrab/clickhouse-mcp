package mcp

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"clickhouse-mcp/clickhouse"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockClickhouseClient - мок для клиента ClickHouse
type MockClickhouseClient struct {
	mock.Mock
}

// GetDatabases - мок метод
func (m *MockClickhouseClient) GetDatabases(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	return args.Get(0).([]string), args.Error(1)
}

// GetTables - мок метод
func (m *MockClickhouseClient) GetTables(ctx context.Context, database string) ([]string, error) {
	args := m.Called(ctx, database)
	return args.Get(0).([]string), args.Error(1)
}

// GetTableSchema - мок метод
func (m *MockClickhouseClient) GetTableSchema(ctx context.Context, database, table string) ([]clickhouse.ColumnInfo, error) {
	args := m.Called(ctx, database, table)
	return args.Get(0).([]clickhouse.ColumnInfo), args.Error(1)
}

// QueryData - мок метод
func (m *MockClickhouseClient) QueryData(ctx context.Context, query string) (clickhouse.QueryResult, error) {
	args := m.Called(ctx, query)
	return args.Get(0).(clickhouse.QueryResult), args.Error(1)
}

// Execute - мок метод
func (m *MockClickhouseClient) Execute(ctx context.Context, query string) error {
	args := m.Called(ctx, query)
	return args.Error(0)
}

// GetConnection - мок метод
func (m *MockClickhouseClient) GetConnection() driver.Conn {
	args := m.Called()
	return args.Get(0).(driver.Conn)
}

// Close - мок метод
func (m *MockClickhouseClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

// getText - вспомогательная функция для извлечения текста из результата инструмента
func getText(result *mcp.CallToolResult) string {
	if len(result.Content) == 0 {
		return ""
	}

	for _, content := range result.Content {
		if textContent, ok := mcp.AsTextContent(content); ok {
			return textContent.Text
		}
	}

	return ""
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestHandleGetDatabasesTool(t *testing.T) {
	// Создаем мок клиента
	mockClient := new(MockClickhouseClient)

	// Устанавливаем ожидаемое поведение
	mockClient.On("GetDatabases", mock.Anything).Return([]string{"db1", "db2", "db3"}, nil)

	// Создаем тестируемый обработчик
	handler := NewToolHandler(mockClient, clickhouse.QueryPolicy{DefaultLimit: 100, MaxLimit: 10000}, testLogger())

	// Создаем тестовый запрос
	request := mcp.CallToolRequest{}
	request.Params.Name = "get_databases"
	request.Params.Arguments = map[string]interface{}{}

	// Вызываем тестируемый метод
	result, err := handler.HandleGetDatabasesTool(context.Background(), request)

	// Проверяем результат
	assert.NoError(t, err)
	assert.NotNil(t, result)
	text := getText(result)
	assert.Contains(t, text, "db1")
	assert.Contains(t, text, "db2")
	assert.Contains(t, text, "db3")

	// Проверяем, что все ожидаемые методы были вызваны
	mockClient.AssertExpectations(t)
}

func TestHandleGetTablesTool(t *testing.T) {
	// Создаем мок клиента
	mockClient := new(MockClickhouseClient)

	// Устанавливаем ожидаемое поведение
	mockClient.On("GetTables", mock.Anything, "test_db").Return([]string{"table1", "table2"}, nil)

	// Создаем тестируемый обработчик
	handler := NewToolHandler(mockClient, clickhouse.QueryPolicy{DefaultLimit: 100, MaxLimit: 10000}, testLogger())

	// Тест 1: корректный запрос
	t.Run("Valid Request", func(t *testing.T) {
		// Создаем тестовый запрос
		request := mcp.CallToolRequest{}
		request.Params.Name = "get_tables"
		request.Params.Arguments = map[string]interface{}{
			"database": "test_db",
		}

		// Вызываем тестируемый метод
		result, err := handler.HandleGetTablesTool(context.Background(), request)

		// Проверяем результат
		assert.NoError(t, err)
		assert.NotNil(t, result)
		text := getText(result)
		assert.Contains(t, text, "table1")
		assert.Contains(t, text, "table2")
	})

	// Тест 2: отсутствует обязательный параметр
	t.Run("Missing Required Parameter", func(t *testing.T) {
		// Создаем тестовый запрос без обязательного параметра
		request := mcp.CallToolRequest{}
		request.Params.Name = "get_tables"
		request.Params.Arguments = map[string]interface{}{}

		// Вызываем тестируемый метод
		result, err := handler.HandleGetTablesTool(context.Background(), request)

		// Проверяем результат
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.IsError)
		text := getText(result)
		assert.Contains(t, text, "Необходимо указать параметр 'database'")
	})

	// Проверяем, что все ожидаемые методы были вызваны
	mockClient.AssertExpectations(t)
}

func TestHandleGetTableSchemaTool(t *testing.T) {
	// Создаем мок клиента
	mockClient := new(MockClickhouseClient)

	// Устанавливаем ожидаемое поведение
	mockClient.On("GetTableSchema", mock.Anything, "test_db", "test_table").Return([]clickhouse.ColumnInfo{
		{Name: "id", Type: "UInt32", Position: 1},
		{Name: "name", Type: "String", Position: 2},
		{Name: "created_at", Type: "DateTime", Position: 3},
	}, nil)

	// Создаем тестируемый обработчик
	handler := NewToolHandler(mockClient, clickhouse.QueryPolicy{DefaultLimit: 100, MaxLimit: 10000}, testLogger())

	// Тест 1: корректный запрос
	t.Run("Valid Request", func(t *testing.T) {
		// Создаем тестовый запрос
		request := mcp.CallToolRequest{}
		request.Params.Name = "get_schema"
		request.Params.Arguments = map[string]interface{}{
			"database": "test_db",
			"table":    "test_table",
		}

		// Вызываем тестируемый метод
		result, err := handler.HandleGetTableSchemaTool(context.Background(), request)

		// Проверяем результат
		assert.NoError(t, err)
		assert.NotNil(t, result)
		text := getText(result)
		assert.Contains(t, text, "id")
		assert.Contains(t, text, "UInt32")
		assert.Contains(t, text, "name")
		assert.Contains(t, text, "String")
	})

	// Тест 2: отсутствуют обязательные параметры
	t.Run("Missing Required Parameters", func(t *testing.T) {
		// Создаем тестовый запрос без обязательных параметров
		request := mcp.CallToolRequest{}
		request.Params.Name = "get_schema"
		request.Params.Arguments = map[string]interface{}{}

		// Вызываем тестируемый метод
		result, err := handler.HandleGetTableSchemaTool(context.Background(), request)

		// Проверяем результат
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.IsError)
		text := getText(result)
		assert.Contains(t, text, "Необходимо указать параметры 'database' и 'table'")
	})

	// Проверяем, что все ожидаемые методы были вызваны
	mockClient.AssertExpectations(t)
}

func TestHandleQueryTool(t *testing.T) {
	// Создаем мок клиента
	mockClient := new(MockClickhouseClient)

	// Устанавливаем ожидаемое поведение для запроса
	mockClient.On("QueryData", mock.Anything, "SELECT 1 as test LIMIT 10").Return(clickhouse.QueryResult{
		Columns: []clickhouse.ColumnInfo{
			{Name: "test", Type: "UInt8", Position: 1},
		},
		Rows: []map[string]interface{}{
			{"test": uint8(1)},
		},
	}, nil)

	// Создаем тестируемый обработчик
	handler := NewToolHandler(mockClient, clickhouse.QueryPolicy{DefaultLimit: 100, MaxLimit: 10000}, testLogger())

	// Тест 1: корректный запрос с указанным лимитом
	t.Run("Valid Request With Limit", func(t *testing.T) {
		// Создаем тестовый запрос
		request := mcp.CallToolRequest{}
		request.Params.Name = "query"
		request.Params.Arguments = map[string]interface{}{
			"query": "SELECT 1 as test",
			"limit": float64(10), // JSON числа всегда конвертируются в float64
		}

		// Вызываем тестируемый метод
		result, err := handler.HandleQueryTool(context.Background(), request)

		// Проверяем результат
		assert.NoError(t, err)
		assert.NotNil(t, result)
		text := getText(result)
		assert.Contains(t, text, "test")
		assert.Contains(t, text, "UInt8")
	})

	// Тест 2: отсутствует обязательный параметр query
	t.Run("Missing Required Parameter", func(t *testing.T) {
		// Создаем тестовый запрос без обязательного параметра
		request := mcp.CallToolRequest{}
		request.Params.Name = "query"
		request.Params.Arguments = map[string]interface{}{
			"limit": float64(10),
		}

		// Вызываем тестируемый метод
		result, err := handler.HandleQueryTool(context.Background(), request)

		// Проверяем результат
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.IsError)
		text := getText(result)
		assert.Contains(t, text, "Необходимо указать параметр 'query'")
	})

	// Проверяем, что все ожидаемые методы были вызваны
	mockClient.AssertExpectations(t)
}

func TestHandleQueryToolRejectsWriteInReadOnlyMode(t *testing.T) {
	mockClient := new(MockClickhouseClient)
	handler := NewToolHandler(mockClient, clickhouse.QueryPolicy{DefaultLimit: 100, MaxLimit: 10000}, testLogger())

	request := mcp.CallToolRequest{}
	request.Params.Name = "query"
	request.Params.Arguments = map[string]interface{}{
		"query": "INSERT INTO test VALUES (1)",
	}

	result, err := handler.HandleQueryTool(context.Background(), request)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.IsError)
	assert.Contains(t, getText(result), "Ошибка валидации запроса")
	mockClient.AssertNotCalled(t, "QueryData", mock.Anything, mock.Anything)
}

func TestHandleQueryToolExecutesWriteInWriteMode(t *testing.T) {
	mockClient := new(MockClickhouseClient)
	mockClient.On("Execute", mock.Anything, "INSERT INTO test VALUES (1)").Return(nil)

	handler := NewToolHandler(mockClient, clickhouse.QueryPolicy{
		AllowWrite:   true,
		DefaultLimit: 100,
		MaxLimit:     10000,
	}, testLogger())

	request := mcp.CallToolRequest{}
	request.Params.Name = "query"
	request.Params.Arguments = map[string]interface{}{
		"query": "INSERT INTO test VALUES (1)",
	}

	result, err := handler.HandleQueryTool(context.Background(), request)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, result.IsError)
	assert.Contains(t, getText(result), "Запрос выполнен, результатов нет.")
	mockClient.AssertNotCalled(t, "QueryData", mock.Anything, mock.Anything)
	mockClient.AssertExpectations(t)
}
