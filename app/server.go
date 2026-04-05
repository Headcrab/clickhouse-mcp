package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"clickhouse-mcp/clickhouse"
	"clickhouse-mcp/mcp"

	"github.com/mark3labs/mcp-go/server"
)

// Server инкапсулирует логику запуска и настройки MCP сервера
type Server struct {
	config     ServerConfig
	mcpServer  *server.MCPServer
	tools      mcp.ToolHandler
	chClient   clickhouse.Client
	httpServer *http.Server
}

// NewServer создает новый экземпляр сервера
func NewServer(config ServerConfig) (*Server, error) {
	// Настраиваем логгер
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("ошибка конфигурации: %w", err)
	}

	// Создаем сервер
	server := &Server{
		config: config,
	}

	// В тестовом режиме не нужно подключаться к ClickHouse
	if config.TestMode {
		// Создаем MCP сервер
		server.mcpServer = server.createMCPServer()
		return server, nil
	}

	// Подключаемся к ClickHouse
	if err := server.connectToClickhouse(); err != nil {
		return nil, err
	}

	// Создаем обработчик инструментов
	server.tools = mcp.NewToolHandler(server.chClient, config.QueryPolicy())

	// Создаем MCP сервер
	server.mcpServer = server.createMCPServer()

	// Регистрируем инструменты
	mcp.RegisterTools(server.mcpServer, server.tools)

	return server, nil
}

// connectToClickhouse устанавливает соединение с ClickHouse
func (s *Server) connectToClickhouse() error {
	slog.Info("Подключение к ClickHouse",
		"host", s.config.ClickhouseHost,
		"port", s.config.ClickhousePort,
		"database", s.config.Database,
		"secure", s.config.Secure,
	)

	client, err := clickhouse.NewClient(s.config.ClickHouseConfig())
	if err != nil {
		return fmt.Errorf("ошибка подключения к ClickHouse: %w", err)
	}

	s.chClient = client
	return nil
}

// createMCPServer создает и настраивает MCP сервер
func (s *Server) createMCPServer() *server.MCPServer {
	return server.NewMCPServer(
		"clickhouse-client",  // имя сервера
		"1.0.0",              // версия
		server.WithLogging(), // включаем логирование
	)
}

// RunTests запускает тестовые примеры
func (s *Server) RunTests() {
	slog.Info("Запуск тестовых примеров")

	fmt.Println("=== Пример запроса для получения списка баз данных ===")
	fmt.Println(`{"jsonrpc":"2.0","id":"test","method":"tools/call","params":{"name":"get_databases","arguments":{}}}`)

	fmt.Println("\n=== Пример запроса для получения списка таблиц ===")
	fmt.Println(`{"jsonrpc":"2.0","id":"test","method":"tools/call","params":{"name":"get_tables","arguments":{"database":"default"}}}`)

	fmt.Println("\n=== Пример запроса для получения схемы таблицы ===")
	fmt.Println(`{"jsonrpc":"2.0","id":"test","method":"tools/call","params":{"name":"get_schema","arguments":{"database":"default","table":"some_table"}}}`)

	fmt.Println("\n=== Пример запроса для выполнения SQL запроса ===")
	fmt.Println(`{"jsonrpc":"2.0","id":"test","method":"tools/call","params":{"name":"query","arguments":{"query":"SELECT 1 as test","limit":10}}}`)

	fmt.Println("\nЗапустите сервер без флага -test и отправьте запросы через клиент MCP")
}

// Start запускает сервер
func (s *Server) Start() error {
	if s.config.TestMode {
		s.RunTests()
		return nil
	}

	if s.config.Transport == "sse" {
		addr := fmt.Sprintf(":%d", s.config.Port)
		baseURL := s.config.SSEBaseURL()
		sseServer := server.NewSSEServer(s.mcpServer, server.WithBaseURL(baseURL))
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})
		mux.Handle("/", sseServer)

		s.httpServer = &http.Server{
			Addr:    addr,
			Handler: mux,
		}

		if s.config.PublicBaseURL == "" {
			slog.Warn("public-base-url не задан: advertised SSE endpoint подходит только для локального доступа",
				"base_url", baseURL,
			)
		}

		slog.Info("SSE сервер запущен",
			"listen_address", addr,
			"base_url", baseURL,
			"healthcheck", fmt.Sprintf("http://127.0.0.1:%d/healthz", s.config.Port),
		)
		if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("ошибка запуска SSE сервера: %w", err)
		}
	} else {
		slog.Info("Запуск ClickHouse MCP сервера через stdio")
		if err := server.ServeStdio(s.mcpServer); err != nil {
			return fmt.Errorf("ошибка запуска stdio сервера: %w", err)
		}
	}

	return nil
}

// Close закрывает соединения
func (s *Server) Close() error {
	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(context.Background()); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	if s.chClient != nil {
		return s.chClient.Close()
	}
	return nil
}
