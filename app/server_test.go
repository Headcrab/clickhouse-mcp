package app

import "testing"

func TestParseClickhouseURL(t *testing.T) {
	tests := []struct {
		name         string
		raw          string
		expectedHost string
		expectedPort int
		expectedDB   string
		expectError  bool
	}{
		{
			name:         "localhost with explicit port and database",
			raw:          "localhost:9440/analytics",
			expectedHost: "localhost",
			expectedPort: 9440,
			expectedDB:   "analytics",
		},
		{
			name:         "full url with scheme",
			raw:          "clickhouse://example.com:8123/test_db",
			expectedHost: "example.com",
			expectedPort: 8123,
			expectedDB:   "test_db",
		},
		{
			name:         "host without port",
			raw:          "custom-host/custom-db",
			expectedHost: "custom-host",
			expectedPort: DefaultCHPort,
			expectedDB:   "custom-db",
		},
		{
			name:         "host without database",
			raw:          "clickhouse:5555",
			expectedHost: "clickhouse",
			expectedPort: 5555,
			expectedDB:   DefaultCHDatabase,
		},
		{
			name:        "empty input",
			raw:         "",
			expectError: true,
		},
		{
			name:        "invalid port",
			raw:         "localhost:abc/default",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port, db, err := ParseClickhouseURL(tt.raw)
			if (err != nil) != tt.expectError {
				t.Fatalf("ParseClickhouseURL() error = %v, expectError = %v", err, tt.expectError)
			}

			if tt.expectError {
				return
			}

			if host != tt.expectedHost {
				t.Fatalf("host = %q, want %q", host, tt.expectedHost)
			}
			if port != tt.expectedPort {
				t.Fatalf("port = %d, want %d", port, tt.expectedPort)
			}
			if db != tt.expectedDB {
				t.Fatalf("db = %q, want %q", db, tt.expectedDB)
			}
		})
	}
}

func TestResolveConfig(t *testing.T) {
	cfg, err := ResolveConfig(ServerInputConfig{
		Transport:         "sse",
		ClickhouseURL:     "localhost:9440/analytics",
		Username:          "default",
		Port:              8082,
		DefaultQueryLimit: 100,
		MaxQueryLimit:     10000,
		PublicBaseURL:     "https://mcp.example.com/root/",
	})
	if err != nil {
		t.Fatalf("ResolveConfig() error = %v", err)
	}

	if cfg.ClickhouseHost != "localhost" {
		t.Fatalf("ClickhouseHost = %q, want localhost", cfg.ClickhouseHost)
	}
	if cfg.ClickhousePort != 9440 {
		t.Fatalf("ClickhousePort = %d, want 9440", cfg.ClickhousePort)
	}
	if cfg.Database != "analytics" {
		t.Fatalf("Database = %q, want analytics", cfg.Database)
	}
	if cfg.PublicBaseURL != "https://mcp.example.com/root" {
		t.Fatalf("PublicBaseURL = %q, want trimmed url", cfg.PublicBaseURL)
	}
}

func TestResolveConfigValidation(t *testing.T) {
	tests := []struct {
		name  string
		input ServerInputConfig
	}{
		{
			name: "invalid transport",
			input: ServerInputConfig{
				Transport:         "http",
				ClickhouseURL:     "localhost:9000/default",
				Port:              8082,
				DefaultQueryLimit: 100,
				MaxQueryLimit:     10000,
			},
		},
		{
			name: "invalid public base url",
			input: ServerInputConfig{
				Transport:         "sse",
				ClickhouseURL:     "localhost:9000/default",
				Port:              8082,
				DefaultQueryLimit: 100,
				MaxQueryLimit:     10000,
				PublicBaseURL:     "localhost:8082",
			},
		},
		{
			name: "insecure without secure",
			input: ServerInputConfig{
				Transport:          "stdio",
				ClickhouseURL:      "localhost:9000/default",
				Port:               8082,
				DefaultQueryLimit:  100,
				MaxQueryLimit:      10000,
				InsecureSkipVerify: true,
			},
		},
		{
			name: "default limit exceeds max",
			input: ServerInputConfig{
				Transport:         "stdio",
				ClickhouseURL:     "localhost:9000/default",
				Port:              8082,
				DefaultQueryLimit: 10001,
				MaxQueryLimit:     10000,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ResolveConfig(tt.input); err == nil {
				t.Fatalf("ResolveConfig() expected error")
			}
		})
	}
}

func TestSSEBaseURL(t *testing.T) {
	cfg := ServerConfig{
		Port: 8082,
	}
	if got := cfg.SSEBaseURL(); got != "http://127.0.0.1:8082" {
		t.Fatalf("SSEBaseURL() = %q", got)
	}

	cfg.PublicBaseURL = "https://mcp.example.com/base"
	if got := cfg.SSEBaseURL(); got != "https://mcp.example.com/base" {
		t.Fatalf("SSEBaseURL() with public URL = %q", got)
	}
}
