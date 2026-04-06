package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	driverapi "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Client определяет интерфейс для работы с ClickHouse.
type Client interface {
	GetDatabases(ctx context.Context) ([]string, error)
	GetTables(ctx context.Context, database string) ([]string, error)
	GetTableSchema(ctx context.Context, database, table string) ([]ColumnInfo, error)
	QueryData(ctx context.Context, query string) (QueryResult, error)
	Execute(ctx context.Context, query string) error
	GetConnection() driver.Conn
	Close() error
}

// ColumnInfo содержит информацию о колонке таблицы.
type ColumnInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Position int    `json:"position"`
	IsArray  bool   `json:"is_array,omitempty"`
	IsNested bool   `json:"is_nested,omitempty"`
}

// QueryResult содержит результат выполнения запроса.
type QueryResult struct {
	Columns []ColumnInfo     `json:"columns"`
	Rows    []map[string]any `json:"rows"`
}

// QueryPolicy задает продуктовые правила выполнения SQL.
type QueryPolicy struct {
	AllowWrite   bool
	DefaultLimit int
	MaxLimit     int
}

// PreparedQuery содержит нормализованный SQL и сопутствующую метаинформацию.
type PreparedQuery struct {
	Query          string
	Kind           QueryKind
	LimitApplied   bool
	EffectiveLimit int
}

// QueryKind описывает тип SQL-запроса с точки зрения продуктовой политики.
type QueryKind string

const (
	QueryKindUnknown QueryKind = "unknown"
	QueryKindRead    QueryKind = "read"
	QueryKindSelect  QueryKind = "select"
	QueryKindWrite   QueryKind = "write"
)

// DefaultClient - стандартная реализация клиента ClickHouse.
type DefaultClient struct {
	conn driver.Conn
}

// Config содержит настройки подключения к ClickHouse.
type Config struct {
	Host               string
	Port               int
	Database           string
	Username           string
	Password           string
	Secure             bool
	InsecureSkipVerify bool
}

// BuildOptions собирает clickhouse.Options из продуктового конфига.
func BuildOptions(cfg Config) *driverapi.Options {
	opts := &driverapi.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: driverapi.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 10 * time.Minute,
		Compression: &driverapi.Compression{
			Method: driverapi.CompressionLZ4,
		},
		Settings: driverapi.Settings{
			"allow_experimental_object_type":             1,
			"output_format_json_named_tuples_as_objects": 1,
			"allow_suspicious_low_cardinality_types":     1,
			"format_csv_delimiter":                       ",",
		},
		Debug: false,
	}

	if cfg.Secure {
		opts.TLS = &tls.Config{
			InsecureSkipVerify: cfg.InsecureSkipVerify,
			MinVersion:         tls.VersionTLS12,
		}
	}

	return opts
}

// NewClient создает новый экземпляр клиента ClickHouse.
func NewClient(cfg Config) (Client, error) {
	conn, err := driverapi.Open(BuildOptions(cfg))
	if err != nil {
		return nil, fmt.Errorf("ошибка подключения к ClickHouse: %w", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("ошибка проверки соединения: %w", err)
	}

	return &DefaultClient{conn: conn}, nil
}

// GetDatabases возвращает список баз данных.
func (c *DefaultClient) GetDatabases(ctx context.Context) ([]string, error) {
	rows, err := c.conn.Query(ctx, "SHOW DATABASES")
	if err != nil {
		return nil, fmt.Errorf("ошибка получения списка баз данных: %w", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("ошибка сканирования базы данных: %w", err)
		}
		if name != "system" && name != "information_schema" {
			databases = append(databases, name)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ошибка чтения списка баз данных: %w", err)
	}

	return databases, nil
}

// GetTables возвращает список таблиц в указанной базе данных.
func (c *DefaultClient) GetTables(ctx context.Context, database string) ([]string, error) {
	rows, err := c.conn.Query(ctx, fmt.Sprintf("SHOW TABLES FROM %s", database))
	if err != nil {
		return nil, fmt.Errorf("ошибка получения списка таблиц: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("ошибка сканирования таблицы: %w", err)
		}
		tables = append(tables, name)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ошибка чтения списка таблиц: %w", err)
	}

	return tables, nil
}

// GetTableSchema возвращает схему указанной таблицы.
func (c *DefaultClient) GetTableSchema(ctx context.Context, database, table string) ([]ColumnInfo, error) {
	query := fmt.Sprintf("DESCRIBE TABLE %s.%s", database, table)
	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения схемы таблицы: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	position := 0
	for rows.Next() {
		position++
		var name, typ, defaultType, defaultExpression, comment, codecExpression, ttlExpression string
		if err := rows.Scan(&name, &typ, &defaultType, &defaultExpression, &comment, &codecExpression, &ttlExpression); err != nil {
			return nil, fmt.Errorf("ошибка сканирования колонки: %w", err)
		}

		columns = append(columns, ColumnInfo{
			Name:     name,
			Type:     typ,
			Position: position,
			IsArray:  IsArrayType(typ),
			IsNested: strings.HasPrefix(typ, "Nested"),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ошибка при получении схемы таблицы: %w", err)
	}

	return columns, nil
}

// QueryData выполняет уже подготовленный SQL запрос.
func (c *DefaultClient) QueryData(ctx context.Context, query string) (QueryResult, error) {
	cleanQuery := strings.TrimSpace(query)
	if cleanQuery == "" {
		return QueryResult{}, fmt.Errorf("SQL запрос пустой")
	}

	if err := c.ensureConnection(ctx); err != nil {
		return QueryResult{}, fmt.Errorf("ошибка подключения: %w", err)
	}

	rows, err := c.conn.Query(ctx, cleanQuery)
	if err != nil {
		return QueryResult{}, fmt.Errorf("ошибка выполнения запроса: %w", err)
	}
	defer rows.Close()

	columnTypes := rows.ColumnTypes()
	columnNames := rows.Columns()

	var columns []ColumnInfo
	for i, ct := range columnTypes {
		dbType := ct.DatabaseTypeName()

		columns = append(columns, ColumnInfo{
			Name:     ct.Name(),
			Type:     dbType,
			Position: i + 1,
			IsArray:  IsArrayType(dbType),
			IsNested: strings.HasPrefix(dbType, "Nested"),
		})
	}

	var results []map[string]any

	destPointers := make([]any, len(columnNames))
	stringVars := make([]string, len(columnNames))
	intVars := make([]int64, len(columnNames))
	uintVars := make([]uint64, len(columnNames))
	floatVars := make([]float64, len(columnNames))
	boolVars := make([]bool, len(columnNames))
	timeVars := make([]time.Time, len(columnNames))
	anyVars := make([]any, len(columnNames))

	for i, col := range columns {
		switch col.Type {
		case "String":
			destPointers[i] = &stringVars[i]
		case "UInt8", "UInt16", "UInt32", "UInt64":
			destPointers[i] = &uintVars[i]
		case "Int8", "Int16", "Int32", "Int64":
			destPointers[i] = &intVars[i]
		case "Float32", "Float64":
			destPointers[i] = &floatVars[i]
		case "Bool":
			destPointers[i] = &boolVars[i]
		case "Date", "DateTime":
			destPointers[i] = &timeVars[i]
		default:
			destPointers[i] = &anyVars[i]
		}
	}

	for rows.Next() {
		if err := rows.Scan(destPointers...); err != nil {
			return QueryResult{}, fmt.Errorf("ошибка сканирования строки: %w", err)
		}

		row := make(map[string]any, len(columns))
		for i, col := range columns {
			switch col.Type {
			case "String":
				row[col.Name] = stringVars[i]
			case "UInt8", "UInt16", "UInt32", "UInt64":
				row[col.Name] = uintVars[i]
			case "Int8", "Int16", "Int32", "Int64":
				row[col.Name] = intVars[i]
			case "Float32", "Float64":
				row[col.Name] = floatVars[i]
			case "Bool":
				row[col.Name] = boolVars[i]
			case "Date", "DateTime":
				row[col.Name] = timeVars[i].Format(time.RFC3339)
			default:
				row[col.Name] = normalizeScannedValue(col, anyVars[i])
			}
		}

		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return QueryResult{}, fmt.Errorf("ошибка при обработке результатов: %w", err)
	}

	return QueryResult{
		Columns: columns,
		Rows:    results,
	}, nil
}

// Execute выполняет SQL без набора строк в ответе.
func (c *DefaultClient) Execute(ctx context.Context, query string) error {
	cleanQuery := strings.TrimSpace(query)
	if cleanQuery == "" {
		return fmt.Errorf("SQL запрос пустой")
	}

	if err := c.ensureConnection(ctx); err != nil {
		return fmt.Errorf("ошибка подключения: %w", err)
	}

	if err := c.conn.Exec(ctx, cleanQuery); err != nil {
		return fmt.Errorf("ошибка выполнения запроса: %w", err)
	}

	return nil
}

func normalizeScannedValue(col ColumnInfo, value any) any {
	switch typed := value.(type) {
	case []byte:
		return string(typed)
	case []any:
		if col.IsArray && len(typed) > 0 {
			if _, ok := typed[0].([]byte); ok {
				strArray := make([]string, len(typed))
				for i, item := range typed {
					if byteItem, ok := item.([]byte); ok {
						strArray[i] = string(byteItem)
					} else {
						strArray[i] = fmt.Sprint(item)
					}
				}
				return strArray
			}
		}
		return typed
	default:
		return value
	}
}

// PrepareQuery применяет продуктовую политику к SQL.
func PrepareQuery(query string, requestedLimit int, policy QueryPolicy) (PreparedQuery, error) {
	normalized := normalizeQuery(query)
	if normalized == "" {
		return PreparedQuery{}, fmt.Errorf("SQL запрос пустой")
	}

	if hasMultipleStatements(normalized) {
		return PreparedQuery{}, fmt.Errorf("многооператорные SQL запросы не поддерживаются")
	}

	analysis := analyzeQuery(normalized)
	switch analysis.Kind {
	case QueryKindWrite:
		if !policy.AllowWrite {
			return PreparedQuery{}, fmt.Errorf("запросы на изменение данных и схемы отключены; включите allow-write, если это действительно нужно")
		}
	case QueryKindUnknown:
		if !policy.AllowWrite {
			return PreparedQuery{}, fmt.Errorf("не удалось безопасно определить тип SQL запроса; в read-only режиме разрешены только SELECT, SHOW, DESCRIBE и EXPLAIN")
		}
	}

	if requestedLimit < 0 {
		return PreparedQuery{}, fmt.Errorf("limit не может быть отрицательным")
	}

	if requestedLimit > 0 && policy.MaxLimit > 0 && requestedLimit > policy.MaxLimit {
		return PreparedQuery{}, fmt.Errorf("limit=%d превышает максимально допустимое значение %d", requestedLimit, policy.MaxLimit)
	}

	prepared := PreparedQuery{
		Query: normalized,
		Kind:  analysis.Kind,
	}

	if !analysis.LimitEligible || containsLimitClause(normalized) {
		return prepared, nil
	}

	effectiveLimit := requestedLimit
	if effectiveLimit == 0 {
		effectiveLimit = policy.DefaultLimit
	}

	if effectiveLimit <= 0 {
		return prepared, nil
	}

	prepared.Query = fmt.Sprintf("%s LIMIT %d", normalized, effectiveLimit)
	prepared.LimitApplied = true
	prepared.EffectiveLimit = effectiveLimit

	return prepared, nil
}

type queryAnalysis struct {
	Kind          QueryKind
	LimitEligible bool
}

func analyzeQuery(query string) queryAnalysis {
	tokens := tokenizeSQL(query)
	statement := detectStatement(tokens)

	switch statement {
	case "SELECT":
		return queryAnalysis{Kind: QueryKindSelect, LimitEligible: true}
	case "SHOW", "DESCRIBE", "DESC", "EXPLAIN":
		return queryAnalysis{Kind: QueryKindRead}
	case "INSERT", "ALTER", "CREATE", "DROP", "TRUNCATE", "RENAME", "OPTIMIZE", "SYSTEM", "GRANT", "REVOKE":
		return queryAnalysis{Kind: QueryKindWrite}
	default:
		return queryAnalysis{Kind: QueryKindUnknown}
	}
}

func detectStatement(tokens []string) string {
	if len(tokens) == 0 {
		return ""
	}

	if tokens[0] != "WITH" {
		return tokens[0]
	}

	for _, token := range tokens[1:] {
		switch token {
		case "SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "INSERT", "ALTER", "CREATE", "DROP", "TRUNCATE", "RENAME", "OPTIMIZE", "SYSTEM", "GRANT", "REVOKE":
			return token
		}
	}

	return "WITH"
}

// ensureConnection проверяет состояние соединения.
func (c *DefaultClient) ensureConnection(ctx context.Context) error {
	if err := c.conn.Ping(ctx); err != nil {
		return fmt.Errorf("потеряно соединение с ClickHouse: %w", err)
	}
	return nil
}

// normalizeQuery выполняет безопасную нормализацию SQL запроса.
func normalizeQuery(query string) string {
	query = strings.TrimSpace(query)
	for strings.HasSuffix(query, ";") {
		query = strings.TrimSpace(strings.TrimSuffix(query, ";"))
	}
	return query
}

// containsLimitClause проверяет, содержит ли запрос уже LIMIT вне строк и комментариев.
func containsLimitClause(query string) bool {
	for _, token := range tokenizeSQL(query) {
		if token == "LIMIT" {
			return true
		}
	}
	return false
}

func hasMultipleStatements(query string) bool {
	tokens := tokenizeSQL(query)
	seenSemicolon := false
	for _, token := range tokens {
		if token == ";" {
			seenSemicolon = true
			continue
		}
		if seenSemicolon {
			return true
		}
	}
	return false
}

// removeComments удаляет SQL комментарии из строки запроса.
func removeComments(query string) string {
	var result strings.Builder
	state := scanStateNormal

	for i := 0; i < len(query); i++ {
		ch := query[i]

		switch state {
		case scanStateNormal:
			if ch == '\'' {
				state = scanStateSingleQuote
				result.WriteByte(ch)
				continue
			}
			if ch == '"' {
				state = scanStateDoubleQuote
				result.WriteByte(ch)
				continue
			}
			if ch == '`' {
				state = scanStateBacktick
				result.WriteByte(ch)
				continue
			}
			if ch == '-' && i+1 < len(query) && query[i+1] == '-' {
				state = scanStateLineComment
				i++
				continue
			}
			if ch == '/' && i+1 < len(query) && query[i+1] == '*' {
				state = scanStateBlockComment
				i++
				continue
			}
			result.WriteByte(ch)
		case scanStateSingleQuote:
			result.WriteByte(ch)
			if ch == '\'' && !isEscaped(query, i) {
				state = scanStateNormal
			}
		case scanStateDoubleQuote:
			result.WriteByte(ch)
			if ch == '"' && !isEscaped(query, i) {
				state = scanStateNormal
			}
		case scanStateBacktick:
			result.WriteByte(ch)
			if ch == '`' && !isEscaped(query, i) {
				state = scanStateNormal
			}
		case scanStateLineComment:
			if ch == '\n' {
				state = scanStateNormal
				result.WriteByte(ch)
			}
		case scanStateBlockComment:
			if ch == '*' && i+1 < len(query) && query[i+1] == '/' {
				state = scanStateNormal
				i++
			}
		}
	}

	return result.String()
}

func tokenizeSQL(query string) []string {
	trimmed := removeComments(query)
	tokens := make([]string, 0, 8)
	state := scanStateNormal

	for i := 0; i < len(trimmed); i++ {
		ch := trimmed[i]

		switch state {
		case scanStateNormal:
			switch {
			case ch == '\'':
				state = scanStateSingleQuote
			case ch == '"':
				state = scanStateDoubleQuote
			case ch == '`':
				state = scanStateBacktick
			case isWordStart(ch):
				start := i
				for i+1 < len(trimmed) && isWordPart(trimmed[i+1]) {
					i++
				}
				tokens = append(tokens, strings.ToUpper(trimmed[start:i+1]))
			case ch == ';':
				tokens = append(tokens, ";")
			}
		case scanStateSingleQuote:
			if ch == '\'' && !isEscaped(trimmed, i) {
				state = scanStateNormal
			}
		case scanStateDoubleQuote:
			if ch == '"' && !isEscaped(trimmed, i) {
				state = scanStateNormal
			}
		case scanStateBacktick:
			if ch == '`' && !isEscaped(trimmed, i) {
				state = scanStateNormal
			}
		}
	}

	return tokens
}

type scanState int

const (
	scanStateNormal scanState = iota
	scanStateSingleQuote
	scanStateDoubleQuote
	scanStateBacktick
	scanStateLineComment
	scanStateBlockComment
)

func isEscaped(input string, index int) bool {
	backslashes := 0
	for i := index - 1; i >= 0 && input[i] == '\\'; i-- {
		backslashes++
	}
	return backslashes%2 == 1
}

func isWordStart(ch byte) bool {
	return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_'
}

func isWordPart(ch byte) bool {
	return isWordStart(ch) || (ch >= '0' && ch <= '9')
}

// GetConnection возвращает соединение с ClickHouse.
func (c *DefaultClient) GetConnection() driver.Conn {
	return c.conn
}

// Close закрывает соединение с ClickHouse.
func (c *DefaultClient) Close() error {
	return c.conn.Close()
}

// IsArrayType проверяет, является ли тип массивом.
func IsArrayType(typeName string) bool {
	return strings.HasPrefix(typeName, "Array(")
}

// GetBaseType возвращает базовый тип из названия типа ClickHouse.
func GetBaseType(typeName string) string {
	if IsArrayType(typeName) {
		return typeName[6 : len(typeName)-1]
	}
	return typeName
}
