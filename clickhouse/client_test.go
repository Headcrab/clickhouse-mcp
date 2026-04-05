package clickhouse

import "testing"

func TestIsArrayType(t *testing.T) {
	tests := []struct {
		name     string
		typeName string
		want     bool
	}{
		{name: "simple type", typeName: "String", want: false},
		{name: "array", typeName: "Array(String)", want: true},
		{name: "nested array", typeName: "Array(Array(Int32))", want: true},
		{name: "empty", typeName: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsArrayType(tt.typeName); got != tt.want {
				t.Fatalf("IsArrayType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetBaseType(t *testing.T) {
	tests := []struct {
		name     string
		typeName string
		want     string
	}{
		{name: "simple type", typeName: "String", want: "String"},
		{name: "array", typeName: "Array(String)", want: "String"},
		{name: "nested array", typeName: "Array(Array(Int32))", want: "Array(Int32)"},
		{name: "empty", typeName: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetBaseType(tt.typeName); got != tt.want {
				t.Fatalf("GetBaseType() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildOptionsTLS(t *testing.T) {
	opts := BuildOptions(Config{
		Host:               "localhost",
		Port:               9440,
		Database:           "default",
		Username:           "default",
		Secure:             true,
		InsecureSkipVerify: false,
	})

	if opts.TLS == nil {
		t.Fatalf("expected TLS config")
	}
	if opts.TLS.InsecureSkipVerify {
		t.Fatalf("expected certificate verification to stay enabled")
	}

	opts = BuildOptions(Config{
		Host:               "localhost",
		Port:               9440,
		Database:           "default",
		Username:           "default",
		Secure:             true,
		InsecureSkipVerify: true,
	})

	if !opts.TLS.InsecureSkipVerify {
		t.Fatalf("expected insecure skip verify to be applied")
	}
}

func TestPrepareQuery(t *testing.T) {
	policy := QueryPolicy{
		AllowWrite:   false,
		DefaultLimit: 100,
		MaxLimit:     10000,
	}

	tests := []struct {
		name        string
		query       string
		limit       int
		policy      QueryPolicy
		wantQuery   string
		wantKind    QueryKind
		wantLimit   int
		wantApplied bool
		expectError bool
	}{
		{
			name:        "select gets default limit",
			query:       "SELECT * FROM test_table",
			policy:      policy,
			wantQuery:   "SELECT * FROM test_table LIMIT 100",
			wantKind:    QueryKindSelect,
			wantLimit:   100,
			wantApplied: true,
		},
		{
			name:        "select keeps explicit limit",
			query:       "SELECT * FROM test_table LIMIT 10",
			policy:      policy,
			wantQuery:   "SELECT * FROM test_table LIMIT 10",
			wantKind:    QueryKindSelect,
			wantLimit:   0,
			wantApplied: false,
		},
		{
			name:        "with select gets limit",
			query:       "WITH x AS (SELECT 1) SELECT * FROM test_table",
			policy:      policy,
			wantQuery:   "WITH x AS (SELECT 1) SELECT * FROM test_table LIMIT 100",
			wantKind:    QueryKindSelect,
			wantLimit:   100,
			wantApplied: true,
		},
		{
			name:        "show stays untouched",
			query:       "SHOW TABLES",
			policy:      policy,
			wantQuery:   "SHOW TABLES",
			wantKind:    QueryKindRead,
			wantLimit:   0,
			wantApplied: false,
		},
		{
			name:        "insert blocked in readonly mode",
			query:       "INSERT INTO test_table VALUES (1)",
			policy:      policy,
			expectError: true,
		},
		{
			name:  "insert allowed with flag",
			query: "INSERT INTO test_table VALUES (1)",
			policy: QueryPolicy{
				AllowWrite:   true,
				DefaultLimit: 100,
				MaxLimit:     10000,
			},
			wantQuery:   "INSERT INTO test_table VALUES (1)",
			wantKind:    QueryKindWrite,
			wantLimit:   0,
			wantApplied: false,
		},
		{
			name:        "multi statement blocked",
			query:       "SELECT 1; SELECT 2",
			policy:      policy,
			expectError: true,
		},
		{
			name:        "limit above max is rejected",
			query:       "SELECT * FROM test_table",
			limit:       10001,
			policy:      policy,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prepared, err := PrepareQuery(tt.query, tt.limit, tt.policy)
			if (err != nil) != tt.expectError {
				t.Fatalf("PrepareQuery() error = %v, expectError = %v", err, tt.expectError)
			}

			if tt.expectError {
				return
			}

			if prepared.Query != tt.wantQuery {
				t.Fatalf("Query = %q, want %q", prepared.Query, tt.wantQuery)
			}
			if prepared.Kind != tt.wantKind {
				t.Fatalf("Kind = %q, want %q", prepared.Kind, tt.wantKind)
			}
			if prepared.EffectiveLimit != tt.wantLimit {
				t.Fatalf("EffectiveLimit = %d, want %d", prepared.EffectiveLimit, tt.wantLimit)
			}
			if prepared.LimitApplied != tt.wantApplied {
				t.Fatalf("LimitApplied = %v, want %v", prepared.LimitApplied, tt.wantApplied)
			}
		})
	}
}

func TestContainsLimitClause(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{name: "without limit", query: "SELECT * FROM test_table", want: false},
		{name: "with limit", query: "SELECT * FROM test_table LIMIT 10", want: true},
		{name: "limit in comment", query: "SELECT * FROM test_table /* LIMIT 10 */", want: false},
		{name: "limit in string", query: "SELECT 'LIMIT 10' FROM test_table", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := containsLimitClause(tt.query); got != tt.want {
				t.Fatalf("containsLimitClause() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRemoveCommentsPreservesStrings(t *testing.T) {
	query := "SELECT '-- not a comment' AS text /* block */ FROM test_table -- line"
	got := removeComments(query)
	want := "SELECT '-- not a comment' AS text  FROM test_table "

	if got != want {
		t.Fatalf("removeComments() = %q, want %q", got, want)
	}
}
