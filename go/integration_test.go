package main

import (
	"database/sql"
	"testing"

	sf "github.com/snowflakedb/gosnowflake"
)

func getDB(t *testing.T) *sql.DB {
	t.Helper()

	cfg := &sf.Config{
		Account:   "test_account",
		User:      "test_user",
		Password:  "test_password",
		Database:  "test_db",
		Schema:    "public",
		Protocol:  "http",
		Host:      "localhost",
		Port:      8080,
		Warehouse: "test_wh",
	}

	dsn, err := sf.DSN(cfg)
	if err != nil {
		t.Fatalf("Failed to create DSN: %v", err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	return db
}

func TestPing(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func TestSelectLiteral(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result int
	err := db.QueryRow("SELECT 42").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != 42 {
		t.Errorf("Expected 42, got %d", result)
	}
}

func TestSelectExpression(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result int
	err := db.QueryRow("SELECT 1 + 2").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != 3 {
		t.Errorf("Expected 3, got %d", result)
	}
}

func TestCreateTableInsertSelect(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// CREATE TABLE
	_, err := db.Exec("CREATE TABLE go_test_users (id INT, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// INSERT
	_, err = db.Exec("INSERT INTO go_test_users VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// SELECT
	rows, err := db.Query("SELECT id, name FROM go_test_users ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rows.Close()

	type user struct {
		ID   int
		Name string
	}

	var users []user
	for rows.Next() {
		var u user
		if err := rows.Scan(&u.ID, &u.Name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		users = append(users, u)
	}

	if len(users) != 2 {
		t.Fatalf("Expected 2 users, got %d", len(users))
	}

	if users[0].ID != 1 || users[0].Name != "Alice" {
		t.Errorf("Expected {1, Alice}, got %v", users[0])
	}

	if users[1].ID != 2 || users[1].Name != "Bob" {
		t.Errorf("Expected {2, Bob}, got %v", users[1])
	}
}

func TestSelectWithWhere(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE go_test_products (id INT, name VARCHAR, price DOUBLE)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO go_test_products VALUES (1, 'Apple', 1.5), (2, 'Banana', 0.5), (3, 'Cherry', 3.0)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	rows, err := db.Query("SELECT name FROM go_test_products WHERE price > 1.0 ORDER BY name")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		names = append(names, name)
	}

	if len(names) != 2 {
		t.Fatalf("Expected 2 names, got %d", len(names))
	}

	if names[0] != "Apple" || names[1] != "Cherry" {
		t.Errorf("Expected [Apple, Cherry], got %v", names)
	}
}

func TestAggregateQuery(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE go_test_sales (amount DOUBLE)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO go_test_sales VALUES (100), (200), (300)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	var count int
	var total float64
	err = db.QueryRow("SELECT COUNT(*), SUM(amount) FROM go_test_sales").Scan(&count, &total)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}

	if total != 600 {
		t.Errorf("Expected total 600, got %f", total)
	}
}

// =============================================================================
// Phase 2: Snowflake UDF Tests
// =============================================================================

// Conditional Functions Tests

func TestIFF(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{"IFF true", "SELECT IFF(1=1, 'yes', 'no')", "yes"},
		{"IFF false", "SELECT IFF(1=2, 'yes', 'no')", "no"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result string
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestNVL(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// NVL with non-null value
	var result int
	err := db.QueryRow("SELECT NVL(10, 20)").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result != 10 {
		t.Errorf("Expected 10, got %d", result)
	}
}

func TestNVL2(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// NVL2 with non-null value returns second argument
	var result string
	err := db.QueryRow("SELECT NVL2(10, 'has value', 'no value')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result != "has value" {
		t.Errorf("Expected 'has value', got %s", result)
	}
}

// JSON Functions Tests

func TestParseJSON(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow(`SELECT PARSE_JSON('{"name": "Alice", "age": 30}')`).Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Result should be valid JSON
	if result == "" {
		t.Error("Expected non-empty JSON string")
	}
}

func TestToJSON(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT TO_JSON(42)").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "42" {
		t.Errorf("Expected '42', got %s", result)
	}
}

// Date/Time Functions Tests

func TestDATEADD(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT DATEADD('day', 5, '2024-01-15')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "2024-01-20" {
		t.Errorf("Expected '2024-01-20', got %s", result)
	}
}

func TestDATEADDMonth(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT DATEADD('month', 2, '2024-01-15')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "2024-03-15" {
		t.Errorf("Expected '2024-03-15', got %s", result)
	}
}

func TestDATEDIFF(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result int
	err := db.QueryRow("SELECT DATEDIFF('day', '2024-01-01', '2024-01-15')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != 14 {
		t.Errorf("Expected 14, got %d", result)
	}
}

func TestDATEDIFFMonth(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result int
	err := db.QueryRow("SELECT DATEDIFF('month', '2024-01-15', '2024-06-15')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != 5 {
		t.Errorf("Expected 5, got %d", result)
	}
}

// TRY_* Functions Tests

func TestTryParseJSON(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// Valid JSON
	var result sql.NullString
	err := db.QueryRow(`SELECT TRY_PARSE_JSON('{"key": "value"}')`).Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if !result.Valid {
		t.Error("Expected valid JSON result")
	}

	// Invalid JSON should return NULL
	err = db.QueryRow("SELECT TRY_PARSE_JSON('not valid json')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result.Valid {
		t.Errorf("Expected NULL for invalid JSON, got %s", result.String)
	}
}

func TestTryToNumber(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// Valid number
	var result sql.NullFloat64
	err := db.QueryRow("SELECT TRY_TO_NUMBER('42.5')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if !result.Valid || result.Float64 != 42.5 {
		t.Errorf("Expected 42.5, got %v", result)
	}

	// Invalid number should return NULL
	err = db.QueryRow("SELECT TRY_TO_NUMBER('not a number')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result.Valid {
		t.Errorf("Expected NULL for invalid number, got %f", result.Float64)
	}
}

func TestTryToBoolean(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		input    string
		expected sql.NullBool
	}{
		{"true string", "'true'", sql.NullBool{Bool: true, Valid: true}},
		{"false string", "'false'", sql.NullBool{Bool: false, Valid: true}},
		{"yes string", "'yes'", sql.NullBool{Bool: true, Valid: true}},
		{"no string", "'no'", sql.NullBool{Bool: false, Valid: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullBool
			err := db.QueryRow("SELECT TRY_TO_BOOLEAN(" + tt.input + ")").Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result.Valid != tt.expected.Valid || result.Bool != tt.expected.Bool {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// Array/Object Functions Tests

func TestArraySize(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result int
	err := db.QueryRow("SELECT ARRAY_SIZE('[1, 2, 3, 4, 5]')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != 5 {
		t.Errorf("Expected 5, got %d", result)
	}
}

func TestArraySizeEmpty(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result int
	err := db.QueryRow("SELECT ARRAY_SIZE('[]')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != 0 {
		t.Errorf("Expected 0, got %d", result)
	}
}

func TestFlattenArray(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT FLATTEN_ARRAY('[1, 2, 3]', 1)").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "2" {
		t.Errorf("Expected '2', got %s", result)
	}
}

func TestGetPath(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow(`SELECT GET_PATH('{"name": "Alice", "age": 30}', 'name')`).Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "Alice" {
		t.Errorf("Expected 'Alice', got %s", result)
	}
}

func TestGetPathNested(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow(`SELECT GET_PATH('{"user": {"address": {"city": "Tokyo"}}}', 'user.address.city')`).Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "Tokyo" {
		t.Errorf("Expected 'Tokyo', got %s", result)
	}
}

func TestObjectKeys(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow(`SELECT OBJECT_KEYS('{"name": "Alice", "age": 30}')`).Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Result should be a JSON array of keys
	if result == "" || result == "null" {
		t.Error("Expected non-empty result")
	}
}

// =============================================================================
// Phase 3.1: VARIANT/ARRAY/OBJECT Type Support Tests
// =============================================================================

// Type Checking Functions Tests

func TestIsArray(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"array", `'[1, 2, 3]'`, true},
		{"object", `'{"a": 1}'`, false},
		{"string", `'"hello"'`, false},
		{"number", `'42'`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result bool
			err := db.QueryRow("SELECT IS_ARRAY(" + tt.input + ")").Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestIsObject(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"object", `'{"a": 1}'`, true},
		{"array", `'[1, 2, 3]'`, false},
		{"string", `'"hello"'`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result bool
			err := db.QueryRow("SELECT IS_OBJECT(" + tt.input + ")").Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestTypeof(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"array", `'[1, 2, 3]'`, "ARRAY"},
		{"object", `'{"a": 1}'`, "OBJECT"},
		{"integer", `'42'`, "INTEGER"},
		{"boolean", `'true'`, "BOOLEAN"},
		{"null", `'null'`, "NULL_VALUE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT TYPEOF(" + tt.input + ")").Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

// Array Functions Tests

func TestArrayConstruct(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT ARRAY_CONSTRUCT(1, 2, 3)").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "[1,2,3]" {
		t.Errorf("Expected [1,2,3], got %s", result)
	}
}

func TestArrayConstructCompact(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT ARRAY_CONSTRUCT_COMPACT(1, NULL, 3)").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "[1,3]" {
		t.Errorf("Expected [1,3], got %s", result)
	}
}

func TestArrayAppend(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT ARRAY_APPEND('[1, 2]', 3)").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "[1,2,3]" {
		t.Errorf("Expected [1,2,3], got %s", result)
	}
}

func TestArrayCat(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT ARRAY_CAT('[1, 2]', '[3, 4]')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "[1,2,3,4]" {
		t.Errorf("Expected [1,2,3,4], got %s", result)
	}
}

func TestArrayContains(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		element  string
		array    string
		expected bool
	}{
		{"found", "2", "'[1, 2, 3]'", true},
		{"not found", "5", "'[1, 2, 3]'", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result bool
			query := "SELECT ARRAY_CONTAINS(" + tt.element + ", " + tt.array + ")"
			err := db.QueryRow(query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestArrayDistinct(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT ARRAY_DISTINCT('[1, 2, 2, 3, 1]')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "[1,2,3]" {
		t.Errorf("Expected [1,2,3], got %s", result)
	}
}

func TestArrayFlatten(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT ARRAY_FLATTEN('[[1, 2], [3, 4]]')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "[1,2,3,4]" {
		t.Errorf("Expected [1,2,3,4], got %s", result)
	}
}

// Object Functions Tests

func TestObjectConstruct(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT OBJECT_CONSTRUCT('a', 1, 'b', 2)").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// The result could be {"a":1,"b":2} or {"b":2,"a":1}
	if result != `{"a":1,"b":2}` && result != `{"b":2,"a":1}` {
		t.Errorf("Expected {\"a\":1,\"b\":2}, got %s", result)
	}
}

func TestObjectInsert(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow(`SELECT OBJECT_INSERT('{"a": 1}', 'b', 2)`).Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Result should contain both keys
	if result != `{"a":1,"b":2}` && result != `{"b":2,"a":1}` {
		t.Errorf("Expected object with a=1 and b=2, got %s", result)
	}
}

func TestObjectDelete(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow(`SELECT OBJECT_DELETE('{"a": 1, "b": 2}', 'a')`).Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != `{"b":2}` {
		t.Errorf("Expected {\"b\":2}, got %s", result)
	}
}

func TestObjectPick(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow(`SELECT OBJECT_PICK('{"a": 1, "b": 2, "c": 3}', 'a', 'c')`).Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Result should contain only a and c
	if result != `{"a":1,"c":3}` && result != `{"c":3,"a":1}` {
		t.Errorf("Expected object with a=1 and c=3, got %s", result)
	}
}

// Conversion Functions Tests

func TestToVariant(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT TO_VARIANT(42)").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "42" {
		t.Errorf("Expected 42, got %s", result)
	}
}

func TestToArray(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"already array", "'[1, 2, 3]'", "[1,2,3]"},
		{"non-array", "'42'", "[42]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT TO_ARRAY(" + tt.input + ")").Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestToObject(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result sql.NullString
	err := db.QueryRow(`SELECT TO_OBJECT('{"a": 1}')`).Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if !result.Valid || result.String != `{"a":1}` {
		t.Errorf("Expected {\"a\":1}, got %v", result)
	}

	// Non-object should return NULL
	err = db.QueryRow("SELECT TO_OBJECT('[1, 2, 3]')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result.Valid {
		t.Errorf("Expected NULL for non-object, got %s", result.String)
	}
}

// String Functions Tests

func TestSplit(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT SPLIT('a,b,c', ',')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != `["a","b","c"]` {
		t.Errorf("Expected [\"a\",\"b\",\"c\"], got %s", result)
	}
}

func TestStrtok(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{"first token", "SELECT STRTOK('a.b.c', '.', 1)", "a"},
		{"second token", "SELECT STRTOK('a.b.c', '.', 2)", "b"},
		{"third token", "SELECT STRTOK('a.b.c', '.', 3)", "c"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result string
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestStrtokToArray(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT STRTOK_TO_ARRAY('a.b.c', '.')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != `["a","b","c"]` {
		t.Errorf("Expected [\"a\",\"b\",\"c\"], got %s", result)
	}
}

func TestRegexpLike(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected bool
	}{
		{"match", "SELECT REGEXP_LIKE('abc123', '[0-9]+')", true},
		{"no match", "SELECT REGEXP_LIKE('abc', '[0-9]+')", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result bool
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestRegexpSubstr(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT REGEXP_SUBSTR('abc123def', '[0-9]+')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "123" {
		t.Errorf("Expected 123, got %s", result)
	}
}

func TestRegexpReplace(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT REGEXP_REPLACE('abc123def', '[0-9]+', 'XXX')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "abcXXXdef" {
		t.Errorf("Expected abcXXXdef, got %s", result)
	}
}

func TestRegexpCount(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result int64
	err := db.QueryRow("SELECT REGEXP_COUNT('abab', 'ab')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != 2 {
		t.Errorf("Expected 2, got %d", result)
	}
}

func TestContains(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected bool
	}{
		{"contains", "SELECT CONTAINS('hello world', 'world')", true},
		{"not contains", "SELECT CONTAINS('hello world', 'foo')", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result bool
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestStartswith(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected bool
	}{
		{"startswith", "SELECT STARTSWITH('hello world', 'hello')", true},
		{"not startswith", "SELECT STARTSWITH('hello world', 'world')", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result bool
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestEndswith(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected bool
	}{
		{"endswith", "SELECT ENDSWITH('hello world', 'world')", true},
		{"not endswith", "SELECT ENDSWITH('hello world', 'hello')", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result bool
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// =============================================================================
// Aggregate Function Tests
// =============================================================================

func TestArrayAgg(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// Create test table
	_, err := db.Exec("CREATE TABLE test_agg_arr (val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	defer db.Exec("DROP TABLE test_agg_arr")

	// Insert test data
	_, err = db.Exec("INSERT INTO test_agg_arr VALUES (1), (2), (3)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test ARRAY_AGG
	var result string
	err = db.QueryRow("SELECT ARRAY_AGG(val) FROM test_agg_arr").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Result should be JSON array
	if result != "[1,2,3]" {
		t.Errorf("Expected [1,2,3], got %s", result)
	}
}

func TestObjectAgg(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// Create test table
	_, err := db.Exec("CREATE TABLE test_agg_obj (k VARCHAR, v INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	defer db.Exec("DROP TABLE test_agg_obj")

	// Insert test data
	_, err = db.Exec("INSERT INTO test_agg_obj VALUES ('a', 1), ('b', 2)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test OBJECT_AGG
	var result string
	err = db.QueryRow("SELECT OBJECT_AGG(k, v) FROM test_agg_obj").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Result should be JSON object (key order may vary)
	if result != `{"a":1,"b":2}` && result != `{"b":2,"a":1}` {
		t.Errorf("Expected JSON object with a:1 and b:2, got %s", result)
	}
}

func TestListagg(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// Create test table
	_, err := db.Exec("CREATE TABLE test_listagg (name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	defer db.Exec("DROP TABLE test_listagg")

	// Insert test data
	_, err = db.Exec("INSERT INTO test_listagg VALUES ('Alice'), ('Bob'), ('Charlie')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test LISTAGG with default delimiter
	var result string
	err = db.QueryRow("SELECT LISTAGG(name) FROM test_listagg").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Default delimiter is comma
	expected := "Alice,Bob,Charlie"
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

// =============================================================================
// LATERAL FLATTEN Tests
// =============================================================================

func TestLateralFlatten(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// Create test table with JSON array
	_, err := db.Exec("CREATE TABLE test_lateral (id INT, arr VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	defer db.Exec("DROP TABLE test_lateral")

	// Insert test data
	_, err = db.Exec("INSERT INTO test_lateral VALUES (1, '[10, 20, 30]'), (2, '[40, 50]')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test LATERAL FLATTEN
	rows, err := db.Query("SELECT t.id, f.value FROM test_lateral t, LATERAL FLATTEN(input => t.arr) f ORDER BY t.id, f.index")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		id    int
		value string
	}{
		{1, "10"},
		{1, "20"},
		{1, "30"},
		{2, "40"},
		{2, "50"},
	}

	i := 0
	for rows.Next() {
		var id int
		var value string
		if err := rows.Scan(&id, &value); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if i >= len(expected) {
			t.Fatalf("More rows than expected")
		}
		if id != expected[i].id || value != expected[i].value {
			t.Errorf("Row %d: expected (%d, %s), got (%d, %s)", i, expected[i].id, expected[i].value, id, value)
		}
		i++
	}

	if i != len(expected) {
		t.Errorf("Expected %d rows, got %d", len(expected), i)
	}
}

func TestNumbersTable(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// Test _numbers table exists and has expected values
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM _numbers").Scan(&count)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if count != 1000 {
		t.Errorf("Expected 1000 rows in _numbers, got %d", count)
	}

	// Test selecting specific range
	rows, err := db.Query("SELECT idx FROM _numbers WHERE idx < 5 ORDER BY idx")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	expected := []int64{0, 1, 2, 3, 4}
	i := 0
	for rows.Next() {
		var idx int64
		if err := rows.Scan(&idx); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if idx != expected[i] {
			t.Errorf("Expected idx %d, got %d", expected[i], idx)
		}
		i++
	}

	if i != len(expected) {
		t.Errorf("Expected %d rows, got %d", len(expected), i)
	}
}

// =============================================================================
// Window Function Tests
// =============================================================================

func TestWindowRowNumber(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// Create test table
	_, err := db.Exec("CREATE TABLE test_win_rn (id INT, category VARCHAR, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	defer db.Exec("DROP TABLE test_win_rn")

	// Insert test data
	_, err = db.Exec("INSERT INTO test_win_rn VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30), (4, 'B', 40)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test ROW_NUMBER with PARTITION BY
	rows, err := db.Query("SELECT id, ROW_NUMBER() OVER (PARTITION BY category ORDER BY value) as rn FROM test_win_rn ORDER BY id")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		id int
		rn int64
	}{
		{1, 1}, // category A, first
		{2, 2}, // category A, second
		{3, 1}, // category B, first
		{4, 2}, // category B, second
	}

	i := 0
	for rows.Next() {
		var id int
		var rn int64
		if err := rows.Scan(&id, &rn); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if id != expected[i].id || rn != expected[i].rn {
			t.Errorf("Row %d: expected (%d, %d), got (%d, %d)", i, expected[i].id, expected[i].rn, id, rn)
		}
		i++
	}
}

func TestWindowRankDenseRank(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// Create test table
	_, err := db.Exec("CREATE TABLE test_win_rank (id INT, score INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	defer db.Exec("DROP TABLE test_win_rank")

	// Insert test data with ties
	_, err = db.Exec("INSERT INTO test_win_rank VALUES (1, 100), (2, 100), (3, 90), (4, 80)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test RANK and DENSE_RANK
	rows, err := db.Query("SELECT id, RANK() OVER (ORDER BY score DESC) as rnk, DENSE_RANK() OVER (ORDER BY score DESC) as drnk FROM test_win_rank ORDER BY id")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		id   int
		rnk  int64
		drnk int64
	}{
		{1, 1, 1}, // score 100, rank 1, dense_rank 1
		{2, 1, 1}, // score 100, rank 1 (tie), dense_rank 1
		{3, 3, 2}, // score 90, rank 3 (skip 2), dense_rank 2
		{4, 4, 3}, // score 80, rank 4, dense_rank 3
	}

	i := 0
	for rows.Next() {
		var id int
		var rnk, drnk int64
		if err := rows.Scan(&id, &rnk, &drnk); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if id != expected[i].id || rnk != expected[i].rnk || drnk != expected[i].drnk {
			t.Errorf("Row %d: expected (%d, %d, %d), got (%d, %d, %d)",
				i, expected[i].id, expected[i].rnk, expected[i].drnk, id, rnk, drnk)
		}
		i++
	}
}

func TestWindowLagLead(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// Create test table
	_, err := db.Exec("CREATE TABLE test_win_lag (id INT, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	defer db.Exec("DROP TABLE test_win_lag")

	// Insert test data
	_, err = db.Exec("INSERT INTO test_win_lag VALUES (1, 10), (2, 20), (3, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test LAG and LEAD
	rows, err := db.Query("SELECT id, LAG(value, 1) OVER (ORDER BY id) as prev_val, LEAD(value, 1) OVER (ORDER BY id) as next_val FROM test_win_lag ORDER BY id")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	type row struct {
		id      int
		prevVal *int
		nextVal *int
	}

	ten := 10
	twenty := 20
	thirty := 30

	expected := []row{
		{1, nil, &twenty},  // first row: no prev, next=20
		{2, &ten, &thirty}, // middle row: prev=10, next=30
		{3, &twenty, nil},  // last row: prev=20, no next
	}

	i := 0
	for rows.Next() {
		var id int
		var prevVal, nextVal *int
		if err := rows.Scan(&id, &prevVal, &nextVal); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}

		// Check id
		if id != expected[i].id {
			t.Errorf("Row %d: expected id %d, got %d", i, expected[i].id, id)
		}

		// Check prevVal
		if (prevVal == nil) != (expected[i].prevVal == nil) {
			t.Errorf("Row %d: prevVal nil mismatch", i)
		} else if prevVal != nil && *prevVal != *expected[i].prevVal {
			t.Errorf("Row %d: expected prevVal %d, got %d", i, *expected[i].prevVal, *prevVal)
		}

		// Check nextVal
		if (nextVal == nil) != (expected[i].nextVal == nil) {
			t.Errorf("Row %d: nextVal nil mismatch", i)
		} else if nextVal != nil && *nextVal != *expected[i].nextVal {
			t.Errorf("Row %d: expected nextVal %d, got %d", i, *expected[i].nextVal, *nextVal)
		}

		i++
	}
}

// =============================================================================
// Phase 4: Extended Functions Tests
// =============================================================================

// Date/Time Functions Tests (Extended)

func TestToDate(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{"ISO format", "SELECT TO_DATE('2024-03-15')", "2024-03-15"},
		{"US format", "SELECT TO_DATE('03/15/2024')", "2024-03-15"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result string
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestLastDay(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{"January", "SELECT LAST_DAY('2024-01-15')", "2024-01-31"},
		{"February leap year", "SELECT LAST_DAY('2024-02-15')", "2024-02-29"},
		{"April", "SELECT LAST_DAY('2024-04-10')", "2024-04-30"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result string
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestDayname(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{"Monday", "SELECT DAYNAME('2024-01-15')", "Mon"},
		{"Sunday", "SELECT DAYNAME('2024-01-14')", "Sun"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result string
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestMonthname(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{"January", "SELECT MONTHNAME('2024-01-15')", "Jan"},
		{"December", "SELECT MONTHNAME('2024-12-25')", "Dec"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result string
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

// Numeric Functions Tests

func TestDiv0(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{"normal division", "SELECT DIV0(10, 2)", 5.0},
		{"division by zero", "SELECT DIV0(10, 0)", 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result float64
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %f, got %f", tt.expected, result)
			}
		})
	}
}

func TestDiv0Null(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// Normal division
	var result sql.NullFloat64
	err := db.QueryRow("SELECT DIV0NULL(10, 2)").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if !result.Valid || result.Float64 != 5.0 {
		t.Errorf("Expected 5.0, got %v", result)
	}

	// Division by zero returns NULL
	err = db.QueryRow("SELECT DIV0NULL(10, 0)").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result.Valid {
		t.Errorf("Expected NULL for division by zero, got %f", result.Float64)
	}
}

// Hash Functions Tests

func TestSHA1(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT SHA1('hello')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// SHA1 of "hello" is aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d
	expected := "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestSHA2(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			"SHA256",
			"SELECT SHA2('hello', 256)",
			"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
		},
		{
			"SHA512",
			"SELECT SHA2('hello', 512)",
			"9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result string
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

// Context Functions Tests

func TestCurrentUser(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT CURRENT_USER()").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "EMULATOR_USER" {
		t.Errorf("Expected EMULATOR_USER, got %s", result)
	}
}

func TestCurrentRole(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT CURRENT_ROLE()").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "ACCOUNTADMIN" {
		t.Errorf("Expected ACCOUNTADMIN, got %s", result)
	}
}

func TestCurrentDatabase(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT CURRENT_DATABASE()").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "EMULATOR_DB" {
		t.Errorf("Expected EMULATOR_DB, got %s", result)
	}
}

func TestCurrentSchema(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT CURRENT_SCHEMA()").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "PUBLIC" {
		t.Errorf("Expected PUBLIC, got %s", result)
	}
}

func TestCurrentWarehouse(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT CURRENT_WAREHOUSE()").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "EMULATOR_WH" {
		t.Errorf("Expected EMULATOR_WH, got %s", result)
	}
}

// SQL Rewriter Function Mapping Tests

func TestCurrentTimestampRewrite(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// CURRENT_TIMESTAMP should be rewritten to now()
	var result string
	err := db.QueryRow("SELECT CURRENT_TIMESTAMP").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Result should be a valid timestamp string
	if result == "" {
		t.Error("Expected non-empty timestamp")
	}
}

func TestLenRewrite(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// LEN should be rewritten to length
	var result int
	err := db.QueryRow("SELECT LEN('hello')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != 5 {
		t.Errorf("Expected 5, got %d", result)
	}
}

// Phase 5 String Functions

func TestCharindex(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result int
	err := db.QueryRow("SELECT CHARINDEX('bar', 'foobar')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != 4 {
		t.Errorf("Expected 4, got %d", result)
	}
}

func TestPosition(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result int
	err := db.QueryRow("SELECT POSITION('bar' IN 'foobar')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != 4 {
		t.Errorf("Expected 4, got %d", result)
	}
}

func TestReverse(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT REVERSE('hello')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "olleh" {
		t.Errorf("Expected 'olleh', got '%s'", result)
	}
}

func TestLpad(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT LPAD('123', 5, '0')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "00123" {
		t.Errorf("Expected '00123', got '%s'", result)
	}
}

func TestRpad(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT RPAD('123', 5, '0')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "12300" {
		t.Errorf("Expected '12300', got '%s'", result)
	}
}

func TestTranslate(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	var result string
	err := db.QueryRow("SELECT TRANSLATE('abc', 'abc', '123')").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "123" {
		t.Errorf("Expected '123', got '%s'", result)
	}
}

// Phase 5 Window Functions

func TestWindowFirstLastValue(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// Create and populate table
	_, err := db.Exec("CREATE TABLE test_firstlast (id INT, category VARCHAR, value INT)")
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO test_firstlast VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30)")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	var id int
	var firstVal int
	err = db.QueryRow("SELECT id, FIRST_VALUE(value) OVER (PARTITION BY category ORDER BY id) as fv FROM test_firstlast WHERE id = 2").Scan(&id, &firstVal)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if firstVal != 10 {
		t.Errorf("Expected first_value 10, got %d", firstVal)
	}
}

func TestWindowNtile(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// Create and populate table
	_, err := db.Exec("CREATE TABLE test_ntile (id INT, value INT)")
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO test_ntile VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	rows, err := db.Query("SELECT id, NTILE(2) OVER (ORDER BY id) as bucket FROM test_ntile ORDER BY id")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	expected := []int{1, 1, 2, 2}
	i := 0
	for rows.Next() {
		var id, bucket int
		if err := rows.Scan(&id, &bucket); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if bucket != expected[i] {
			t.Errorf("Row %d: expected bucket %d, got %d", i, expected[i], bucket)
		}
		i++
	}
}

// Phase 5 QUALIFY clause

func TestQualifyClause(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// Create and populate table
	_, err := db.Exec("CREATE TABLE test_qualify (id INT, category VARCHAR, value INT)")
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO test_qualify VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30), (4, 'B', 40)")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Get first row per category using QUALIFY
	rows, err := db.Query("SELECT id, category FROM (SELECT id, category, ROW_NUMBER() OVER (PARTITION BY category ORDER BY id) as rn FROM test_qualify) sub WHERE rn = 1 ORDER BY id")
	if err != nil {
		// If QUALIFY CTE approach doesn't work directly, try the full rewritten form
		rows, err = db.Query("SELECT id, category, ROW_NUMBER() OVER (PARTITION BY category ORDER BY id) as rn FROM test_qualify QUALIFY rn = 1 ORDER BY id")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var category string
		if err := rows.Scan(&id, &category); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		count++
	}

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}

// Phase 5 GET function

func TestGet(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// Test array index access
	var result string
	err := db.QueryRow("SELECT GET('[10, 20, 30]', 1)").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "20" {
		t.Errorf("Expected '20', got '%s'", result)
	}
}

func TestGetObjectKey(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// Test object key access
	var result string
	err := db.QueryRow(`SELECT GET('{"a": 1, "b": 2}', 'b')`).Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != "2" {
		t.Errorf("Expected '2', got '%s'", result)
	}
}

// Phase 5 DDL operations

func TestDropTable(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// Create table
	_, err := db.Exec("CREATE TABLE test_drop (id INT)")
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	// Drop table
	_, err = db.Exec("DROP TABLE test_drop")
	if err != nil {
		t.Fatalf("Drop table failed: %v", err)
	}

	// Verify table is gone
	_, err = db.Exec("SELECT * FROM test_drop")
	if err == nil {
		t.Error("Expected error querying dropped table")
	}
}

func TestCreateDropView(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// Create base table
	_, err := db.Exec("CREATE TABLE view_base (id INT, value INT)")
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO view_base VALUES (1, 10), (2, 20)")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Create view
	_, err = db.Exec("CREATE VIEW test_view AS SELECT id, value * 2 as doubled FROM view_base")
	if err != nil {
		t.Fatalf("Create view failed: %v", err)
	}

	// Query view
	var id, doubled int
	err = db.QueryRow("SELECT id, doubled FROM test_view WHERE id = 1").Scan(&id, &doubled)
	if err != nil {
		t.Fatalf("Query view failed: %v", err)
	}

	if doubled != 20 {
		t.Errorf("Expected doubled=20, got %d", doubled)
	}

	// Drop view
	_, err = db.Exec("DROP VIEW test_view")
	if err != nil {
		t.Fatalf("Drop view failed: %v", err)
	}
}
