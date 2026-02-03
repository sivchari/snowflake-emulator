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
