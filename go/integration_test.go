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
