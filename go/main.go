package main

import (
	"database/sql"
	"log"

	sf "github.com/snowflakedb/gosnowflake"
)

func main() {
	cfg := &sf.Config{
		Account:  "account",
		User:     "user",
		Password: "password",
		Database: "database",
		Schema:   "scheme",
		Protocol: "http",
		Host:     "localhost",
		Port:     8000,
	}
	dsn, _ := sf.DSN(cfg)
	db, _ := sql.Open("snowflake", dsn)
	if err := db.Ping(); err != nil {
		panic(err)
	}
	rows, err := db.Query("SELECT id, name FROM personal2")
	if err != nil {
		panic(err)
	}
	type dest struct {
		ID   int
		Name string
	}

	var d dest
	for rows.Next() {
		if err := rows.Scan(&d.ID, &d.Name); err != nil {
			log.Println(err)
			break
		}
	}

	log.Println(d)

	log.Println("end")
}
