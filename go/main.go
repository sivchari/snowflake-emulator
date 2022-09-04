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
	var i int
	rows, err := db.Query("SELECT 1;")
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		if err := rows.Scan(&i); err != nil {
			panic(err)
		}
		log.Println(i)
	}
	log.Println("end")
}
