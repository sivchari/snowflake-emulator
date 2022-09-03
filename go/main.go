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
	log.Println(dsn)
	db, _ := sql.Open("snowflake", dsn)
	// log.Println(db.Ping())
	var i int
	rows, err := db.Query("SELECT 1;")
	log.Println("rows")
	log.Println(err)
	for rows.Next() {
		err = rows.Scan(&i)
		log.Println(err)
	}
	log.Println(i)
}
