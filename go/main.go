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
	rows, err := db.Query(`SELECT 1, 1.1, true, "a", b;`)
	if err != nil {
		panic(err)
	}
	type dest struct {
		a int
		b float64
		c bool
		d string
		e string
	}
	var d dest
	for rows.Next() {
		if err := rows.Scan(
			&d.a,
			&d.b,
			&d.c,
			&d.d,
			&d.e,
		); err != nil {
			panic(err)
		}
	}
	log.Printf("%v", d)

	log.Println("end")
}
