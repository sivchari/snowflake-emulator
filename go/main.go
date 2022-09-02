package main

import (
	"io"
	"log"
	"net/http"
)

func main() {
	resp, err := http.Get("http://127.0.0.1:8000")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	log.Println(string(body))
}
