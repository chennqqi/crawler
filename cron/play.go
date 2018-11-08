package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"
)

func main() {
	go func() {
		for {
			fmt.Println("hello world")
			time.Sleep(time.Second)
		}
	}()
	log.Fatal(http.ListenAndServe("0.0.0.0:80", nil))
}
