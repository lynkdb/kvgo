package main

import (
	"fmt"

	"github.com/lynkdb/kvgo"
)

func main() {

	db, err := kvgo.Open(kvgo.ConfigStorage{
		DataDirectory: "/tmp/kvgo-demo",
	})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if rs := db.NewWriter([]byte("key"), []byte("value")).Commit(); rs.OK() {
		fmt.Println("OK")
	} else {
		fmt.Println("ER", rs.Message)
	}
}
