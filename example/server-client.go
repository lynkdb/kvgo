package main

import (
	"fmt"

	"github.com/lynkdb/kvgo"
)

var (
	addr          = "127.0.0.1:9100"
	authSecretKey = "9ABtTYi9qN63/8T+n1jtLWllVWoKsJeOAwR7vzZ3ch42MiCw"
)

func main() {

	if err := startServer(); err != nil {
		panic(err)
	}

	client()
}

func startServer() error {

	_, err := kvgo.Open(kvgo.ConfigStorage{
		DataDirectory: "/tmp/kvgo-server",
	}, kvgo.ConfigServer{
		Bind:          addr,
		AuthSecretKey: authSecretKey,
	})
	if err != nil {
		return err
	}

	return nil
}

func client() {

	db, err := kvgo.Open(kvgo.ConfigCluster{
		Masters: []kvgo.ConfigClusterMaster{
			{
				Addr:          addr,
				AuthSecretKey: authSecretKey,
			},
		},
	})
	if err != nil {
		panic(err)
	}

	if rs := db.NewWriter([]byte("demo-key"), []byte("demo-value")).Commit(); rs.OK() {
		fmt.Println("OK")
	} else {
		fmt.Println("ER", rs.Message)
	}
}
