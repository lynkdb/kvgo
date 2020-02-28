package main

import (
	"fmt"

	"github.com/lynkdb/kvgo"
)

var (
	authSecretKey = "9ABtTYi9qN63/8T+n1jtLWllVWoKsJeOAwR7vzZ3ch42MiCw"
	masters       = []*kvgo.ConfigClusterMaster{
		{
			Addr:          "127.0.0.1:9101",
			AuthSecretKey: authSecretKey,
		},
		{
			Addr:          "127.0.0.1:9102",
			AuthSecretKey: authSecretKey,
		},
		{
			Addr:          "127.0.0.1:9103",
			AuthSecretKey: authSecretKey,
		},
	}
)

func main() {

	if err := startCluster(); err != nil {
		panic(err)
	}

	client()
}

func startCluster() error {

	for i, m := range masters {

		_, err := kvgo.Open(kvgo.ConfigStorage{
			DataDirectory: fmt.Sprintf("/tmp/kvgo-cluster-%d", i),
		}, kvgo.ConfigServer{
			Bind:          m.Addr,
			AuthSecretKey: authSecretKey,
		}, kvgo.ConfigCluster{
			Masters: masters,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func client() {

	db, err := kvgo.Open(kvgo.ConfigCluster{
		Masters: masters,
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
