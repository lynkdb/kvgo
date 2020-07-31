package main

import (
	"fmt"

	hauth "github.com/hooto/hauth/go/hauth/v1"

	"github.com/lynkdb/kvgo"
)

var (
	authKey = &hauth.AuthKey{
		AccessKey: "00000000",
		SecretKey: "9ABtTYi9qN63/8T+n1jtLWllVWoKsJeOAwR7vzZ3ch42MiCw",
	}
	mainNodes = []*kvgo.ClientConfig{
		{
			Addr:    "127.0.0.1:9101",
			AuthKey: authKey,
		},
		{
			Addr:    "127.0.0.1:9102",
			AuthKey: authKey,
		},
		{
			Addr:    "127.0.0.1:9103",
			AuthKey: authKey,
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

	for i, m := range mainNodes {

		_, err := kvgo.Open(kvgo.ConfigStorage{
			DataDirectory: fmt.Sprintf("/tmp/kvgo-cluster-%d", i),
		}, kvgo.ConfigServer{
			Bind:    m.Addr,
			AuthKey: authKey,
		}, kvgo.ConfigCluster{
			MainNodes: mainNodes,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func client() {

	db, err := kvgo.Open(kvgo.ConfigCluster{
		MainNodes: mainNodes,
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
