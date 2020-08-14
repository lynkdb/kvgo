package main

import (
	"fmt"

	"github.com/lynkdb/kvgo"
)

var (
	accessKey = kvgo.NewSystemAccessKey()
	mainNodes = []*kvgo.ClientConfig{
		{
			Addr:      "127.0.0.1:9101",
			AccessKey: accessKey,
		},
		{
			Addr:      "127.0.0.1:9102",
			AccessKey: accessKey,
		},
		{
			Addr:      "127.0.0.1:9103",
			AccessKey: accessKey,
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
			Bind:      m.Addr,
			AccessKey: accessKey,
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

	clientConfig := kvgo.ClientConfig{
		Addr:      "127.0.0.1:9102",
		AccessKey: accessKey,
	}

	client, err := clientConfig.NewClient()
	if err != nil {
		panic(err)
	}

	if rs := client.NewWriter([]byte("demo-key"), []byte("demo-value")).Commit(); rs.OK() {
		fmt.Println("OK")
	} else {
		fmt.Println("ER", rs.Message)
	}
}
