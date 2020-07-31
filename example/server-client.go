package main

import (
	"fmt"

	hauth "github.com/hooto/hauth/go/hauth/v1"
	"github.com/hooto/hflag4g/hflag"

	"github.com/lynkdb/kvgo"
)

var (
	addr    = "127.0.0.1:9100"
	authKey = &hauth.AuthKey{
		AccessKey: "00000000",
		SecretKey: "9ABtTYi9qN63/8T+n1jtLWllVWoKsJeOAwR7vzZ3ch42MiCw",
	}
	Server  *kvgo.Conn
	tlsCert *kvgo.ConfigTLSCertificate
	err     error
)

func main() {

	if _, ok := hflag.ValueOK("tls_enable"); ok {
		// openssl genrsa -out server.key 2048
		// openssl req -new -x509 -sha256 -key server.key -out server.crt -days 3650 -subj '/CN=CommonName'
		// openssl x509 -in server.crt -noout -text
		tlsCert = &kvgo.ConfigTLSCertificate{
			ServerKeyFile:  "server.key",
			ServerCertFile: "server.crt",
		}
	}

	if err := startServer(); err != nil {
		panic(err)
	}

	client()
}

func startServer() error {

	if Server, err = kvgo.Open(kvgo.ConfigStorage{
		DataDirectory: "/tmp/kvgo-server",
	}, kvgo.ConfigServer{
		Bind:        addr,
		AuthKey:     authKey,
		AuthTLSCert: tlsCert,
	}); err != nil {
		return err
	}

	return nil
}

func client() {

	db, err := kvgo.Open(kvgo.ConfigCluster{
		MainNodes: []*kvgo.ClientConfig{
			{
				Addr:    addr,
				AuthKey: authKey,
			},
		},
	}, kvgo.ConfigServer{
		AuthTLSCert: tlsCert,
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
