package main

import (
	"fmt"

	"github.com/hooto/hflag4g/hflag"
	"github.com/lynkdb/kvgo"
)

var (
	addr      = "127.0.0.1:9100"
	accessKey = kvgo.NewSystemAccessKey()
	Server    *kvgo.Conn
	tlsCert   *kvgo.ConfigTLSCertificate
	err       error
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
		AccessKey:   accessKey,
		AuthTLSCert: tlsCert,
	}); err != nil {
		return err
	}

	return nil
}

func client() {

	clientConfig := kvgo.ClientConfig{
		Addr:        addr,
		AccessKey:   accessKey,
		AuthTLSCert: tlsCert,
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
