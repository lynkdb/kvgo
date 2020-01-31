package main

import (
	"fmt"
	"os/exec"

	"github.com/hooto/hflag4g/hflag"
	"github.com/lynkdb/kvgo"
	kvbench "github.com/lynkdb/lynkbench/kvbench/v1"
)

var (
	dir          = "/tmp/kvgo-bench"
	err          error
	compressName = ""
)

func main() {

	if v, ok := hflag.ValueOK("data_dir"); ok {
		dir = v.String()
	}

	if v, ok := hflag.ValueOK("compress"); ok {
		compressName = v.String()
	}

	mode := hflag.Value("mode").String()

	switch mode {
	case "embed":
		if err := benchEmbedAction(); err != nil {
			panic(err)
		}

	case "node-x1":
		if err := benchNodeAction(1); err != nil {
			panic(err)
		}

	case "node-x3":
		if err := benchNodeAction(3); err != nil {
			panic(err)
		}

	case "chart":
		if err := kvbench.ChartOutput(); err != nil {
			panic(err)
		}

	default:
		fmt.Println("invalid mode")
	}
}

func benchEmbedAction() error {

	kvBench, err := kvbench.NewKeyValueBench()
	if err != nil {
		return err
	}

	bc := &benchEmbed{}

	return kvBench.Run(bc)
}

func benchNodeAction(n int) error {

	kvBench, err := kvbench.NewKeyValueBench()
	if err != nil {
		return err
	}

	bc := &benchNode{
		nodeNum: n,
	}

	return kvBench.Run(bc)
}

type benchEmbed struct {
	db *kvgo.Conn
}

func (it *benchEmbed) Attrs() []string {
	return []string{"embed"}
}

func (it *benchEmbed) Write(k, v []byte) kvbench.ResultStatus {
	if rs := it.db.NewWriter(k, v).Commit(); rs.OK() {
		return kvbench.ResultOK
	}
	return kvbench.ResultERR
}

func (it *benchEmbed) Read(k []byte) kvbench.ResultStatus {
	if rs := it.db.NewReader(k).Query(); rs.OK() {
		return kvbench.ResultOK
	}
	return kvbench.ResultERR
}

func (it *benchEmbed) Clean() error {

	if it.db != nil {
		it.db.Close()
	}

	sdir := fmt.Sprintf("%s/embed", dir)

	exec.Command("rm", "-rf", sdir).Output()

	it.db, err = kvgo.Open(kvgo.ConfigStorage{
		DataDirectory: sdir,
	}, kvgo.ConfigPerformance{
		WriteBufferSize: 64,
		BlockCacheSize:  64,
		MaxTableSize:    16,
		MaxOpenFiles:    1000,
	}, kvgo.ConfigFeature{
		TableCompressName: compressName,
	})

	return err
}

var (
	addr          = "127.0.0.1:6%04d"
	authSecretKey = "9ABtTYi9qN63/8T+n1jtLWllVWoKsJeOAwR7vzZ3ch42MiCw"
)

type benchNode struct {
	nodeNum   int
	dbServers []*kvgo.Conn
	db        *kvgo.Conn
}

func (it *benchNode) Attrs() []string {
	return []string{
		fmt.Sprintf("node-x%d", it.nodeNum),
	}
}

func (it *benchNode) Write(k, v []byte) kvbench.ResultStatus {
	if rs := it.db.NewWriter(k, v).Commit(); rs.OK() {
		return kvbench.ResultOK
	}
	return kvbench.ResultERR
}

func (it *benchNode) Read(k []byte) kvbench.ResultStatus {
	if rs := it.db.NewReader(k).Query(); rs.OK() {
		return kvbench.ResultOK
	}
	return kvbench.ResultERR
}

func (it *benchNode) Clean() error {

	if it.db != nil {
		it.db.Close()
	}

	if it.nodeNum < 1 {
		it.nodeNum = 1
	} else if it.nodeNum > 7 {
		it.nodeNum = 7
	}

	for _, vdb := range it.dbServers {
		vdb.Close()
	}

	it.dbServers = nil

	masters := []kvgo.ConfigClusterMaster{}

	for i := 0; i < it.nodeNum; i++ {
		masters = append(masters, kvgo.ConfigClusterMaster{
			Addr:          fmt.Sprintf(addr, i),
			AuthSecretKey: authSecretKey,
		})
	}

	cfgCC := kvgo.ConfigCluster{}
	if len(masters) > 1 {
		cfgCC.Masters = masters
	}

	for i := 0; i < it.nodeNum; i++ {

		sdir := fmt.Sprintf("%s/%s_n%d", dir, compressName, i)

		exec.Command("rm", "-rf", sdir).Output()

		dbServer, err := kvgo.Open(kvgo.ConfigStorage{
			DataDirectory: sdir,
		}, kvgo.ConfigServer{
			Bind:          fmt.Sprintf(addr, i),
			AuthSecretKey: authSecretKey,
		}, kvgo.ConfigPerformance{
			WriteBufferSize: 64,
			BlockCacheSize:  64,
			MaxTableSize:    16,
			MaxOpenFiles:    1000,
		}, kvgo.ConfigFeature{
			TableCompressName: compressName,
		}, cfgCC)
		if err != nil {
			return err
		}

		it.dbServers = append(it.dbServers, dbServer)
	}

	it.db, err = kvgo.Open(kvgo.ConfigCluster{
		Masters: masters,
	})

	return err
}
