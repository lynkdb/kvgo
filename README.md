## kvgo

An embeddable, persistent, and distributed key-value database engine, written in Go.

## Features

* Embedded and server-client modes with a unified `kvapi.Client` interface
* Storage engine based on [Pebble](https://github.com/cockroachdb/pebble) (default) or [goleveldb](https://github.com/syndtr/goleveldb)
* Data sorted by key, supporting forward and backward range queries
* Automatic compression using snappy (default) or zstd
* HMAC-based authentication with role-based access control
* Large object storage with block-based chunking
* gRPC-based API with TLS support
* Cluster deployment with built-in replica and sharding support

## Install

```bash
go get github.com/lynkdb/kvgo/v2
```

## Getting Started

### Embedded Mode

```go
package main

import (
    "fmt"

    "github.com/lynkdb/kvgo/v2/pkg/kvapi"
    "github.com/lynkdb/kvgo/v2/pkg/kvrep"
    "github.com/lynkdb/kvgo/v2/pkg/storage"
)

func main() {
    db, err := kvrep.NewReplica(&storage.Options{
        DataDirectory: "/tmp/kvgo-demo",
    })
    if err != nil {
        panic(err)
    }
    defer db.Close()

    if rs := db.NewWriter([]byte("key"), []byte("value")).Exec(); rs.OK() {
        fmt.Println("OK")
    } else {
        fmt.Println("ER", rs.ErrorMessage())
    }
}
```

### Server-Client Mode

Start the server:

```bash
make server
bin/kvgod -logtostderr true
```

Connect with a Go client:

```go
package main

import (
    "fmt"

    "github.com/hooto/hauth/go"
    "github.com/lynkdb/kvgo/v2/pkg/client"
)

func main() {
    c, err := (&client.Config{
        Addr:    "127.0.0.1:9566",
        Database: "test",
        AccessKey: &hauth.AccessKey{
            Id:     "00000000",
            Secret: "<your-secret>",
        },
    }).NewClient()
    if err != nil {
        panic(err)
    }
    defer c.Close()

    if rs := c.NewWriter([]byte("key"), []byte("value")).Exec(); rs.OK() {
        fmt.Println("OK")
    }
}
```

### CLI Tool

```bash
make cli
bin/kvgo etc/local.toml
```

## Data APIs

The `kvapi.Client` interface provides two API styles: direct request and builder pattern.

### Write

```go
// Direct request
rs := db.Write(kvapi.NewWriteRequest([]byte("key"), []byte("value")))

// Builder pattern
rs := db.NewWriter([]byte("key"), []byte("value")).Exec()

// Write only if key does not exist
rs := db.NewWriter([]byte("key"), []byte("value")).SetCreateOnly(true).Exec()

// Set TTL (milliseconds)
rs := db.NewWriter([]byte("key"), []byte("value")).SetTTL(3000).Exec()

// JSON encoding via builder
rs := db.NewWriter([]byte("key"), myStruct).SetJsonValue(myStruct).Exec()

// Conditional write with version check
rs := db.NewWriter([]byte("key"), []byte("new-value")).SetPrevVersion(oldVersion).Exec()

// Auto-increment namespace
rs := db.NewWriter([]byte("key"), []byte("value")).SetIncr(0, "ns").Exec()
if rs.OK() {
    fmt.Println("IncrId:", rs.Meta().IncrId)
}
```

### Read

```go
// Single key
rs := db.Read(kvapi.NewReadRequest([]byte("key")))
if rs.OK() {
    item := rs.Item()
    fmt.Println(item.StringValue())
    fmt.Println(item.Uint64Value())
    fmt.Println(item.Int64Value())
    fmt.Println(item.Float64Value())
    fmt.Println(item.BoolValue())
} else if rs.NotFound() {
    fmt.Println("Not Found")
}

// Builder with meta-only (no value data)
rs := db.NewReader([]byte("key")).SetMetaOnly(true).Exec()

// JSON decode
var obj MyStruct
if rs.OK() {
    rs.Item().JsonDecode(&obj)
}
```

### Range Query

```go
// Forward range
rs := db.Range(kvapi.NewRangeRequest([]byte("a"), []byte("z")).SetLimit(100))
if rs.OK() {
    for _, item := range rs.Items {
        fmt.Printf("Key: %s, Value: %s\n", item.Key, item.StringValue())
    }
}

// Backward range via builder
rs := db.NewRanger([]byte("z"), []byte("a")).SetRevert(true).SetLimit(100).Exec()
```

### Delete

```go
// Direct request
rs := db.Delete(kvapi.NewDeleteRequest([]byte("key")))

// Builder pattern
rs := db.NewDeleter([]byte("key")).Exec()

// Delete with version check
rs := db.NewDeleter([]byte("key")).SetPrevVersion(version).Exec()
```

### Batch

```go
batch := kvapi.NewBatchRequest()
batch.Write([]byte("k1"), []byte("v1"))
batch.Write([]byte("k2"), []byte("v2"))
batch.Read([]byte("k1"))
batch.Delete([]byte("k3"))

resp := db.Batch(batch)
if resp.OK() {
    for i, rs := range resp.Items {
        fmt.Printf("Result %d: OK=%v\n", i, rs.OK())
    }
}
```

### Large Object Storage

```go
import "github.com/lynkdb/kvgo/v2/pkg/object"

oc, _ := object.NewObjectClient(db)

// Upload a file
oc.Put("path/to/remote", "/path/to/local/file")

// Read (returns io.ReadSeeker)
reader, _ := oc.Open("path/to/remote")
```

### Database Selection

```go
// Set default database on client
db2 := db.SetDatabase("mydb")

// Or per-request
rs := db.Write(kvapi.NewWriteRequest([]byte("key"), []byte("value")).SetDatabase("mydb"))
```

## Result Types

All read/write operations return `*ResultSet` with status checking:

```go
rs.OK()              // true if status == 2000
rs.NotFound()        // true if status == 4040
rs.Error()           // error or nil
rs.ErrorMessage()    // "#4000 message"
rs.Item()            // first *KeyValue
rs.Meta()            // first item's *Meta
rs.JsonDecode(&obj)  // JSON decode first item's value
rs.Lookup([]byte("key")) // find item by key in range results
```

`KeyValue` value accessors: `StringValue()`, `Int64Value()`, `Uint64Value()`, `BoolValue()`, `Float64Value()`, `JsonDecode(&obj)`.

## Configuration

Server configuration is TOML-based (`etc/kvgo-server.toml`):

```toml
[storage]
data_directory = "var/data"
engine = "v2"

[[storage.stores]]
uni_id = "store-1"
engine = "v2"
mountpoint = "/data/kvgo"
store_id = 2

[server]
bind = "127.0.0.1:9566"

[server.access_key]
id = "00000000"
secret = "<your-secret>"
status = 1
roles = ["sa"]

[[server.access_key.scopes]]
name = "kvgo/db"
value = "*"

[performance]
write_buffer_size = 8
block_cache_size = 32

[feature]
compression = "snappy"
```

### Storage Options (Embedded Mode)

```go
&storage.Options{
    DataDirectory:   "/path/to/data",
    WriteBufferSize: 8,    // MiB, range 2-256, default 8
    BlockCacheSize:  32,   // MiB, range 4-1024, default 32
    MaxTableSize:    8,    // MiB, range 2-64, default 8
    MaxOpenFiles:    500,  // range 500-10000, default 500
    Compression:     "snappy", // "snappy" or "none"
}
```

## Build

```bash
make all      # Build server (bin/kvgod) and CLI (bin/kvgo)
make server   # Build server only
make cli      # Build CLI only (excludes storage engine)
make test     # Run tests
make api      # Regenerate protobuf code
```

## License

Licensed under the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).
