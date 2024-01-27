#!/bin/bash

# set -e

docker stop test-n1s4

GOOS=linux GOARCH=amd64 go build -o tempdir/sys/bin/kvgo-server ../../cmd/server/main.go

docker start test-n1s4

