#!/bin/bash

# set -e

docker stop test-n1s4
docker rm test-n1s4

sleep 1

for ((n = 1; n <= 4; n++)); do 
  umount tempdir/n1s4-s${n}_vol
done

rm -fr tempdir

mkdir -p tempdir/sys/bin
mkdir -p tempdir/sys/etc
mkdir -p tempdir/sys/var/data
mkdir -p tempdir/sys/var/log

cp server.toml tempdir/sys/etc/kvgo-server.toml 

for ((n = 1; n <= 4; n++)); do 
  siz=$[10 + ${n}]

  truncate -s ${siz}G tempdir/n1s4-s${n}_blk
  mkfs.xfs tempdir/n1s4-s${n}_blk

  mkdir -p tempdir/n1s4-s${n}_vol
  mount tempdir/n1s4-s${n}_blk tempdir/n1s4-s${n}_vol
done

GOOS=linux GOARCH=amd64 go build -o tempdir/sys/bin/kvgo-server ../../cmd/server/main.go

docker run -d --name test-n1s4 \
  --memory=16g --cpus=4 \
  -p 0.0.0.0:9566:9566 \
  -p 0.0.0.0:9567:9567 \
  -v ./tempdir/sys:/opt/kvgo \
  -v ./tempdir/n1s4-s1_vol:/data/n1s4-s1 \
  -v ./tempdir/n1s4-s2_vol:/data/n1s4-s2 \
  -v ./tempdir/n1s4-s3_vol:/data/n1s4-s3 \
  -v ./tempdir/n1s4-s4_vol:/data/n1s4-s4 \
  --ulimit nofile=10000:10000 alpine:3.19 /opt/kvgo/bin/kvgo-server -log_dir /opt/kvgo/var/log -minloglevel 1 -logtolevel true

