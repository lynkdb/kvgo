#!/bin/bash

# set -e

docker stop test-n1s4
docker rm test-n1s4

sleep 1

rm -fr tempdir

mkdir -p tempdir/sys/bin
mkdir -p tempdir/sys/etc
mkdir -p tempdir/sys/var/data
mkdir -p tempdir/sys/var/log

cp server.toml tempdir/sys/etc/kvgo-server.toml 

for ((n = 1; n <= 4; n++)); do 
  siz=$[10 + ${n}]

  docker volume rm n1s4-s${n}
  docker volume create --opt type=tmpfs --opt device=tmpfs --opt o=size=${siz}G,uid=1000 n1s4-s${n}
done

GOOS=linux GOARCH=amd64 go build -o tempdir/sys/bin/kvgo-server ../../cmd/server/main.go

docker run -d --name test-n1s4 \
  --memory=16g --cpus=4 \
  -p 0.0.0.0:9566:9566 \
  -p 0.0.0.0:9567:9567 \
  -v ./tempdir/sys:/opt/kvgo \
  --mount 'type=volume,src=n1s4-s1,dst=/data/n1s4-s1,volume-driver=local' \
  --mount 'type=volume,src=n1s4-s2,dst=/data/n1s4-s2,volume-driver=local' \
  --mount 'type=volume,src=n1s4-s3,dst=/data/n1s4-s3,volume-driver=local' \
  --mount 'type=volume,src=n1s4-s4,dst=/data/n1s4-s4,volume-driver=local' \
  --ulimit nofile=10000:10000 alpine:3.19 /opt/kvgo/bin/kvgo-server -log_dir /opt/kvgo/var/log -minloglevel 1 -logtolevel true

