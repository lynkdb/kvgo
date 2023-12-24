# Copyright 2019 Eryx <evorui at gmail dot com>, All rights reserved.
#

EXE_SERVER = bin/kvgo-server
EXE_CLI = bin/kvgo-cli
APP_HOME = /opt/lynkdb/kvgo
APP_USER = kvgo

PROTOC_CMD = protoc
PROTOC_ARGS = --proto_path=./api/v2/ --go_opt=paths=source_relative --go_out=./pkg/kvapi --go-grpc_out=./pkg/kvapi ./api/v2/*.proto

HTOML_TAG_FIX_CMD = htoml-tag-fix
HTOML_TAG_FIX_ARGS = pkg/kvapi

.PHONY: cli cli-install cli-run install test test-v2 api clean code-stats

all:
	go build -o ${EXE_SERVER} cmd/server/main.go
	@echo ""
	@echo "build complete"
	@echo ""

server:
	go build -o ${EXE_SERVER} cmd/server/main.go

cli:
	go build -o ${EXE_CLI} cmd/cli/main.go

cli-install: cli
	mkdir -p ${APP_HOME}/bin
	install -m 755 ${EXE_CLI} ${APP_HOME}/${EXE_CLI}

cli-run: cli
	${EXE_CLI}

server-run: server
	${EXE_SERVER} -logtostderr true

install: server
	mkdir -p ${APP_HOME}/bin
	mkdir -p ${APP_HOME}/etc
	mkdir -p ${APP_HOME}/var/log
	mkdir -p ${APP_HOME}/var/data
	mkdir -p ${APP_HOME}/init
	cp -rp init/server ${APP_HOME}/init
	install -m 755 ${EXE_SERVER} ${APP_HOME}/${EXE_SERVER}
	id -u ${APP_USER} || useradd -d ${APP_HOME} -s /sbin/nologin ${APP_USER}
	chown -R ${APP_USER}:${APP_USER} ${APP_HOME}
	install -m 600 init/server/systemd/systemd.service /lib/systemd/system/kvgo.service
	systemctl daemon-reload

test:
	go test -count=1 .

code-stats:
	find ./pkg -type f -name "*.go" -not -path "*_test2.go" -not -path "*.pb.go" | xargs wc -l|sort -n
	find ./internal -type f -name "*.go" -not -path "*_test2.go" -not -path "*.pb.go" | xargs wc -l|sort -n

api:
	# go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install github.com/golang/protobuf/protoc-gen-go
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc
	go install github.com/hooto/htoml4g/cmd/htoml-tag-fix
	$(PROTOC_CMD) $(PROTOC_ARGS)
	$(HTOML_TAG_FIX_CMD) $(HTOML_TAG_FIX_ARGS)

clean:
	rm -f ${EXE_SERVER}


