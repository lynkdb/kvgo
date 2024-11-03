# Copyright 2015 Eryx <evorui at gmail dot com>, All rights reserved.

EXE_SERVER = bin/kvgod
EXE_CLI = bin/kvgo
APP_HOME = /opt/lynkdb/kvgo
APP_USER = kvgo

PROTOC_CMD = protoc
PROTOC_ARGS = --proto_path=./api/ --go_opt=paths=source_relative --go_out=./pkg/kvapi --go-grpc_out=./pkg/kvapi ./api/*.proto

LYNKX_FITTER_CMD = lynkx-fitter
LYNKX_FITTER_ARGS = pkg/kvapi

.PHONY: server server-run cli cli-install cli-run install test api clean code-stats

all: server cli
	@echo ""
	@echo "build complete"
	@echo ""

server:
	go build -trimpath -o ${EXE_SERVER} cmd/server/main.go

cli:
	go build -trimpath -ldflags="-s -w" -tags="disable_storage" -o ${EXE_CLI} cmd/cli/main.go

cli-install: cli
	mkdir -p ${APP_HOME}/bin
	install -m 755 ${EXE_CLI} ${APP_HOME}/${EXE_CLI}

cli-run: cli
	${EXE_CLI} etc/local.toml

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
	systemctl restart kvgo

test:
	go test -count=1 ./internal/server -v

code-stats:
	find ./pkg -type f -name "*.go" -not -path "*_test2.go" -not -path "*.pb.go" | xargs wc -l|sort -n
	find ./internal -type f -name "*.go" -not -path "*_test2.go" -not -path "*.pb.go" | xargs wc -l|sort -n

api:
	##  go install github.com/golang/protobuf/protoc-gen-go
	##  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc
	##  go install github.com/hooto/htoml4g/cmd/htoml-tag-fix
	go install github.com/lynkdb/lynkx/cmd/lynkx-fitter
	$(PROTOC_CMD) $(PROTOC_ARGS)
	$(LYNKX_FITTER_CMD) $(LYNKX_FITTER_ARGS)

clean:
	rm -f ${EXE_SERVER}

