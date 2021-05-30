PKG_PREFIX := github.com/faceair/VictoriaTraces

BUILDINFO_TAG ?= $(shell echo $$(git describe --long --all | tr '/' '-')$$( \
	      git diff-index --quiet HEAD -- || echo '-dirty-'$$(git diff-index -u HEAD | openssl sha1 | cut -c 10-17)))

GO_BUILDINFO = -X '$(PKG_PREFIX)/lib/buildinfo.Version=$(APP_NAME)-$(shell date -u +'%Y%m%d-%H%M%S')-$(BUILDINFO_TAG)'

vmstorage-amd64:
	APP_NAME=vmstorage
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -ldflags "$(GO_BUILDINFO)" -o bin/vmstorage-amd64 ./app/vmstorage

jaeger-agent:
	cd app/jaeger-agent && go build . && mv ./jaeger-agent ../../bin

run-jaeger-agent: jaeger-agent
	SPAN_STORAGE_TYPE=grpc-plugin bin/jaeger-all-in-one --grpc-storage-plugin.binary bin/jaeger-agent --grpc-storage-plugin.configuration-file app/jaeger-agent/jaeger-agent.yaml --grpc-storage-plugin.log-level debug
