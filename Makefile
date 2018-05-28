SRCS := $(shell find . -type f -iname '*.go' -not -path './vendor/*')

.PHONY: bin
bin: target/emitio-forwarder-mock_linux_amd64

.PHONY: generate
generate:
	protoc -I../emitioapis -I../emitioapis/third_party emitio/v1/span.proto emitio/v1/emitio.proto --go_out=plugins=grpc:./pkg

target/emitio-forwarder-mock_linux_amd64: $(SRCS)
	go build -o $@ cmd/emitio-forwarder-mock/main.go
