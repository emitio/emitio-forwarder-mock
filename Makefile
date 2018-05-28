.PHONY: generate
generate:
	protoc -I../emitioapis -I../emitioapis/third_party emitio/v1/span.proto emitio/v1/emitio.proto --go_out=plugins=grpc:./pkg