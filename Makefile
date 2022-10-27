

.PHONY: proto
proto:
	protoc -I ./proto_idl --go_out=./gen ./proto_idl/nobu.proto