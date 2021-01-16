.PHONY: cover

GOPATH ?= $(HOME)/go

cover:
	go test ./... -coverprofile=cover.html
	go tool cover -html=cover.html

.PHONY: clean
clean:
	git clean -Xqf

.PHONY: protoc
protoc:
	protoc -I=$(GOPATH)/src/ -I=. --gogofaster_out=paths=source_relative:. types/types.proto
	protoc -I=$(GOPATH)/src/ -I=. --gogofaster_out=plugins=grpc:. transport/grpcstream/pb/stream.proto
