SHELL = bash

.PHONY: build
build:
	mkdir -p build/
	go build -o build/kv ./kv

.PHONY: clean
clean:
	rm -rf /tmp/raft*
	mkdir -p /tmp/raft{1,2,3}
