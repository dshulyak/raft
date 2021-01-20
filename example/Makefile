SHELL = bash

IMAGE ?= yashulyak/raft-kv
VERSION ?= $(shell git rev-parse --short HEAD)

.PHONY: build
build:
	go build -o ./build/kv ./kv

.PHONY: test-setup
test-setup:
	rm -rf /tmp/raft*
	mkdir -p /tmp/raft{1,2,3}

.PHONY: image
image: build
	docker build -t ${IMAGE}:$(VERSION) -f Dockerfile .
	docker tag ${IMAGE}:$(VERSION) ${IMAGE}:latest

.PHONY: push
push:
	docker push ${IMAGE}:$(VERSION)
	docker push ${IMAGE}:latest

.PHONY: release
release: build push
