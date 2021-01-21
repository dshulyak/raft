SHELL = bash

IMAGE ?= yashulyak/raft-kv
VERSION ?= $(shell git rev-parse --short HEAD)

K8S_NAME ?= raft-kv

HELM ?= helm

.PHONY: build
build:
	go build -o ./build/kv ./kv

.PHONY: test-setup
test-setup:
	rm -rf /tmp/raft*
	mkdir -p /tmp/raft{1,2,3}

.PHONY: image
image:
	# context is set to ../. because of the module files (go.mod/go.sum)
	# gopls doesn't support nested modules, so i decided to keep one
	docker build -t ${IMAGE}:${VERSION} -f Dockerfile ../.
	docker tag ${IMAGE}:${VERSION} ${IMAGE}:latest

.PHONY: push
push:
	docker push ${IMAGE}:${VERSION}
	docker push ${IMAGE}:latest

.PHONY: release
release: image push

.PHONY: install
install:
	${HELM} install --name ${K8S_NAME} --set image.tag=${VERSION} ./helm/


.PHONY: purge
purge:
	${HELM} del --purge ${K8S_NAME}
