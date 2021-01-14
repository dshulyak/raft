.PHONY: cover
cover:
	go test ./... -coverprofile=cover.html
	go tool cover -html=cover.html

.PHONY: clean
clean:
	git clean -Xqf
