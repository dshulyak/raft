.PHONY: devmod
devmod:
	go mod edit -replace github.com/dshulyak/raftlog=../raftlog

.PHONY: undevmod
undevmod:
	go mod edit -dropreplace=github.com/dshulyak/raftlog

.PHONY: cover
cover:
	go test ./... -coverprofile=cover.html
	go tool cover -html=cover.html

.PHONY: clean
clean:
	git clean -Xqf
