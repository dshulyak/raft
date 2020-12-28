.PHONY: devmod
devmod:
	go mod edit -replace github.com/dshulyak/raftlog=../raftlog

.PHONY: undevmod
undevmod:
	go mod edit -dropreplace=github.com/dshulyak/raftlog
