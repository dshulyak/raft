module github.com/dshulyak/raft

go 1.15

require (
	github.com/dshulyak/raftlog v0.0.0-20201227120234-30a9f7967948
	github.com/stretchr/testify v1.6.1
	github.com/tysonmote/gommap v0.0.0-20201017170033-6edfc905bae0
	go.uber.org/zap v1.16.0
	pgregory.net/rapid v0.4.4
)

replace github.com/dshulyak/raftlog => ../raftlog
