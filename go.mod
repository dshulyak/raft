module github.com/dshulyak/raft

go 1.15

require (
	github.com/dshulyak/raftlog v0.0.0-20201227120234-30a9f7967948
	github.com/stretchr/testify v1.6.1
	github.com/tysonmote/gommap v0.0.0-20201017170033-6edfc905bae0
	go.uber.org/zap v1.16.0
	golang.org/x/mod v0.3.1-0.20200828183125-ce943fd02449 // indirect
	golang.org/x/sync v0.0.0-20201207232520-09787c993a3a
	golang.org/x/tools v0.0.0-20200207183749-b753a1ba74fa // indirect
	pgregory.net/rapid v0.4.4
)

replace github.com/dshulyak/raftlog => ../raftlog
