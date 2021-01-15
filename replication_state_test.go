package raft

import (
	"testing"

	"github.com/dshulyak/raft/raftlog"
	"github.com/stretchr/testify/require"
)

func TestPeerReplicationFromScratch(t *testing.T) {
	cluster := getTestCluster(t)

	peer := cluster.replicationPeer(1, 3, 1)
	msg := &AppendEntries{
		Term:    1,
		Leader:  1,
		Entries: make([]*raftlog.LogEntry, 3),
	}
	for i := 1; i <= len(msg.Entries); i++ {
		msg.Entries[i-1] = &raftlog.LogEntry{
			Term:   msg.Term,
			Index:  uint64(i),
			OpType: raftlog.LogApplication,
		}
	}
	last := msg.Entries[len(msg.Entries)-1]
	follower := NodeID(2)
	cluster.runReplication(peer, follower, msg)

	cluster.compareMsgHistories([]interface{}{
		msg,
		&AppendEntriesResponse{
			Term:     msg.Term,
			Follower: follower,
			Success:  true,
			LastLog: LogHeader{
				Index: last.Index,
				Term:  last.Term,
			},
		},
	})
}

func TestPeerReplicationOutdated(t *testing.T) {
	cluster := getTestCluster(t)
	peerID := NodeID(1)
	batch := uint64(20)
	peer := cluster.replicationPeer(peerID, batch, 1)
	log := cluster.logs[peerID]
	entries := []*raftlog.LogEntry{}
	n := 10

	for i := 1; i <= n; i++ {
		entry := &raftlog.LogEntry{
			Term:   1,
			Index:  uint64(i),
			OpType: raftlog.LogNoop,
		}
		require.NoError(t, log.Append(entry))
		entries = append(entries, entry)
	}
	require.NoError(t, log.Sync())

	msg := &AppendEntries{
		Term:    1,
		Leader:  peerID,
		PrevLog: LogHeader{Term: 1, Index: uint64(n)},
	}
	follower := NodeID(2)
	cluster.runReplication(peer, follower, msg)

	history := []interface{}{msg, &AppendEntriesResponse{
		Term:     1,
		Follower: follower,
		Success:  false,
	}}
	for i := uint64(9); i > 0; i-- {
		history = append(history,
			&AppendEntries{
				Term:    1,
				Leader:  peerID,
				PrevLog: LogHeader{Term: 1, Index: uint64(i)},
				Entries: entries[i:],
			},
			&AppendEntriesResponse{
				Term:     1,
				Follower: follower,
			},
		)
	}
	history = append(history,
		&AppendEntries{
			Term:    1,
			Leader:  peerID,
			PrevLog: LogHeader{Term: 0, Index: 0},
			Entries: entries,
		},
		&AppendEntriesResponse{
			Term:     1,
			Follower: follower,
			Success:  true,
			LastLog: LogHeader{
				Term: 1, Index: uint64(n),
			},
		},
	)
	cluster.compareMsgHistories(history)
}

func TestPeerSendHeartbeat(t *testing.T) {
	cluster := getTestCluster(t)
	peer := cluster.replicationPeer(1, 1, 1)
	require.Nil(t, peer.next())
	peer.tick(1)
	hb := peer.next()
	require.NotNil(t, hb)
	require.Len(t, hb.Entries, 0)
}
