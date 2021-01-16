package raft

import (
	"fmt"

	"github.com/dshulyak/raft/types"
)

func wrap(msg Message) *types.Message {
	switch m := msg.(type) {
	case *types.AppendEntries:
		return &types.Message{Type: &types.Message_Append{Append: m}}
	case *types.AppendEntriesResponse:
		return &types.Message{Type: &types.Message_AppendResp{AppendResp: m}}
	case *types.RequestVote:
		return &types.Message{Type: &types.Message_ReqVote{ReqVote: m}}
	case *types.RequestVoteResponse:
		return &types.Message{Type: &types.Message_ReqVoteResp{ReqVoteResp: m}}
	default:
		panic(fmt.Sprintf("unknown message %+v", msg))
	}
}

func unwrap(msg *types.Message) Message {
	switch m := msg.Type.(type) {
	case *types.Message_Append:
		return m.Append
	case *types.Message_AppendResp:
		return m.AppendResp
	case *types.Message_ReqVote:
		return m.ReqVote
	case *types.Message_ReqVoteResp:
		return m.ReqVoteResp
	default:
		panic(fmt.Sprintf("unknown message %+v", msg))
	}
}
