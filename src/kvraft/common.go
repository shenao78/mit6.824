package kvraft

import (
	"sync/atomic"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type Reply interface {
	Error() Err
}

// Put or Append
type PutAppendArgs struct {
	ID       int32
	ClientID int32
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

func (p *PutAppendReply) Error() Err {
	return p.Err
}

type GetArgs struct {
	ID       int32
	ClientID int32
	Key      string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

func (g *GetReply) Error() Err {
	return g.Err
}

type SnapshotData struct {
	Store        map[string]string
	ProcessedMsg map[int32]int32
}

var clientID int32 = 1
var reqID int32 = 1

func newReqID() int32 {
	return atomic.AddInt32(&reqID, 1)
}

func newClientID() int32 {
	return atomic.AddInt32(&clientID, 1)
}
