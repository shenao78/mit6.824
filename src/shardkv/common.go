package shardkv

import (
	"sync/atomic"

	"../shardmaster"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNotTakeOver = "ErrNotTakeOver"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	ID        int32
	ClientID  int32
	ConfigNum int
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ID        int32
	ClientID  int32
	ConfigNum int
	Key       string
}

type GetReply struct {
	Err   Err
	Value string
}

type SnapshotData struct {
	Store          map[string]string
	ProcessedMsg   map[int32]UniMsg
	Config         *shardmaster.Config
	ClientID       int32
}

var clientID int32 = 1
var reqID int32 = 1

func newReqID() int32 {
	return atomic.AddInt32(&reqID, 1)
}

func newClientID() int32 {
	return atomic.AddInt32(&clientID, 1)
}
