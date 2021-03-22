package kvraft

import (
	"crypto/rand"
	"fmt"
	"log"
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
	ID       string
	ClientID string
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
	ID       string
	ClientID string
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
	ProcessedMsg map[string]string
}

func uuid() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal(err)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
