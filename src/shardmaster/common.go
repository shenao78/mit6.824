package shardmaster

import (
	"crypto/rand"
	"fmt"
	"log"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

const (
	JOIN  = "JOIN"
	LEAVE = "LEAVE"
	MOVE  = "MOVE"
	QUERY = "QUERY"
)

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (c Config) copyGroups() map[int][]string {
	groups := make(map[int][]string)
	for gid, servers := range c.Groups {
		groups[gid] = servers
	}
	return groups
}

type JoinArgs struct {
	ID       string
	ClientID string
	Servers  map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
}

type LeaveArgs struct {
	ID       string
	ClientID string
	GIDs     []int
}

type LeaveReply struct {
	WrongLeader bool
}

type MoveArgs struct {
	ID       string
	ClientID string
	Shard    int
	GID      int
}

type MoveReply struct {
	WrongLeader bool
}

type QueryArgs struct {
	ID       string
	ClientID string
	Num      int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Config      Config
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
