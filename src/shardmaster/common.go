package shardmaster

import (
	"sync/atomic"
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

func (c Config) copy() Config {
	var shards [NShards]int
	for s, g := range c.Shards {
		shards[s] = g
	}

	groups := make(map[int][]string)
	for g, s := range c.Groups {
		groups[g] = s
	}

	return Config{
		Num:    c.Num,
		Shards: shards,
		Groups: groups,
	}
}

func (c *Config) reloadBalance() {
	c.Shards = reloadBalance(c.toShardsList(), c.orphanShards())
}

type gidToShards struct {
	gid    int
	shards []int
}

func (c *Config) toShardsList() []*gidToShards {
	gidToShardsMap := make(map[int][]int)
	for shard, gid := range c.Shards {
		if _, ok := c.Groups[gid]; ok {
			gidToShardsMap[gid] = append(gidToShardsMap[gid], shard)
		}
	}

	for gid := range c.Groups {
		if _, ok := gidToShardsMap[gid]; !ok {
			gidToShardsMap[gid] = []int{}
		}
	}

	var gidToShardsList []*gidToShards
	for gid, shards := range gidToShardsMap {
		gidToShardsList = append(gidToShardsList, &gidToShards{gid: gid, shards: shards})
	}
	return gidToShardsList
}

func (c *Config) orphanShards() []int {
	var orphanShards []int
	for shard, gid := range c.Shards {
		if _, ok := c.Groups[gid]; !ok {
			orphanShards = append(orphanShards, shard)
		}
	}

	return orphanShards
}

type JoinArgs struct {
	ID       int32
	ClientID int32
	Servers  map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
}

type LeaveArgs struct {
	ID       int32
	ClientID int32
	GIDs     []int
}

type LeaveReply struct {
	WrongLeader bool
}

type MoveArgs struct {
	ID       int32
	ClientID int32
	Shard    int
	GID      int
}

type MoveReply struct {
	WrongLeader bool
}

type QueryArgs struct {
	ID       int32
	ClientID int32
	Num      int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Config      Config
}

var clientID int32 = 1
var reqID int32 = 1

func newReqID() int32 {
	return atomic.AddInt32(&reqID, 1)
}

func newClientID() int32 {
	return atomic.AddInt32(&clientID, 1)
}
