package shardmaster

import (
	"sort"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

type ShardMaster struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	processedMsg map[string]string
	msgRegister  *sync.Map

	configs []Config // indexed by config num
}

type Op struct {
	ID       string
	ClientID string
	OpName   string
	Servers  map[int][]string
	GIDs     []int
	Shard    int
	GID      int
	Num      int
}

type JoinReq struct {
	Servers map[int][]string
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{ID: args.ID, ClientID: args.ClientID, OpName: JOIN, Servers: args.Servers}
	if !sm.startCommand(op) {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) handleJoin(req Op) {
	var orphanShards []int
	gidToShardsMap := make(map[int][]int)
	for shard, gid := range sm.latestConfig().Shards {
		if _, ok := sm.latestConfig().Groups[gid]; !ok {
			orphanShards = append(orphanShards, shard)
		} else {
			gidToShardsMap[gid] = append(gidToShardsMap[gid], shard)
		}
	}

	groups := sm.latestConfig().copyGroups()
	for gid, servers := range req.Servers {
		gidToShardsMap[gid] = []int{}
		groups[gid] = servers
	}

	var gidToShardsList []*gidToShards
	for gid, shards := range gidToShardsMap {
		gidToShardsList = append(gidToShardsList, &gidToShards{gid: gid, shards: shards})
	}

	shards := reloadBalance(gidToShardsList, orphanShards)
	sm.configs = append(sm.configs, Config{
		Num:    len(sm.configs),
		Shards: shards,
		Groups: groups,
	})
}

type LeaveReq struct {
	GIDs []int
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{ID: args.ID, ClientID: args.ClientID, OpName: LEAVE, GIDs: args.GIDs}
	if !sm.startCommand(op) {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) handleLeave(req Op) {
	delGidMap := make(map[int]bool)
	groups := sm.latestConfig().copyGroups()
	for _, gid := range req.GIDs {
		delete(groups, gid)
		delGidMap[gid] = true
	}

	gidToShardsMap := make(map[int][]int)
	var orphanShards []int
	for shard, gid := range sm.latestConfig().Shards {
		if delGidMap[gid] {
			orphanShards = append(orphanShards, shard)
		} else {
			gidToShardsMap[gid] = append(gidToShardsMap[gid], shard)
		}
	}

	for gid := range groups {
		if _, ok := gidToShardsMap[gid]; !ok {
			gidToShardsMap[gid] = []int{}
		}
	}

	var gidToShardsList []*gidToShards
	for gid, shards := range gidToShardsMap {
		gidToShardsList = append(gidToShardsList, &gidToShards{gid: gid, shards: shards})
	}

	shards := reloadBalance(gidToShardsList, orphanShards)
	sm.configs = append(sm.configs, Config{
		Num:    len(sm.configs),
		Shards: shards,
		Groups: groups,
	})
}

type gidToShards struct {
	gid    int
	shards []int
}

func reloadBalance(gidToShardsList []*gidToShards, orphanShards []int) [NShards]int {
	var config [NShards]int
	gidLen := len(gidToShardsList)
	if gidLen == 0 {
		return config
	}

	sort.Slice(gidToShardsList, func(i, j int) bool {
		return len(gidToShardsList[i].shards) > len(gidToShardsList[j].shards)
	})

	shardLen := NShards - len(orphanShards)
	avgShards := shardLen / gidLen
	extraShards := shardLen % gidLen

	for i := 0; i < gidLen-1; i++ {
		shards := gidToShardsList[i].shards
		finalShardsNum := avgShards
		if extraShards > 0 {
			finalShardsNum++
			extraShards--
		}

		if diff := len(shards) - finalShardsNum; diff > 0 {
			endPos := len(shards) - diff
			movShards := shards[endPos:]
			gidToShardsList[i].shards = shards[:endPos]
			gidToShardsList[i+1].shards = append(gidToShardsList[i+1].shards, movShards...)
		}
	}

	i := gidLen - 1
	for _, shard := range orphanShards {
		if i < 0 {
			i += gidLen
		}
		gidToShardsList[i].shards = append(gidToShardsList[i].shards, shard)
		i--
	}

	for _, gidToShards := range gidToShardsList {
		gid := gidToShards.gid
		for _, shard := range gidToShards.shards {
			config[shard] = gid
		}
	}

	return config
}

type MoveReq struct {
	Shard int
	GID   int
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{ID: args.ID, ClientID: args.ClientID, OpName: MOVE, Shard: args.Shard, GID: args.GID}
	if !sm.startCommand(op) {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) handleMove(req Op) {
	latestConfig := sm.latestConfig()
	shards := [NShards]int{}
	copy(shards[:], latestConfig.Shards[:])
	shards[req.Shard] = req.GID

	sm.configs = append(sm.configs, Config{
		Num:    len(sm.configs),
		Shards: shards,
		Groups: latestConfig.copyGroups(),
	})
}

func (sm *ShardMaster) latestConfig() Config {
	return sm.configs[len(sm.configs)-1]
}

type QueryReq struct {
	Num int
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{ID: args.ID, ClientID: args.ClientID, OpName: QUERY, Num: args.Num}
	if !sm.startCommand(op) {
		reply.WrongLeader = true
		return
	}

	num := args.Num
	if args.Num < 0 || args.Num > len(sm.configs) {
		num = len(sm.configs) - 1
	}

	sm.mu.RLock()
	defer sm.mu.RUnlock()

	reply.Config = sm.configs[num]
}

func (sm *ShardMaster) startCommand(cmd Op) bool {
	notifyCh := make(chan raft.ApplyMsg, 1)
	sm.msgRegister.Store(cmd.ID, notifyCh)
	defer func() {
		sm.msgRegister.Delete(cmd.ID)
	}()

	_, term, leader := sm.rf.Start(cmd)
	if !leader {
		return false
	}

	for {
		select {
		case msg := <-notifyCh:
			if msg.CommandTerm > term {
				return false
			} else {
				return true
			}
		case <-time.After(20 * time.Millisecond):
			if sm.rf.MyTerm() > term {
				return false
			}
		}
	}
}

func (sm *ShardMaster) applyCommitLoop() {
	for msg := range sm.applyCh {
		sm.applyCmd(msg)
	}
}

func (sm *ShardMaster) applyCmd(msg raft.ApplyMsg) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := msg.Command.(Op)
	if sm.processedMsg[op.ClientID] != op.ID {
		switch op.OpName {
		case JOIN:
			sm.handleJoin(op)
		case LEAVE:
			sm.handleLeave(op)
		case MOVE:
			sm.handleMove(op)
		}
		sm.processedMsg[op.ClientID] = op.ID
	}

	if val, ok := sm.msgRegister.Load(op.ID); ok {
		notifyCh := val.(chan raft.ApplyMsg)
		select {
		case notifyCh <- msg:
		default:
		}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.processedMsg = make(map[string]string)
	sm.msgRegister = new(sync.Map)

	go sm.applyCommitLoop()
	return sm
}
