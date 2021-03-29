package shardmaster

import (
	"sort"
	"sync"
	"time"

	"fmt"

	"../labgob"
	"../labrpc"
	"../raft"
)

type ShardMaster struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	processedMsg map[int32]int32
	msgRegister  *sync.Map

	configs []Config // indexed by config num
}

type Op struct {
	ID       int32
	ClientID int32
	OpName   string
	Servers  map[int][]string
	GIDs     []int
	Shard    int
	GID      int
	Num      int
}

func (o *Op) id() string {
	return fmt.Sprintf("%d:%d", o.ClientID, o.ID)
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
	config := sm.latestConfig().copy()
	config.Num++

	for gid, servers := range req.Servers {
		config.Groups[gid] = servers
	}

	config.reloadBalance()
	sm.configs = append(sm.configs, config)
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
	config := sm.latestConfig().copy()
	config.Num++

	for _, gid := range req.GIDs {
		delete(config.Groups, gid)
	}

	config.reloadBalance()
	sm.configs = append(sm.configs, config)
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
	config := sm.latestConfig().copy()
	config.Num++
	config.Shards[req.Shard] = req.GID

	sm.configs = append(sm.configs, config)
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

	sm.mu.RLock()
	defer sm.mu.RUnlock()

	num := args.Num
	if args.Num < 0 || args.Num > len(sm.configs) {
		num = len(sm.configs) - 1
	}

	reply.Config = sm.configs[num]
}

func (sm *ShardMaster) startCommand(cmd Op) bool {
	notifyCh := make(chan raft.ApplyMsg, 1)
	sm.msgRegister.Store(cmd.id(), notifyCh)
	defer func() {
		sm.msgRegister.Delete(cmd.id())
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
			fmt.Printf("join servers:%v, config:%v\n", op.Servers, sm.latestConfig())
		case LEAVE:
			sm.handleLeave(op)
			fmt.Printf("leave servers:%v, config:%v\n", op.Servers, sm.latestConfig())
		case MOVE:
			sm.handleMove(op)
		}
		sm.processedMsg[op.ClientID] = op.ID
	}

	if val, ok := sm.msgRegister.Load(op.id()); ok {
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
	sm.applyCh = make(chan raft.ApplyMsg, 1024)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.processedMsg = make(map[int32]int32)
	sm.msgRegister = new(sync.Map)

	go sm.applyCommitLoop()
	return sm
}
