package shardkv

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

const (
	Get              = "Get"
	Put              = "Put"
	Append           = "Append"
	GetState         = "GetState"
	ReConfigurations = "ReConfigurations"
)

type Op struct {
	ID           int32
	ClientID     int32
	OpName       string
	configNum    int
	Key          string
	Value        string
	Config       shardmaster.Config
	State        map[string]string
	ProcessedMsg map[int32]UniMsg
}

type UniMsg struct {
	ID    int32
	Shard int
}

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	sm           *shardmaster.Clerk

	// Your definitions here.
	config           shardmaster.Config
	store            map[string]string
	lastAppliedIndex int
	msgRegister      *sync.Map
	processedMsg     map[int32]UniMsg
	clientID         int32
	knownConfigNum   int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	wrongGroup, configNum := kv.isWrongGroup(args.Key, args.ConfigNum)
	if wrongGroup {
		reply.Err = ErrWrongGroup
		return
	}

	cmd := Op{ID: args.ID, ClientID: args.ClientID, OpName: Get, configNum: configNum, Key: args.Key}
	if err := kv.startCommand(cmd); err != OK {
		reply.Err = err
		return
	}

	kv.mu.RLock()
	defer kv.mu.RUnlock()

	fmt.Printf("gid:%d peer:%d get key:%s, store:%v config:%v\n", kv.gid, kv.me, args.Key, kv.store, kv.config)
	if val, ok := kv.store[args.Key]; !ok {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
		reply.Value = val
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	wrongGroup, configNum := kv.isWrongGroup(args.Key, args.ConfigNum)
	if wrongGroup {
		reply.Err = ErrWrongGroup
		return
	}

	cmd := Op{ID: args.ID, ClientID: args.ClientID, OpName: args.Op, configNum: configNum, Key: args.Key, Value: args.Value}
	if err := kv.startCommand(cmd); err != OK {
		reply.Err = err
		return
	}
	reply.Err = OK
}

func (kv *ShardKV) isWrongGroup(key string, clientConfigNum int) (bool, int) {
	shard := key2shard(key)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if clientConfigNum > kv.knownConfigNum {
		kv.knownConfigNum = clientConfigNum
	}

	wrongGroup := kv.knownConfigNum > kv.config.Num || kv.config.Shards[shard] != kv.gid
	return wrongGroup, kv.config.Num
}

type cmdResult struct {
	err      Err
	applyMsg raft.ApplyMsg
}

func (kv *ShardKV) startCommand(cmd Op) Err {
	notifyCh := make(chan *cmdResult, 1)
	kv.msgRegister.Store(cmd.ID, notifyCh)
	defer func() {
		kv.msgRegister.Delete(cmd.ID)
	}()

	_, term, leader := kv.rf.Start(cmd)
	if !leader {
		return ErrWrongLeader
	}

	for {
		select {
		case result := <-notifyCh:
			if result.err != OK {
				return result.err
			}
			if result.applyMsg.CommandTerm > term {
				return ErrWrongLeader
			} else {
				return OK
			}
		case <-time.After(20 * time.Millisecond):
			if kv.rf.MyTerm() > term {
				fmt.Printf("gid:%d server%d lose leader\n", kv.gid, kv.me)
				return ErrWrongLeader
			}
		}
	}
}

func (kv *ShardKV) snapshotIfNeed() {
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
		snapshot, _ := json.Marshal(&SnapshotData{
			Store:        kv.store,
			ProcessedMsg: kv.processedMsg,
			ClientID:     kv.clientID,
			Config:       kv.config,
		})
		kv.rf.Snapshot(kv.lastAppliedIndex, snapshot)
	}
}

func (kv *ShardKV) applyCommitLoop() {
	for msg := range kv.applyCh {
		switch msg.Command.(type) {
		case []byte:
			kv.applySnapshot(msg)
		case Op:
			kv.applyCmd(msg)
		}
	}
}

func (kv *ShardKV) applyCmd(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.lastAppliedIndex = msg.CommandIndex

	var err Err = OK
	cmd := msg.Command.(Op)
	if kv.processedMsg[cmd.ClientID].ID != cmd.ID {
		switch cmd.OpName {
		case Put:
			if kv.config.Num < kv.knownConfigNum || kv.config.Num != cmd.configNum {
				err = ErrWrongGroup
			}
			kv.store[cmd.Key] = cmd.Value
			fmt.Printf("gid:%d peer:%d (id:%d) err:%s put key:%s(shard:%d), val:%s store:%v\n", kv.gid, kv.me, cmd.ID, err, cmd.Key, key2shard(cmd.Key), cmd.Value, kv.store)
		case Append:
			if kv.config.Num < kv.knownConfigNum || kv.config.Num != cmd.configNum {
				err = ErrWrongGroup
			}
			kv.store[cmd.Key] += cmd.Value
			fmt.Printf("gid:%d peer:%d (id:%d) err:%s append key:%s(shard:%d), val:%s store:%v\n", kv.gid, kv.me, cmd.ID, err, cmd.Key, key2shard(cmd.Key), cmd.Value, kv.store)
		case ReConfigurations:
			kv.applyReConfigurations(cmd)
		}
		kv.processedMsg[cmd.ClientID] = UniMsg{ID: cmd.ID, Shard: key2shard(cmd.Key)}
	}

	kv.snapshotIfNeed()

	if val, ok := kv.msgRegister.Load(cmd.ID); ok {
		notifyCh := val.(chan *cmdResult)
		select {
		case notifyCh <- &cmdResult{err: err, applyMsg: msg}:
			if err != OK {
				fmt.Printf("notify msg:%s err\n", cmd.Value)
			}
		default:
		}
	}
}

func (kv *ShardKV) applyReConfigurations(cmd Op) {
	for key, val := range cmd.State {
		kv.store[key] = val
	}
	for clientID, msg := range cmd.ProcessedMsg {
		kv.processedMsg[clientID] = msg
	}
	kv.config = cmd.Config
	fmt.Printf("gid:%d peer:%d change to config:%v\n", kv.gid, kv.me, kv.config)
}

func (kv *ShardKV) applySnapshot(msg raft.ApplyMsg) {
	snapshot := msg.Command.([]byte)
	if ok := kv.rf.CondInstallSnapshot(msg.CommandTerm, msg.CommandIndex, snapshot); ok {
		snapshotData := readStoreFromSnapshot(snapshot)
		kv.mu.Lock()
		kv.store = snapshotData.Store
		kv.processedMsg = snapshotData.ProcessedMsg
		kv.config = snapshotData.Config
		kv.lastAppliedIndex = msg.CommandIndex
		kv.mu.Unlock()
	}
}

func readStoreFromSnapshot(snapshot []byte) *SnapshotData {
	snapshotData := &SnapshotData{
		Store:        make(map[string]string),
		ProcessedMsg: make(map[int32]UniMsg),
	}
	if snapshot != nil {
		if err := json.Unmarshal(snapshot, &snapshotData); err != nil {
			panic("fail to decode snapshot")
		}
	}
	return snapshotData
}

func (kv *ShardKV) myConfig() shardmaster.Config {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	return kv.config
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg, 1024)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.msgRegister = new(sync.Map)

	snapshotData := readStoreFromSnapshot(persister.ReadSnapshot())
	kv.processedMsg = snapshotData.ProcessedMsg
	kv.store = snapshotData.Store
	kv.config = snapshotData.Config
	kv.clientID = snapshotData.ClientID

	kv.persister = persister
	kv.lastAppliedIndex = kv.rf.LastIncludedIndex()
	kv.sm = shardmaster.MakeClerk(masters)
	kv.clientID = newClientID()

	go kv.fetchConfigLoop()
	go kv.applyCommitLoop()
	return kv
}
