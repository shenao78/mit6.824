package kvraft

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	ID       int32
	ClientID int32
	OpName   string
	Key      string
	Value    string
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	store            map[string]string
	lastAppliedIndex int
	msgRegister      *sync.Map
	processedMsg     map[int32]int32
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	cmd := Op{ID: args.ID, ClientID: args.ClientID, OpName: GET, Key: args.Key}
	if err := kv.startCommand(cmd); err != OK {
		reply.Err = err
		return
	}

	kv.mu.RLock()
	defer kv.mu.RUnlock()

	if val, ok := kv.store[args.Key]; !ok {
		fmt.Printf("peer:%d get key:%s, val:%s client id:%d, id:%d\n", kv.me, args.Key, val, args.ClientID, args.ID)
		reply.Err = ErrNoKey
	} else {
		fmt.Printf("peer:%d get key:%s, val:%s client id:%d, id:%d\n", kv.me, args.Key, val, args.ClientID, args.ID)
		reply.Err = OK
		reply.Value = val
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	cmd := Op{ID: args.ID, ClientID: args.ClientID, OpName: args.Op, Key: args.Key, Value: args.Value}
	if err := kv.startCommand(cmd); err != OK {
		reply.Err = err
		return
	}
	reply.Err = OK
}

func (kv *KVServer) startCommand(cmd Op) Err {
	notifyCh := make(chan raft.ApplyMsg, 1)
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
		case msg := <-notifyCh:
			if msg.CommandTerm > term {
				return ErrWrongLeader
			} else {
				return OK
			}
		case <-time.After(20 * time.Millisecond):
			if kv.rf.MyTerm() > term {
				return ErrWrongLeader
			}
		}
	}
}

func (kv *KVServer) snapshotIfNeed() {
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
		snapshot, _ := json.Marshal(&SnapshotData{
			Store:        kv.store,
			ProcessedMsg: kv.processedMsg,
		})
		kv.rf.Snapshot(kv.lastAppliedIndex, snapshot)
	}
}

func (kv *KVServer) applyCommitLoop() {
	for msg := range kv.applyCh {
		switch msg.Command.(type) {
		case []byte:
			kv.applySnapshot(msg)
		case Op:
			kv.applyCmd(msg)
		}
	}
}

func (kv *KVServer) applyCmd(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.lastAppliedIndex = msg.CommandIndex

	cmd := msg.Command.(Op)
	if kv.processedMsg[cmd.ClientID] < cmd.ID {
		switch cmd.OpName {
		case PUT:
			kv.store[cmd.Key] = cmd.Value
			fmt.Printf("peer:%d (id:%d, client:%d) put key:%s, val:%s store:%v procesed:%v\n", kv.me, cmd.ID, cmd.ClientID,  cmd.Key, cmd.Value, kv.store, kv.processedMsg)
		case APPEND:
			kv.store[cmd.Key] += cmd.Value
			fmt.Printf("peer:%d (id:%d, client:%d) append key:%s, val:%s store:%v, processed:%v\n", kv.me, cmd.ID, cmd.ClientID,  cmd.Key, cmd.Value, kv.store, kv.processedMsg)
		}
		kv.processedMsg[cmd.ClientID] = cmd.ID
	}

	kv.snapshotIfNeed()

	if val, ok := kv.msgRegister.Load(cmd.ID); ok {
		notifyCh := val.(chan raft.ApplyMsg)
		select {
		case notifyCh <- msg:
		default:
		}
	}
}

func (kv *KVServer) applySnapshot(msg raft.ApplyMsg) {
	snapshot := msg.Command.([]byte)
	if ok := kv.rf.CondInstallSnapshot(msg.CommandTerm, msg.CommandIndex, snapshot); ok {
		snapshotData := readStoreFromSnapshot(snapshot)
		kv.mu.Lock()
		fmt.Printf("peer %d install snapshot store:%v lastTerm:%d lastIndex:%d\n", kv.me, snapshotData.Store, msg.CommandTerm, msg.CommandIndex)
		kv.store = snapshotData.Store
		kv.processedMsg = snapshotData.ProcessedMsg
		kv.lastAppliedIndex = msg.CommandIndex
		kv.mu.Unlock()
	}
}

func readStoreFromSnapshot(snapshot []byte) *SnapshotData {
	snapshotData := &SnapshotData{
		Store:        make(map[string]string),
		ProcessedMsg: make(map[int32]int32),
	}
	if snapshot != nil {
		if err := json.Unmarshal(snapshot, &snapshotData); err != nil {
			panic("fail to decode snapshot")
		}
	}
	return snapshotData
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg, 1024)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.msgRegister = new(sync.Map)

	snapshotData := readStoreFromSnapshot(persister.ReadSnapshot())
	kv.processedMsg = snapshotData.ProcessedMsg
	kv.store = snapshotData.Store

	kv.lastAppliedIndex = kv.rf.LastIncludedIndex()

	go kv.applyCommitLoop()
	return kv
}
