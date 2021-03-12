package kvraft

import (
	// "fmt"
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
	ID       string
	ClientID string
	OpName   string
	Key      string
	Value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store        map[string]string
	msgRegister  *sync.Map
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// fmt.Printf("peer %d get %s\n", kv.me, args.ID)
	cmd := Op{ID: args.ID, OpName: GET, Key: args.Key}
	if err := kv.startCommand(cmd); err != "" {
		// fmt.Printf("peer %d get key start command err:%s id:%s store:%v\n", kv.me, err, args.ID, kv.store)
		reply.Err = err
		return
	}

	if val, ok := kv.store[args.Key]; !ok {
		reply.Err = ErrNoKey
		// fmt.Printf("finish get no key:%s, id:%s, store:%v\n", args.Key, args.ID, kv.store)
	} else {
		// fmt.Printf("finish get key:%s, id:%s, store:%v\n", args.Key, args.ID, kv.store)
		reply.Err = OK
		reply.Value = val
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	cmd := Op{ID: args.ID, ClientID: args.ClientID, OpName: args.Op, Key: args.Key, Value: args.Value}
	if err := kv.startCommand(cmd); err != "" {
		reply.Err = err
		return
	}
	reply.Err = OK
}

func (kv *KVServer) startCommand(cmd Op) Err {
	if kv.rf.IsLeader() && kv.isProcessed(cmd) {
		return ""
	}

	notifyCh := make(chan raft.ApplyMsg, 1)
	kv.msgRegister.Store(notifyCh, true)
	defer func() {
		kv.msgRegister.Delete(notifyCh)
	}()

	_, term, leader := kv.rf.Start(cmd)
	if !leader {
		return ErrWrongLeader
	}

	for {
		select {
		case msg := <-notifyCh:
			// fmt.Printf("got notify channel:%v\n", msg)
			if msg.CommandTerm > term {
				// fmt.Println("err wrong leader")
				return ErrWrongLeader
			} else {
				return ""
			}
		case <-time.After(20 * time.Millisecond):
			if kv.rf.MyTerm() > term {
				// fmt.Printf("peer %d lose leader\n", kv.me)
				return ErrWrongLeader
			}
		}
	}
}

func (kv *KVServer) isProcessed(cmd Op) bool {
	logs := kv.rf.Logs()
	for i := len(logs) - 1; i >= 0; i-- {
		op := logs[i].Data.(Op)
		if op.OpName != GET && op.ClientID == cmd.ClientID {
			if op.ID == cmd.ID {
				return true
			} else {
				return false
			}
		}
	}
	return false
}

func (kv *KVServer) applyCommitLoop() {
	for msg := range kv.applyCh {
		cmd := msg.Command.(Op)
		switch cmd.OpName {
		case PUT:
			kv.store[cmd.Key] = cmd.Value
			// fmt.Printf("peer %d put key:%s val:%s, id:%s client_id:%s store:%v\n", kv.me, cmd.Key, cmd.Value, cmd.ID, cmd.ClientID, kv.store)
		case APPEND:
			kv.store[cmd.Key] += cmd.Value
			// fmt.Printf("peer %d append key:%s val:%s, id:%s client_id:%s store:%v\n", kv.me, cmd.Key, cmd.Value, cmd.ID, cmd.ClientID, kv.store)
		}

		kv.msgRegister.Range(func(key, val interface{}) bool {
			notifyCh := key.(chan raft.ApplyMsg)
			select{
			case notifyCh <- msg:
			default:
			}
			return true
		})
	}
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.msgRegister = new(sync.Map)

	go kv.applyCommitLoop()
	return kv
}
