package kvraft

import (
	"crypto/rand"
	"math/big"
	"reflect"
	"sync/atomic"
	"time"

	"../labrpc"
)

type Clerk struct {
	clientID string
	servers  []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientID = uuid()
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	reply := &GetReply{}
	args := &GetArgs{ID: uuid(), ClientID: uuid(), Key: key}
	res := ck.trySendToLeader("KVServer.Get", args, reply)
	reply = res.(*GetReply)
	if reply.Err == ErrNoKey {
		return ""
	}
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	reply := &PutAppendReply{}
	ck.trySendToLeader("KVServer.PutAppend", &PutAppendArgs{ID: uuid(), ClientID: ck.clientID, Key: key, Value: value, Op: op}, reply)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}

type rpcResult struct {
	reply interface{}
	ok    bool
}

func (ck *Clerk) trySendToLeader(svcMeth string, args interface{}, reply Reply) interface{} {
	leader := ck.curLeader()
	result := make(chan *rpcResult)
	for {
		go func() {
			reply := reflect.New(reflect.TypeOf(reply).Elem()).Interface()
			result <- &rpcResult{
				ok:    ck.servers[leader].Call(svcMeth, args, reply),
				reply: reply,
			}
		}()

		select {
		case <-time.After(2 * time.Second):
			leader = (leader + 1) % len(ck.servers)
		case res := <-result:
			if !res.ok || res.reply.(Reply).Error() == ErrWrongLeader {
				leader = (leader + 1) % len(ck.servers)
			} else {
				ck.ResetLeader(leader)
				return res.reply
			}
		}
	}
}

func (ck *Clerk) curLeader() int {
	return int(atomic.LoadInt32(&ck.leader))
}

func (ck *Clerk) ResetLeader(leader int) {
	atomic.StoreInt32(&ck.leader, int32(leader))
}
