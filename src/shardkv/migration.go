package shardkv

import (
	"time"

	"../shardmaster"
)

func (kv *ShardKV) fetchConfigLoop() {
	for {
		if kv.rf.IsLeader() {
			kv.reConfigurations()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) reConfigurations() {
	latestConfig := kv.sm.Query(-1)
	config := kv.myConfig()
	for nextConfigNum := config.Num + 1; nextConfigNum <= latestConfig.Num; nextConfigNum++ {
		nextConfig := latestConfig
		if nextConfigNum != latestConfig.Num {
			nextConfig = kv.sm.Query(nextConfigNum)
		}

		if kv.migrationConfig(config, nextConfig) != OK {
			break
		}
		config = nextConfig
	}
}

func (kv *ShardKV) migrationConfig(config, nextConfig shardmaster.Config) Err {
	oldShards := make(map[int]bool)
	for shard, gid := range config.Shards {
		if gid == kv.gid {
			oldShards[shard] = true
		}
	}

	var newShards []int
	for shard, gid := range nextConfig.Shards {
		if gid == kv.gid && !oldShards[shard] {
			newShards = append(newShards, shard)
		}
	}

	newState := make(map[string]string)
	processedMsg := make(map[int32]UniMsg)
	if len(newShards) != 0 {
		if config.Num > 0 {
			gidToShards := groupShardsByGid(newShards, config.Shards)
			for gid, shards := range gidToShards {
				servers := config.Groups[gid]
				reply := kv.requestState(shards, servers, nextConfig.Num)
				for key, val := range reply.State {
					newState[key] = val
				}
				for clientID, msg := range reply.ProcessedMsg {
					if msg.ID > processedMsg[clientID].ID {
						processedMsg[clientID] = msg
					}
				}
			}
		}
	}

	return kv.startCommand(Op{
		ID:           newReqID(),
		ClientID:     kv.clientID,
		OpName:       ReConfigurations,
		Config:       nextConfig,
		State:        newState,
		ProcessedMsg: processedMsg,
	})
}

func groupShardsByGid(shards []int, shardToGid [shardmaster.NShards]int) map[int][]int {
	result := make(map[int][]int)
	for _, shard := range shards {
		gid := shardToGid[shard]
		result[gid] = append(result[gid], shard)
	}
	return result
}

type GetStateArgs struct {
	ID            int32
	ClientID      int32
	Shards        []int
	NextConfigNum int
}

type GetStateReply struct {
	Err          Err
	State        map[string]string
	ProcessedMsg map[int32]UniMsg
}

func (kv *ShardKV) GetState(args *GetStateArgs, reply *GetStateReply) {
	op := Op{ID: args.ID, ClientID: args.ClientID, OpName: GetState}
	if err := kv.startCommand(op); err != OK {
		reply.Err = err
		return
	}

	shardMap := make(map[int]bool)
	for _, shard := range args.Shards {
		shardMap[shard] = true
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.NextConfigNum > kv.knownConfigNum {
		kv.knownConfigNum = args.NextConfigNum
	}

	if args.NextConfigNum-1 > kv.config.Num {
		reply.Err = ErrNotTakeOver
		return
	}

	state := make(map[string]string)
	for key, val := range kv.store {
		if shardMap[key2shard(key)] {
			state[key] = val
		}
	}

	processedMsg := make(map[int32]UniMsg)
	for clientID, msg := range kv.processedMsg {
		if shardMap[msg.Shard] {
			processedMsg[clientID] = msg
		}
	}
	reply.Err = OK
	reply.State = state
	reply.ProcessedMsg = processedMsg
}

func (kv *ShardKV) requestState(shards []int, servers []string, nextConfigNum int) *GetStateReply {
	for {
		for _, server := range servers {
			client := kv.make_end(server)
			args := &GetStateArgs{
				ID:            newReqID(),
				ClientID:      kv.clientID,
				Shards:        shards,
				NextConfigNum: nextConfigNum,
			}

			reply := &GetStateReply{}
			ok := client.Call("ShardKV.GetState", args, reply)
			if ok && reply.Err == OK {
				return reply
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
