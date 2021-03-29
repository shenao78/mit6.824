package shardkv

import (
	"fmt"
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
	configNum := 0
	if kv.config != nil {
		configNum = kv.config.Num
	}

	latestConfig := kv.sm.Query(-1)
	config := kv.config
	// fmt.Printf("gid:%d peer:%d config num:%d latest config num%d\n", kv.gid, kv.me, configNum, latestConfig.Num)
	for nextConfigNum := configNum + 1; nextConfigNum <= latestConfig.Num; nextConfigNum++ {
		nextConfig := latestConfig
		if nextConfigNum != latestConfig.Num {
			nextConfig = kv.sm.Query(nextConfigNum)
		}

		kv.migrationConfig(config, &nextConfig)
		config = &nextConfig
	}
}

func (kv *ShardKV) migrationConfig(config, nextConfig *shardmaster.Config) {
	oldShards := make(map[int]bool)
	if config != nil {
		for gid, shard := range config.Shards {
			if gid == kv.gid {
				oldShards[shard] = true
			}
		}
	}

	var newShards []int
	for shard, gid := range nextConfig.Shards {
		if gid == kv.gid && !oldShards[shard] {
			newShards = append(newShards, shard)
		}
	}

	newState := make(map[string]string)
	fmt.Printf("gid:%d peer:%d reConfigurations new shard %v\n", kv.gid, kv.me, newShards)
	if len(newShards) != 0 {
		prevConfigNum := nextConfig.Num - 1
		// fmt.Printf("gid:%d peer:%d prev config num:%d\n", kv.gid, kv.me, prevConfigNum)
		if prevConfigNum > 0 {
			prevConfig := kv.sm.Query(prevConfigNum)
			gidToShards := groupShardsByGid(newShards, prevConfig.Shards)
			// fmt.Printf("gid:%d peer:%d get prev config:%v\n", kv.gid, kv.me, prevConfig)
			for gid, shards := range gidToShards {
				servers := prevConfig.Groups[gid]
				subState := kv.requestState(shards, servers)
				for key, val := range subState {
					newState[key] = val
				}
			}
		}
	}

	kv.startCommand(Op{
		ID:       newReqID(),
		ClientID: kv.clientID,
		OpName:   ReConfigurations,
		Config:   nextConfig,
		State:    newState,
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
	Err   Err
	State map[string]string
}

func (kv *ShardKV) GetState(args *GetStateArgs, reply *GetStateReply) {
	result := make(map[string]string)
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

	for key, val := range kv.store {
		if shardMap[key2shard(key)] {
			result[key] = val
		}
	}
	// fmt.Printf("gid %d peer:%d send state to (gid:%d peer:%d):%v\n", kv.gid, kv.me, args.GID, result)
	reply.Err = OK
	reply.State = result
}

func (kv *ShardKV) requestState(shards []int, servers []string) map[string]string {
	for {
		for _, server := range servers {
			client := kv.make_end(server)
			args := &GetStateArgs{
				ID:            newReqID(),
				ClientID:      kv.clientID,
				Shards:        shards,
				NextConfigNum: kv.config.Num + 1,
			}

			reply := &GetStateReply{}
			ok := client.Call("ShardKV.GetState", args, reply)
			if ok && reply.Err == OK {
				fmt.Printf("gid:%d peer:%d request shards %v from server:%s, state:%v\n", kv.gid, kv.me, shards, server, reply.State)
				return reply.State
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
