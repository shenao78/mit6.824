package raft

import (
	"time"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	reply.Term = rf.MyTerm()
	if args.Term < reply.Term {
		return
	}

	rf.applyCh <- ApplyMsg{
		CommandValid: true,
		Command:      args.Data,
		CommandTerm:  args.LastIncludedTerm,
		CommandIndex: args.LastIncludedIndex,
	}
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	pos := rf.logPos(index)
	tailLogs := rf.logs[pos+1:]
	newLogs := make([]*Log, len(tailLogs))
	copy(newLogs[:], tailLogs[:])

	rf.lastIncludedTerm = rf.logByIndex(index).Term
	rf.lastIncludedIndex = index
	rf.logs = newLogs

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isNewerLog(lastIncludedTerm, lastIncludedIndex) {
		return false
	}

	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastIncludedIndex = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.logs = []*Log{}
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	return true
}

func (rf *Raft) installSnapshotToServer(server int) {
	rf.mu.RLock()
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.snapshot,
	}
	rf.mu.RUnlock()

	result := make(chan *InstallSnapshotReply)
	go func() {
		reply := &InstallSnapshotReply{}
		if ok := rf.sendInstallSnapshot(server, args, reply); !ok {
			return
		}
		result <- reply
	}()

	select {
	case <-time.After(10 * time.Millisecond):
		return
	case reply := <-result:
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.toFollower(reply.Term)
		} else {
			rf.nextIndexes[server] = args.LastIncludedIndex + 1
			rf.matchIndexes[server] = args.LastIncludedIndex
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
