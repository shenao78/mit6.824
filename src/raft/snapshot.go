package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	lastIncludedTerm  int
	data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.RLock()
	rf.mu.RUnlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      args.data,
			CommandIndex: args.lastIncludedTerm,
			CommandTerm:  args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	rf.mu.Unlock()

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)

	pos := rf.logPos(index)
	tailLogs := rf.logs[pos+1:]
	newLogs := make([]*Log, len(tailLogs))
	copy(newLogs[:], tailLogs[:])

	rf.lastIncludedTerm = rf.logs[index].Term
	rf.lastIncludedIndex = index
	rf.logs = newLogs
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isNewerLog(lastIncludedTerm, lastIncludedIndex) {
		return false
	}

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastIncludedIndex = lastIncludedIndex
	rf.logs = []*Log{}
	return false
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
