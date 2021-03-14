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

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.persister.SaveRaftState(snapshot)

	rf.mu.Lock()
	rf.mu.Unlock()

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

	rf.persister.SaveRaftState(snapshot)
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastIncludedIndex = lastIncludedIndex
	rf.logs = []*Log{}
	return false
}
