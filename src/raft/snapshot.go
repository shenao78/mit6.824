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

}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return false
}
