package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"../labgob"
	"../labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

type State int

const (
	FollowerState = iota
	CandidateState
	LeaderState
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	state     State
	timeout   int32
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm       int
	votedFor          int
	logs              []*Log
	lastIncludedTerm  int
	lastIncludedIndex int

	// volatile state
	commitIndex int

	// volatile state on leader
	nextIndexes  []int
	matchIndexes []int
}

type Log struct {
	Term int
	Data interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	// Your code here (2A).
	return rf.currentTerm, rf.state == LeaderState
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodeState())
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if err := e.Encode(rf.currentTerm); err != nil {
		log.Fatal("fail to persist current term")
	}

	if err := e.Encode(rf.commitIndex); err != nil {
		log.Fatal(err, "fail to persist commit index")
	}

	if err := e.Encode(rf.lastIncludedTerm); err != nil {
		log.Fatal(err, "fail to persist last included term")
	}

	if err := e.Encode(rf.lastIncludedIndex); err != nil {
		log.Fatal(err, "fail to persist last included index")
	}

	if err := e.Encode(rf.votedFor); err != nil {
		log.Fatal(err, "fail to persist voted for")
	}

	if err := e.Encode(rf.logs); err != nil {
		log.Fatal(err, "fail to persist logs")
	}

	return w.Bytes()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm, commitIndex, votedFor, lastIncludedTerm, lastIncludedIndex int
	var logs []*Log
	if d.Decode(&currentTerm) != nil || d.Decode(&commitIndex) != nil || d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		panic("fail to decode raft state")
	} else {
		rf.currentTerm = currentTerm
		rf.commitIndex = commitIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastIncludedIndex = lastIncludedIndex
		// fmt.Printf("read last include index:%d, commit index:%d\n", lastIncludedIndex, commitIndex)
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	_, lastLogIndex := rf.lastLogTermIndex()
	index := lastLogIndex + 1

	if rf.state == LeaderState {
		rf.logs = append(rf.logs, &Log{
			Term: rf.currentTerm,
			Data: command,
		})
		rf.persist()
	}
	return index, rf.currentTerm, rf.state == LeaderState
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = FollowerState
	rf.timeout = 1
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.applyCh = applyCh
	rf.nextIndexes = make([]int, len(rf.peers))
	rf.matchIndexes = make([]int, len(rf.peers))
	// Your initialization code here (2A, 2B, 2C).
	rf.readPersist(persister.ReadRaftState())
	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())
	go rf.initApplyCommands()
	go rf.electWhenTimeout()
	return rf
}

func (rf *Raft) initApplyCommands() {
	for i, l := range rf.logs {
		index := i + rf.lastIncludedIndex + 1
		if index <= rf.commitIndex {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      l.Data,
				CommandIndex: index,
				CommandTerm:  l.Term,
			}
		}
	}
}

func (rf *Raft) IsLeader() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.state == LeaderState
}

func (rf *Raft) MyTerm() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.currentTerm
}

func (rf *Raft) Logs() []*Log {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.logs
}

func (rf *Raft) toFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.state = FollowerState
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) toLeader() {
	rf.state = LeaderState
	_, lastLogIndex := rf.lastLogTermIndex()
	for i := range rf.nextIndexes {
		rf.nextIndexes[i] = lastLogIndex + 1
	}
}

func (rf *Raft) toCandidate() {
	rf.state = CandidateState
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
}

func (rf *Raft) logByIndex(index int) *Log {
	return rf.logs[rf.logPos(index)]
}

func (rf *Raft) logPos(index int) int {
	return index - rf.lastIncludedIndex - 1
}

func (rf *Raft) lastLogTermIndex() (int, int) {
	term, index := rf.lastIncludedTerm, rf.lastIncludedIndex
	if len(rf.logs) != 0 {
		lastLog := rf.logs[len(rf.logs)-1]
		term = lastLog.Term
		index += len(rf.logs)
	}
	return term, index
}

func (rf *Raft) LastIncludedIndex() int {
	return rf.lastIncludedIndex
}

func (rf *Raft) termOfIndex(index int) int {
	_, lastIndex := rf.lastLogTermIndex()
	if index > lastIndex {
		return -1
	}

	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}

	return rf.logByIndex(index).Term
}
