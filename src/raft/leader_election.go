package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Team         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Team < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Team >= rf.currentTerm {
		if args.Team > rf.currentTerm {
			rf.toFollower(args.Team)
		}
		if rf.votedFor == -1 && rf.isNewerLog(args.LastLogTerm, args.LastLogIndex) {
			rf.votedFor = args.CandidateID
			rf.persist()
			reply.Term = args.Team
			reply.VoteGranted = true
		}
	}
}

func (rf *Raft) isNewerLog(lastLogTerm, lastLogIndex int) bool {
	if len(rf.logs) == 0 {
		return true
	}

	term, index := rf.lastLogTermIndex()
	if lastLogTerm > term {
		return true
	}
	if lastLogTerm == term {
		return lastLogIndex >= index
	}
	return false
}

func (rf *Raft) electWhenTimeout() {
	for {
		time.Sleep(randElectTime())
		if rf.killed() {
			return
		}

		if rf.myState() != LeaderState && rf.isElectTimeout() {
			rf.startElection()
		}
		rf.resetElectTimeout(true)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.toCandidate()
	rf.mu.Unlock()

	args := rf.buildRequestVoteArgs()
	rf.parallelSendRequestVote(args)
}

func (rf *Raft) buildRequestVoteArgs() *RequestVoteArgs {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	args := &RequestVoteArgs{
		Team:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogTerm:  lastLogTerm,
		LastLogIndex: lastLogIndex,
	}
	return args
}

func (rf *Raft) parallelSendRequestVote(args *RequestVoteArgs) {
	replies := make(chan *RequestVoteReply, len(rf.peers))
	for p := range rf.peers {
		server := p
		if server == rf.me {
			continue
		}

		go func() {
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(server, args, reply); ok {
				replies <- reply
			}
		}()
	}

	replyCnt, grantedCnt := 0, 1
	for {
		select {
		case <-time.After(20 * time.Millisecond):
			return
		case reply := <-replies:
			replyCnt++
			if reply.VoteGranted {
				grantedCnt++
			}
			if over := rf.handleRequestVoteReply(grantedCnt, reply); over || replyCnt == len(rf.peers) {
				return
			}
		}
	}
}

func (rf *Raft) handleRequestVoteReply(grantedCnt int, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != CandidateState {
		return true
	}

	if reply.Term > rf.currentTerm {
		rf.toFollower(reply.Term)
		return true
	}

	if grantedCnt > len(rf.peers)/2 {
		rf.toLeader()
		go rf.leaderLoop()
		return true
	}
	return false
}

func (rf *Raft) isElectTimeout() bool {
	return atomic.LoadInt32(&rf.timeout) == 1
}

func (rf *Raft) resetElectTimeout(timeout bool) {
	val := int32(0)
	if timeout {
		val = 1
	}
	atomic.StoreInt32(&rf.timeout, val)
}

func randElectTime() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Duration(r.Intn(150)+200) * time.Millisecond
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
