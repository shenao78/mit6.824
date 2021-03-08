package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogTerm  int
	PrevLogIndex int
	Logs         []*Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.resetElectTimeout(false)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.state == CandidateState) {
		rf.toFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	prevIndex := args.PrevLogIndex
	if len(rf.logs) < prevIndex {
		reply.Success = false
		reply.NextIndex = len(rf.logs) + 1
	} else if prevIndex == 0 || rf.logs[prevIndex-1].Term == args.PrevLogTerm {
		reply.Success = true
		rf.logs = append(rf.logs[:args.PrevLogIndex], args.Logs...)
		if args.LeaderCommit > rf.commitIndex {
			newCommitIndex := Mini(args.LeaderCommit, len(rf.logs))
			rf.commitLog(newCommitIndex)
		}
	} else {
		reply.Success = false
		index := prevIndex - 1
		for ; index >= 1; index-- {
			if rf.logs[index].Term != rf.logs[index-1].Term {
				break
			}
		}
		reply.NextIndex = index + 1
		rf.logs = rf.logs[:reply.NextIndex]
		reply.Success = false

	}
	rf.persist()
}

type appendReplyWithServer struct {
	server int
	reply  *AppendEntriesReply
}

func (rf *Raft) leaderLoop() {
	for !rf.killed() && rf.myState() == LeaderState {
		argsList := rf.buildAppendEntriesArgs()
		peerCnt := len(rf.peers) - 1
		replies := make(chan *appendReplyWithServer, peerCnt)
		for s, a := range argsList {
			server, args := s, a
			if server == rf.me {
				continue
			}

			go func() {
				reply := &AppendEntriesReply{}
				if ok := rf.sendAppendEntries(server, args, reply); !ok {
					return
				}
				replies <- &appendReplyWithServer{server: server, reply: reply}
			}()
		}
		replyCnt := 0
	Out:
		for replyCnt < peerCnt {
			select {
			case <-time.After(10 * time.Millisecond):
				break Out
			case reply := <-replies:
				rf.mu.Lock()
				if reply.reply.Term > rf.currentTerm {
					rf.toFollower(reply.reply.Term)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				rf.handleAppendEntriesReply(reply.server, argsList[reply.server], reply.reply)
				replyCnt++
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Success {
		rf.nextIndexes[server] = args.PrevLogIndex + len(args.Logs) + 1
		rf.matchIndexes[server] = rf.nextIndexes[server] - 1
	} else {
		rf.nextIndexes[server] = reply.NextIndex
	}

	for n := rf.commitIndex + 1; n <= len(rf.logs); n++ {
		cnt := 1
		for server, matchIndex := range rf.matchIndexes {
			if matchIndex >= n && server != rf.me {
				cnt++
			}
		}
		if n > rf.commitIndex && cnt > len(rf.peers)/2 && rf.logs[n-1].Term == rf.myTerm() {
			rf.commitLog(n)
		}
	}
}

func (rf *Raft) commitLog(newCommitIndex int) {
	for index := rf.commitIndex + 1; index <= newCommitIndex; index++ {
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.logs[index-1].Data, CommandIndex: index}
	}
	rf.commitIndex = newCommitIndex
}

func (rf *Raft) buildAppendEntriesArgs() []*AppendEntriesArgs {
	result := make([]*AppendEntriesArgs, len(rf.peers))
	commitIndex := rf.commitIndex
	for server, nextIndex := range rf.nextIndexes {
		if server == rf.me {
			continue
		}

		prevLogTerm, prevLogIndex := 0, nextIndex-1
		if prevLogIndex != 0 {
			prevLogTerm = rf.logs[prevLogIndex-1].Term
		}

		var sendLogs []*Log
		if len(rf.logs) >= nextIndex {
			sendLogs = rf.logs[nextIndex-1:]
		}

		result[server] = &AppendEntriesArgs{
			Term:         rf.myTerm(),
			LeaderID:     rf.me,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: prevLogIndex,
			Logs:         sendLogs,
			LeaderCommit: commitIndex,
		}
	}
	return result
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
