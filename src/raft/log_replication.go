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
	if args.PrevLogIndex+1 <= rf.lastIncludedIndex {
		rf.appendLogsAfterSnapshot(args)
		reply.Success = true
		return
	}

	prevIndex := args.PrevLogIndex
	_, lastIndex := rf.lastLogTermIndex()
	if lastIndex < prevIndex {
		reply.Success = false
		reply.NextIndex = lastIndex + 1
	} else if prevIndex == 0 || rf.termOfIndex(prevIndex) == args.PrevLogTerm {
		reply.Success = true
		prevPos := rf.logPos(args.PrevLogIndex)
		newLogs := append(rf.logs[:prevPos+1], args.Logs...)
		if len(rf.logs) > len(newLogs) {
			newLogs = append(newLogs, rf.logs[len(newLogs):]...)
		}
		rf.logs = newLogs
		if args.LeaderCommit > rf.commitIndex {
			rf.commitLog(args.LeaderCommit)
		}
	} else {
		reply.Success = false
		index := prevIndex
		for ; rf.logPos(index-1) > 0; index-- {
			if rf.logByIndex(index).Term != rf.logByIndex(index - 1).Term {
				break
			}
		}
		reply.NextIndex = index
		nextPos := rf.logPos(index)
		if nextPos < 0 {
			nextPos = 0
		}
		rf.logs = rf.logs[:nextPos]
	}
	rf.persist()
}

func (rf *Raft) appendLogsAfterSnapshot(args *AppendEntriesArgs) {
	var newLogs []*Log
	for i := range args.Logs {
		index := args.PrevLogIndex + i + 1
		if index > rf.lastIncludedIndex {
			newLogs = append(newLogs, args.Logs[i])
		}
	}
	if len(rf.logs) > len(newLogs) {
		newLogs = append(newLogs, rf.logs[len(newLogs):]...)
	}
	rf.logs = newLogs
}

type appendReplyWithServer struct {
	server int
	reply  *AppendEntriesReply
}

func (rf *Raft) leaderLoop() {
	for !rf.killed() && rf.IsLeader() {
		argsList := rf.buildAppendEntriesArgs()
		sendCnt := 0
		replies := make(chan *appendReplyWithServer, len(rf.peers))
		for s, a := range argsList {
			server, args := s, a
			if a == nil {
				continue
			}

			sendCnt++
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
		for replyCnt < sendCnt {
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

		select {
		case <-time.After(100 * time.Millisecond):
		case <-rf.newLogCh:
		}
	}
}

func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Success {
		rf.nextIndexes[server] = args.PrevLogIndex + len(args.Logs) + 1
		rf.matchIndexes[server] = rf.nextIndexes[server] - 1
	} else {
		rf.nextIndexes[server] = reply.NextIndex
	}

	_, lastIndex := rf.lastLogTermIndex()
	for n := rf.commitIndex + 1; n <= lastIndex; n++ {
		cnt := 1
		var servers []int
		for server, matchIndex := range rf.matchIndexes {
			if matchIndex >= n && server != rf.me {
				servers = append(servers, server)
				cnt++
			}
		}
		curTerm := rf.currentTerm
		if n > rf.commitIndex && cnt > len(rf.peers)/2 && rf.logByIndex(n).Term == curTerm {
			rf.commitLog(n)
		}
	}
}

func (rf *Raft) commitLog(commitIndex int) {
	_, lastIndex := rf.lastLogTermIndex()
	newCommitIndex := Mini(commitIndex, lastIndex)
	for index := rf.commitIndex + 1; index <= newCommitIndex; index++ {
		log := rf.logByIndex(index)
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: log.Data, CommandIndex: index, CommandTerm: log.Term}
	}
	rf.commitIndex = newCommitIndex
}

func (rf *Raft) buildAppendEntriesArgs() []*AppendEntriesArgs {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	result := make([]*AppendEntriesArgs, len(rf.peers))
	for server, nextIndex := range rf.nextIndexes {
		if server == rf.me {
			continue
		}

		result[server] = rf.buildAppendEntriesArg(server, nextIndex)
	}
	return result
}

func (rf *Raft) buildAppendEntriesArg(server, nextIndex int) *AppendEntriesArgs {
	beginPos := rf.logPos(nextIndex)
	if beginPos < 0 {
		go rf.installSnapshotToServer(server)
		return nil
	}

	prevLogTerm, prevLogIndex := rf.lastIncludedTerm, nextIndex-1
	if prevLogIndex != 0 && beginPos > 0 && beginPos <= len(rf.logs) {
		prevLogTerm = rf.logByIndex(prevLogIndex).Term
	}

	var sendLogs []*Log
	if beginPos < len(rf.logs) {
		sendLogs = rf.logs[rf.logPos(nextIndex):]
	}

	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogTerm:  prevLogTerm,
		PrevLogIndex: prevLogIndex,
		Logs:         sendLogs,
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
