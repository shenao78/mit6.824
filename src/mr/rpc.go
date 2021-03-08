package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

const (
	NoTask = iota
	MapTask
	ReduceTask
)

// Add your RPC definitions here.

type ReqTaskArgs struct{}

type ReqTaskReply struct {
	TaskType  int
	FileName  string
	TaskNum   int
	MapCnt    int
	ReduceCnt int
}

type WorkDoneArgs struct {
	TaskType int
	TaskNum  int
}

type WorkDoneReplay struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
