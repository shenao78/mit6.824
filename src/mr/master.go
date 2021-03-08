package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	nReduce int
	nMap    int

	mapDoneCnt  int
	mapWorkCh   chan *mapWork
	mapDoneNums *sync.Map

	reduceDoneCnt  int
	reduceWorkCh   chan int
	reduceDoneNums *sync.Map

	done bool
	lock sync.RWMutex
}

type mapWork struct {
	file   string
	mapNum int
}

func (m *Master) ReqTask(args *ReqTaskArgs, reply *ReqTaskReply) error {
	reply.TaskType = NoTask
	reply.MapCnt = m.nMap
	reply.ReduceCnt = m.nReduce

	work, ok := <-m.mapWorkCh
	if ok {
		reply.TaskType = MapTask
		reply.TaskNum = work.mapNum
		reply.FileName = work.file

		go func() {
			time.Sleep(10 * time.Second)
			if _, ok := m.mapDoneNums.Load(work.mapNum); !ok {
				// timeout, reinsert the work to the chan
				m.mapWorkCh <- work
			}
		}()
	} else {
		for work := range m.reduceWorkCh {
			reply.TaskType = ReduceTask
			reply.TaskNum = work

			go func() {
				time.Sleep(10 * time.Second)
				if _, ok := m.reduceDoneNums.Load(work); !ok {
					m.reduceWorkCh <- work
				}
			}()
		}
	}
	return nil
}

func (m *Master) WorkDone(args *WorkDoneArgs, reply *WorkDoneReplay) error {
	switch args.TaskType {
	case MapTask:
		m.setMapDone(args.TaskNum)
	case ReduceTask:
		m.setReduceDone(args.TaskNum)
	}
	return nil
}

func (m *Master) setMapDone(taskNum int) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.mapDoneNums.Load(taskNum); !ok {
		m.mapDoneCnt++
		m.mapDoneNums.Store(taskNum, true)
		if m.mapDoneCnt == m.nMap {
			close(m.mapWorkCh)
			for i := 0; i < m.nReduce; i++ {
				m.reduceWorkCh <- i
			}
		}
	}
}

func (m *Master) setReduceDone(taskNum int) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.reduceDoneNums.Load(taskNum); !ok {
		m.reduceDoneCnt++
		m.reduceDoneNums.Store(taskNum, true)
		if m.reduceDoneCnt == m.nReduce {
			m.done = true
		}
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	// Your code here.

	return m.done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nReduce:        nReduce,
		nMap:           len(files),
		done:           false,
		mapDoneNums:    new(sync.Map),
		reduceDoneNums: new(sync.Map),
		mapWorkCh:      make(chan *mapWork, len(files)),
		reduceWorkCh:   make(chan int, nReduce),
	}

	for i, file := range files {
		m.mapWorkCh <- &mapWork{file: file, mapNum: i}
	}
	// Your code here.

	m.server()
	return &m
}
