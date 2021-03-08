package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapFun func(string, string) []KeyValue,
	reduceFun func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		taskReply, err := ReqTask()
		if err != nil {
			os.Exit(0)
		}
		switch taskReply.TaskType {
		case MapTask:
			execMap(taskReply, mapFun)
		case ReduceTask:
			execReduce(taskReply, reduceFun)
		}
	}
}

func execMap(taskReply *ReqTaskReply, mapFun func(string, string) []KeyValue) {
	fileName := taskReply.FileName
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}

	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}

	kva := mapFun(fileName, string(content))
	writeIntermediate(kva, taskReply.TaskNum, taskReply.ReduceCnt)
	workDone(taskReply.TaskType, taskReply.TaskNum)
}

func writeIntermediate(kva []KeyValue, mapNum, reduceCnt int) {
	files := make([]*AtomicRenameFile, reduceCnt)
	for i := 0; i < reduceCnt; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", mapNum, i)
		file, err := NewAtomicRenameFile(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}

		files[i] = file
	}

	for _, kv := range kva {
		reduceNum := ihash(kv.Key) % reduceCnt
		if err := json.NewEncoder(files[reduceNum]).Encode(&kv); err != nil {
			log.Fatal("cannot write intermediate key value")
		}
	}

	for _, file := range files {
		file.Close()
	}
}

func execReduce(taskReply *ReqTaskReply, reduceFun func(string, []string) string) {
	intermediate := parseIntermediateData(taskReply.MapCnt, taskReply.TaskNum)
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	fileName := fmt.Sprintf("mr-out-%d", taskReply.TaskNum)
	outFile, err := NewAtomicRenameFile(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reduceFun(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	outFile.Close()
	workDone(taskReply.TaskType, taskReply.TaskNum)
}

func parseIntermediateData(mapCnt, reduceNum int) []KeyValue {
	var result []KeyValue
	for mapNum := 0; mapNum < mapCnt; mapNum++ {
		fileName := fmt.Sprintf("mr-%d-%d", mapNum, reduceNum)
		if !fileExists(fileName) {
			continue
		}

		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			result = append(result, kv)
		}
	}
	return result
}

func workDone(taskType, taskNum int) {
	args := &WorkDoneArgs{
		TaskType: taskType,
		TaskNum:  taskNum,
	}
	call("Master.WorkDone", args, &WorkDoneReplay{})
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

// ReqTask request a map/reduce task from master
func ReqTask() (*ReqTaskReply, error) {
	reply := &ReqTaskReply{}
	if ok := call("Master.ReqTask", &ReqTaskArgs{}, &reply); !ok {
		return nil, errors.New("call rpc error")
	}
	return reply, nil
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
