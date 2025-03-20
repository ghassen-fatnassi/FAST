package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// doing the rpc here
	args := ExampleArgs{}
	reply := ExampleReply{}
	ok := call("Coordinator.GiveTask", &args, &reply)
	if ok {
		fmt.Printf("reply.Y %v\n", reply.FileName)
	} else {
		fmt.Printf("RPC failed\n")
	}
	// got rpc output in reply , gonna do the map now
	filename := reply.FileName
	fmt.Printf("the file: %v\n", filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Error: can't open file %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Error: can't read file %v", filename)
	}
	file.Close()
	kvs := mapf(filename, string(content))
	s := "Mr-"
	s += strconv.Itoa(reply.MapId)
	json_file, err := os.Create(s)
	if err != nil {
		log.Fatalf("Error: couldn't create file  %v", filename)
	}
	encoder := json.NewEncoder(json_file)
	fmt.Println(kvs[0])
	fmt.Println(encoder)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
