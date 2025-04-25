package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
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

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	args := Args{}
	reply := Reply{}

	// Request a task from the coordinator
	ok := call("Coordinator.GiveTask", &args, &reply)
	if !ok {
		fmt.Println("RPC failed")
		return
	}
	if reply.FileName == "" {
		log.Fatal("mapQ is empty")
	}
	fmt.Printf("Received task: %v\n", reply.FileName)

	filename := reply.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Error: can't open file %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Error: can't read file %v", filename)
	}
	file.Close()

	kvs := mapf(filename, string(content)) // Run map function

	// Organize key-value pairs by reducer
	reducerBuckets := make(map[int][]KeyValue)
	for i, kv := range kvs {
		reduceTask := (i * reply.R) / len(kvs)
		if reduceTask >= reply.R {
			reduceTask = reply.R - 1
		}
		reducerBuckets[reduceTask] = append(reducerBuckets[reduceTask], kv)
	}

	// Write each bucket to a separate JSON file as an array
	for reduceTask, kvList := range reducerBuckets {
		jsonFileName := fmt.Sprintf("mr-%d-%d.json", reply.MapId, reduceTask)

		jsonFile, err := os.OpenFile(jsonFileName, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatalf("Error: couldn't create/open file %v", jsonFileName)
		}

		encoder := json.NewEncoder(jsonFile)
		err = encoder.Encode(kvList) // Write full list as a JSON array
		if err != nil {
			log.Fatalf("Error writing to JSON: %v", err)
		}

		jsonFile.Close()
	}
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
