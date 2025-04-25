package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Queue struct {
	elems    []string
	head     int
	tail     int
	size     int
	capacity int
}

// Create a new queue with a fixed capacity
func NewQueue(capacity int) *Queue {
	return &Queue{
		elems:    make([]string, capacity),
		head:     0,
		tail:     capacity - 1,
		size:     capacity,
		capacity: capacity,
	}
}

// Enqueue (Add an item to the queue)
func (q *Queue) Enqueue(item string) bool {
	if q.IsFull() {
		return false // Queue is full
	}
	q.elems[q.tail] = item
	q.tail = (q.tail + 1) % q.capacity
	q.size++
	return true
}

// Dequeue (Remove and return the front item)
func (q *Queue) Dequeue() (string, bool) {
	if q.IsEmpty() {
		return "", false // Queue is empty
	}
	item := q.elems[q.head]
	q.head = (q.head + 1) % q.capacity
	q.size--
	return item, true
}

// Check if the queue is empty
func (q *Queue) IsEmpty() bool {
	return q.size == 0
}

// Check if the queue is full
func (q *Queue) IsFull() bool {
	return q.size == q.capacity
}

type Coordinator struct {
	mapQ Queue
	R    int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GiveTask(args *Args, reply *Reply) error {
	curr_file, good := c.mapQ.Dequeue()
	if !good {
		fmt.Print("mapQueue is empty")
		return nil
	}
	reply.FileName = curr_file
	reply.MapId = c.mapQ.capacity - c.mapQ.size
	reply.R = c.R
	fmt.Print(reply)

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mapQ = *NewQueue(len(files))
	c.mapQ.elems = files
	c.R = nReduce
	// Your code here.
	c.server()
	return &c
}
