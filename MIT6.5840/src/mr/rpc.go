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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	FileName string
	MapId    int
	R        int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /tmp, for the coordinator.
// possible to use the current directory since
// this lab is done not on Athena FileSystsem but on Unix's NTFS
func coordinatorSock() string {
	s := "/tmp/5840-mr-" // ADDED : changed this since i'm doing this lab on Arch
	s += strconv.Itoa(os.Getuid())
	return s
}
