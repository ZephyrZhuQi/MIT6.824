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
	Y int
}

// Add your RPC definitions here.

type AllocateTaskArgs struct {
}

type AllocateTaskReply struct {
	TaskType int
	TaskNo   int
	Filename string
	NReduce  int
	NumFiles int
}

const (
	MapApplication    int = 0
	ReduceApplication int = 1
)

type FinishTaskArgs struct {
	TaskType int
	TaskNo   int
}

type FinishTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
