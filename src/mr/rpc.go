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

const (
	Map    int = 0
	Reduce int = 1
	Done   int = 2
	Idle   int = 3 // ask the worker to sleep a while, but no to exit
)

// Add your RPC definitions here.
type AskTaskArgs struct {
}

type AskTaskReply struct {
	TaskType int    // which type of task
	Mmap     int    // index of map task
	Nreduce  int    // index of reduce task
	filename string // for map task to read a file
}
type FinishedTaskArgs struct {
	TaskType int
	Mmap     int
	Nreduce  int
}
type FinishedTaskReply struct {
	// don't care
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
