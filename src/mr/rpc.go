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
// master only focus on the task unfinished
type WorkerArgs struct {
	MapTaskNum    int // unfinished map tasks
	ReduceTaskNum int // unfinished reduce tasks
}

// some map or reduce tasks return
type WorkerReply struct {
	TaskType      int // 0: map task, 1: reduce task, 2: waiting, 3: job finished
	MapTaskNum    int
	ReduceTaskNum int
	FileName      string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
