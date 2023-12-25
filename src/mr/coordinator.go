package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	nReduce        int // number of reduce task
	nMap           int // number of map task
	files          []string
	mapfinished    int        // number of finished map task
	maptasklog     []int      // log for map task, 0: not allocated, 1: waiting, 2:finished
	reducefinished int        // number of finished map task
	reducetasklog  []int      // log for reduce task
	mu             sync.Mutex // lock
}

// Your code here -- RPC handlers for the worker to call.
// guarantue the atmoic
func (c *Coordinator) FinishedMap(args *WorkerArgs, argv *WorkerReply) {
	c.mu.Lock()
	c.mapfinished += 1
	c.maptasklog[args.MapTaskNum] = 2
	c.mu.Unlock()
}

func (c *Coordinator) FinishedReduce(args *WorkerArgs, argv *WorkerReply) {
	c.mu.Lock()
	c.reducefinished += 1
	c.reducetasklog[argv.ReduceTaskNum] = 2
	c.mu.Unlock()
}

func (c *Coordinator) AllowTask(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	if c.mapfinished < c.nMap {
		// allocate map tasks

	} else if c.reducefinished < c.nReduce {

	} else {
		reply.TaskType = 3
		c.mu.Unlock()
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := c.reducefinished == c.nReduce

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// initialism
	c := Coordinator{}
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.mapfinished = 0
	c.maptasklog = make([]int, c.nMap)
	c.reducetasklog = make([]int, c.nReduce)
	// Your code here.
	c.server()
	return &c
}
