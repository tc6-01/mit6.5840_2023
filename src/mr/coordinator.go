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
func (c *Coordinator) ReceiveMapFinished(args *WorkerArgs, argv *ExampleReply) error {
	c.mu.Lock()
	c.mapfinished += 1
	c.maptasklog[args.MapTaskNum] = 2
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) ReceiveReduceFinished(args *WorkerArgs, reply *ExampleReply) error {
	c.mu.Lock()
	c.reducefinished += 1
	c.reducetasklog[args.ReduceTaskNum] = 2
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) AllowTask(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	// fmt.Println("master启动，开始分配任务")
	// time.Sleep(time.Duration(10) * time.Second)
	if c.mapfinished < c.nMap {
		// allocate map tasks
		// get the task location
		allocate := -1
		for i := 0; i < c.nMap; i++ {
			if c.maptasklog[i] == 0 {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			// all task finished
			reply.TaskType = 2
			c.mu.Unlock()
		} else {
			reply.Nreduce = c.nReduce
			reply.MapTaskNum = allocate
			reply.TaskType = 0
			reply.FileName = c.files[allocate]
			c.maptasklog[allocate] = 1
			c.mu.Unlock()
			// check the status of Worker
			go func() {
				time.Sleep(time.Duration(4) * time.Second)
				c.mu.Lock()
				if c.maptasklog[allocate] == 1 {
					c.maptasklog[allocate] = 0
				}
				c.mu.Unlock()
			}()
		}
	} else if c.mapfinished == c.nMap && c.reducefinished < c.nReduce {
		// after all map task finished, reduce task will be allocated!
		allocate := -1
		for i := 0; i < c.nReduce; i++ {
			if c.reducetasklog[i] == 0 {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			// all task finished
			reply.TaskType = 2
			c.mu.Unlock()
		} else {
			reply.ReduceTaskNum = allocate
			reply.TaskType = 1
			reply.Nmap = c.nMap
			c.reducetasklog[allocate] = 1
			c.mu.Unlock()
			// check the status of Worker
			go func() {
				time.Sleep(time.Duration(4) * time.Second)
				c.mu.Lock()
				if c.reducetasklog[allocate] == 1 {
					c.reducetasklog[allocate] = 0
				}
				c.mu.Unlock()
			}()
		}
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
