package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
// Here main usage is for the related work between master & worker
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// ask the coordinator for a task,
	// read the task's input from one or more files,
	// execute the task,
	// write the task's output to one or more files.
	// send finish message
	for {
		args := WorkerArgs{}
		reply := WorkerReply{}
		ok := call("Coordinator.AllowTask", &args, &reply)
		if !ok || reply.TaskType == 3 {
			break
		}
		if reply.TaskType == 0 {
			//do Map task
			intermediate := []KeyValue{}
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("open file error %v", reply.FileName)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.FileName)
			}
			file.Close()

			// call mapf
			kva := mapf(reply.FileName, string(content))
			intermediate = append(intermediate, kva...)

			// send into reduce
			buckets := make([][]KeyValue, reply.Nreduce)
			for i := range buckets {
				buckets[i] = []KeyValue{}
			}
			for _, kva := range intermediate {
				buckets[ihash(kva.Key)%reply.Nreduce] = append(buckets[ihash(kva.Key)%reply.Nreduce], kva)
			}
			// Write them into disk
			for i := range buckets {
				oname := "mr-" + strconv.Itoa(reply.MapTaskNum) + "-" + strconv.Itoa(i)
				ofile, _ := os.CreateTemp("", oname+"*")
				enc := json.NewEncoder(ofile)
				for _, kva := range buckets[i] {
					err = enc.Encode(&kva)
					if err != nil {
						log.Fatalf("cannot write file %v", oname)
					}
				}
				os.Rename(ofile.Name(), oname)
				ofile.Close()
			}
			// send finish map message
			finishedArgs := WorkerArgs{reply.MapTaskNum, -1}
			finishedReply := ExampleReply{}
			call("Coordinator.ReceiveMapFinished", &finishedArgs, &finishedReply)
		} else if reply.TaskType == 1 {
			// do reduce task
			intermediate := []KeyValue{}
			for i := 0; i < reply.Nmap; i++ {
				iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceTaskNum)
				file, err := os.Open(iname)
				if err != nil {
					log.Fatalf("cannot open file %v", reply.FileName)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(intermediate))
			oname := "mr-out-" + strconv.Itoa(reply.ReduceTaskNum)
			ofile, _ := os.CreateTemp("", oname+"*")

			// call reducef and merge keys into files
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
				output := reducef(intermediate[i].Key, values)
				// log.Println(intermediate[i].Key, output)
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			os.Rename(ofile.Name(), oname)
			ofile.Close()
			// delete the intermediate file
			for i := 0; i < reply.Nmap; i++ {
				iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceTaskNum)
				err := os.Remove(iname)
				if err != nil {
					log.Fatalf("cannot remove file %v", iname)
				}
			}
			// send reduce task finished message
			finishedArgs := WorkerArgs{-1, reply.ReduceTaskNum}
			finishedReply := ExampleReply{}
			call("Coordinator.ReceiveReduceFinished", &finishedArgs, &finishedReply)
		}
		time.Sleep(time.Second)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

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
