package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
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
	reply := AskTaskReply{}
	for {
		reply = callHandleAskTask()
		switch reply.TaskType {
		case Map:
			//doMap
			log.Printf("do [ Map ] task with name %v,mMap %v, nReduce %v \n", reply.filename, reply.Mmap, reply.Nreduce)
			time.Sleep(time.Second * 1)
			// call finished task
			callHandleFinishedTask(FinishedTaskArgs{TaskType: reply.TaskType, Mmap: reply.Mmap, Nreduce: reply.Nreduce})
		case Reduce:
			log.Printf("do [ Reduce ] task with mMap %v, nReduce %v \n", reply.Mmap, reply.Nreduce)
			time.Sleep(time.Second * 1)
			// call finished task
			callHandleFinishedTask(FinishedTaskArgs{TaskType: reply.TaskType, Mmap: reply.Mmap, Nreduce: reply.Nreduce})
		case Done:
			log.Printf("do [ Done ] task  \n")
			time.Sleep(time.Second * 1)
			os.Exit(0)
		case Idle:
			log.Println("idle, sleep for 1 second")
			time.Sleep(time.Second * 1)
		default:
			log.Fatalln("cannot reach here")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}
func callHandleAskTask() AskTaskReply {
	args := AskTaskArgs{}
	reply := AskTaskReply{}
	ok := call("Coordinator.HandleAskTask", &args, &reply)
	if ok {
		return reply
	} else {
		log.Fatal("no reply from coordinator, just exit")
	}
	return reply
}
func callHandleFinishedTask(finishargs FinishedTaskArgs) {
	args := finishargs
	reply := FinishedTaskReply{}
	ok := call("Coordinator.HandleFinishedTask", &args, &reply)
	if ok {
		log.Println("task ", args, "finished")
	} else {
		log.Fatal("no reply from coordinator, just exit")
	}
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
