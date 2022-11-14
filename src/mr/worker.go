package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
			//	log.Printf(" ------------------- \n \t\t\t do [ Map ] task with name %v,mMap %v, nReduce %v \n--------------------\n", reply.Filename, reply.Mmap, reply.Nreduce)
			doMap(mapf, reply)
			// call finished task
			//	log.Printf(" ------------------- \n \t\t\t finished [ Map ] task with name %v,mMap %v, nReduce %v \n---------------------------\n", reply.Filename, reply.Mmap, reply.Nreduce)
			callHandleFinishedTask(FinishedTaskArgs{TaskType: reply.TaskType, Mmap: reply.Mmap, Nreduce: reply.Nreduce})
		case Reduce:
			//	log.Printf("------------------- \n \t\t\t do [ Reduce ] task with mMap %v, nReduce %v \n -----------------------\n", reply.Mmap, reply.Nreduce)
			doReduce(reducef, reply)
			//	log.Printf("------------------- \n \t\t\t finished [ Reduce ] task with mMap %v, nReduce %v \n-----------------\n", reply.Mmap, reply.Nreduce)
			callHandleFinishedTask(FinishedTaskArgs{TaskType: reply.TaskType, Mmap: reply.Mmap, Nreduce: reply.Nreduce})
		case Done:
			//	log.Printf("do [ Done ] task  \n")
			time.Sleep(time.Second * 1)
			os.Exit(0)
		case Idle:
			//log.Println("idle, sleep for 1 second")
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
		//log.Println("task ", args, "finished")
	} else {
		log.Fatal("no reply from coordinator, just exit")
	}
}

func doMap(mapf func(string, string) []KeyValue, reply AskTaskReply) {
	//log.Println(reply)
	if reply.TaskType != Map {
		log.Fatal("why TaskType not equal to Map?")
	}
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}

	kva := mapf(reply.Filename, string(content))

	tmpFiles := make([]*os.File, reply.Nreduce)
	encoders := make([]*json.Encoder, reply.Nreduce)
	for i := 0; i < reply.Nreduce; i++ {
		tmpFiles[i], _ = ioutil.TempFile("", "")
		encoders[i] = json.NewEncoder(tmpFiles[i])
	}
	for _, kv := range kva {
		index := ihash(kv.Key) % reply.Nreduce
		err := encoders[index].Encode(&kv)
		if err != nil {
			log.Fatal("fail to encode")
		}
	}
	prefix := "mr-" + strconv.Itoa(reply.Mmap) + "-"
	for i := 0; i < reply.Nreduce; i++ {
		os.Rename(tmpFiles[i].Name(), prefix+strconv.Itoa(i))
		tmpFiles[i].Close()
	}

}

func doReduce(reducef func(string, []string) string, reply AskTaskReply) {
	intermediate := []KeyValue{}
	for i := 0; i < reply.Mmap; i++ {
		openfilename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.Nreduce)
		file, err := os.Open(openfilename)
		if err != nil {
			log.Fatal("cannot open %v", openfilename)
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
	tmpFile, _ := ioutil.TempFile("", "")
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
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	os.Rename(tmpFile.Name(), "mr-out-"+strconv.Itoa(reply.Nreduce))

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
