package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

/*
	what information do we need to perform a map or reduce task ?
		for map task:
				filename                   -> as input
				m, index of which map task -> as part of intermediate filename -> "mr-m-n"
				n, number of reduce task   -> as part of intermediate filename -> "mr-m-n"

		for reduce task:
				m, index of which map task -> as part of intermediate filename -> "mr-m-n"
				n, number of reduce task   -> as part of intermediate filename -> "mr-m-n"

*/

type Coordinator struct {
	// Your definitions here.
	filename           []string
	mMap               int
	nReduce            int
	mapTaskIssued      []time.Time
	mapTaskFinished    []bool
	isMapTasksDone     bool
	reduceTaskIssued   []time.Time
	reduceTaskFinished []bool
	isAllDone          bool

	mu sync.Mutex
}

func (c *Coordinator) checkAllDone() bool {
	// do something to check is all tasks done
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isAllDone {
		//	log.Println("check [ All ] tasks done: ", true)
		return true
	}
	// only check reduce tasks, we will not issue reduce task before all map tasks finished
	flag := true
	for _, done := range c.reduceTaskFinished {
		if !done {
			flag = false
			break
		}
	}
	c.isAllDone = flag
	//log.Println("check [ All ] tasks done: ", c.isAllDone)
	return c.isAllDone
}

func (c *Coordinator) checkMapTasksDone() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	// check wheather all map tasks done ?
	if c.isMapTasksDone {
		//		log.Println("check [ Map ] tasks done: ", c.mapTaskFinished)
		return true
	}

	flag := true
	for _, done := range c.mapTaskFinished {
		if !done {
			flag = false
			break
		}
	}
	c.isMapTasksDone = flag
	// log.Println("check [ Map ] tasks done: ", c.mapTaskFinished)
	return c.isMapTasksDone
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
func (c *Coordinator) HandleAskTask(args *AskTaskArgs, reply *AskTaskReply) error {

	allDoneFlag := c.checkAllDone()
	allMapDoneFlag := c.checkMapTasksDone()

	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		// if we are done, just ask the RPC clien to exit
		if allDoneFlag {
			reply.TaskType = Done
			return nil
		} else if allMapDoneFlag {
			// if we finished all map tasks, we are going to deal with reduce task
			//	log.Println("going to issue reduce tasks")
			reply.TaskType = Reduce
			// check weather there are not finished task
			for n, done := range c.reduceTaskFinished {
				// if this reduce task not finished, 2 cases: not issued or time out
				if !done {
					if c.reduceTaskIssued[n].IsZero() || time.Since(c.reduceTaskIssued[n]).Seconds() > 10 {
						// we are going to reissue this reduce task
						reply.Nreduce = n
						reply.Mmap = c.mMap
						c.reduceTaskIssued[n] = time.Now() // renew timer
						return nil
					}
				}
			}
			// can not find reduce task, ask the worker to sleep
			reply.TaskType = Idle
			return nil

		} else {
			// here, we are going to issue all map tasks
			reply.TaskType = Map
			for m, done := range c.mapTaskFinished {
				if !done {
					if c.mapTaskIssued[m].IsZero() || time.Since(c.mapTaskIssued[m]).Seconds() > 10 {
						reply.Filename = c.filename[m]
						reply.Nreduce = c.nReduce
						reply.Mmap = m

						c.mapTaskIssued[m] = time.Now()
						//	log.Println("map task [ index:", m, " name: ", c.filename[m], " ] issued")
						return nil
					}
				}
			}
			// can not find reduce task, ask the worker to sleep
			reply.TaskType = Idle
			return nil
		}
	}
	// we can not reach here
	log.Fatalln("reach end of HandleAskTask")
	return errors.New("this message should not appear")
}

func (c *Coordinator) HandleFinishedTask(args *FinishedTaskArgs, reply *FinishedTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	//	log.Printf("--------------- HandleFinishedTask -------------\n")
	//	log.Println(args)
	switch args.TaskType {
	case Map:
		c.mapTaskFinished[args.Mmap] = true
		return nil
	case Reduce:
		c.reduceTaskFinished[args.Nreduce] = true
		return nil
	default:
		log.Fatalln("HandleFinishedTask with unkown tasktype, this should not appear")
	}
	return errors.New("End of HandleFinishedTask: this message should not appear")

}

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
	c.mu.Lock()
	ret := c.isAllDone
	c.mu.Unlock()
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.filename = make([]string, len(files))
	c.mapTaskFinished = make([]bool, len(files))
	c.reduceTaskFinished = make([]bool, nReduce)
	c.mapTaskIssued = make([]time.Time, len(files))
	c.reduceTaskIssued = make([]time.Time, nReduce)
	c.mMap = len(files)
	c.nReduce = nReduce
	c.isAllDone = false
	c.isMapTasksDone = false

	c.filename = files
	for i := 0; i < c.mMap; i++ {
		c.mapTaskFinished[i] = false
	}
	for i := 0; i < c.nReduce; i++ {
		c.reduceTaskFinished[i] = false
	}
	// log.Println("Initilization finished, content is as following:")
	//for index, filename := range c.filename {
	//		log.Println("	index:", index, "filename:", filename, "finished:", c.mapTaskFinished[index], "time:", c.mapTaskIssued[index])
	// }
	c.server()
	return &c
}
