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
	todoMapPool    map[int]bool // the map tasks needed to do
	todoReducePool map[int]bool // the reduce tasks needed to do
	files          []string
	nReduce        int
	mu             sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
// AllocateTask allocates a task to worker
func (c *Coordinator) AllocateTask(args *AllocateTaskArgs, reply *AllocateTaskReply) error {
	c.mu.Lock()
	lenM := len(c.todoMapPool)
	lenR := len(c.todoReducePool)
	c.mu.Unlock()
	if lenM > 0 { // allocate map
		reply.TaskType = MapApplication
		c.mu.Lock()
		for k := range c.todoMapPool {
			reply.TaskNo = k
			reply.Filename = c.files[k]
			break
		}
		c.mu.Unlock()
		reply.NReduce = c.nReduce
		reply.NumFiles = len(c.files)
		return nil
	} else if lenR > 0 { // allocate reduce
		reply.TaskType = ReduceApplication
		c.mu.Lock()
		for k := range c.todoReducePool {
			reply.TaskNo = k
			break
		}
		c.mu.Unlock()
		reply.NReduce = c.nReduce
		reply.NumFiles = len(c.files)
		return nil
	} else {
		return nil
	}
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	finishTaskNo := args.TaskNo
	finishTaskType := args.TaskType
	if finishTaskType == MapApplication {
		c.mu.Lock()
		delete(c.todoMapPool, finishTaskNo)
		c.mu.Unlock()
	} else if finishTaskType == ReduceApplication {
		c.mu.Lock()
		delete(c.todoReducePool, finishTaskNo)
		c.mu.Unlock()
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	if len(c.todoMapPool) == 0 && len(c.todoReducePool) == 0 {
		ret = true
	}
	c.mu.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.todoMapPool = make(map[int]bool)
	for i := 0; i < len(files); i++ {
		c.todoMapPool[i] = true
	}
	c.todoReducePool = make(map[int]bool)
	for i := 0; i < nReduce; i++ {
		c.todoReducePool[i] = true
	}
	c.files = files
	c.nReduce = nReduce
	c.server()
	return &c
}
