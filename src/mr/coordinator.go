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

// in todoMapPool and todoReducePool, a task can be any of the three states
// First assigned to Todo; when the task id is being processed, the state is Allocated
// After it's done, the status is Finished (actually delete it from the map)
const (
	Todo      int = 0
	Allocated int = 1
	Finished  int = 2
)

type Coordinator struct {
	// Your definitions here.
	// todoMapPool and todoReducePool are shared data, should be protected by mu
	todoMapPool    map[int]int  // the map tasks needed to do
	todoReducePool map[int]int  // the reduce tasks needed to do
	todoMap        map[int]bool // the map tasks needed to do
	todoReduce     map[int]bool // the reduce tasks needed to do
	files          []string
	nReduce        int
	mu             sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.
// AllocateTask allocates a task to worker
func (c *Coordinator) AllocateTask(args *AllocateTaskArgs, reply *AllocateTaskReply) error {
	c.mu.RLock()
	lenM := len(c.todoMap)
	lenR := len(c.todoReduce)
	c.mu.RUnlock()
	// reduces can't start until the last map has finished
	if lenM > 0 { // allocate map
		reply.TaskType = MapApplication

		found := false
		for k := 0; k < len(c.files); k++ {
			c.mu.RLock()
			status := c.todoMapPool[k]
			c.mu.RUnlock()
			if status == Todo {
				found = true
				reply.TaskNo = k
				reply.Filename = c.files[k]
				c.mu.Lock()
				c.todoMapPool[k] = Allocated
				c.mu.Unlock()
				break
			}
		}

		reply.NReduce = c.nReduce
		reply.NumFiles = len(c.files)
		if found {
			go func() {
				time.Sleep(10 * time.Second)
				c.mu.RLock()
				status := c.todoMapPool[reply.TaskNo]
				c.mu.RUnlock()
				if status != Finished {
					c.mu.Lock()
					c.todoMapPool[reply.TaskNo] = Todo
					c.mu.Unlock()
				}
			}()
		}
		return nil
	} else if lenR > 0 { // allocate reduce
		reply.TaskType = ReduceApplication
		found := false
		for k := 0; k < c.nReduce; k++ {
			c.mu.RLock()
			status := c.todoReducePool[k]
			c.mu.RUnlock()
			if status == Todo {
				found = true
				reply.TaskNo = k
				c.mu.Lock()
				c.todoReducePool[k] = Allocated
				c.mu.Unlock()
				break
			}
		}
		reply.NReduce = c.nReduce
		reply.NumFiles = len(c.files)
		if found {
			go func() {
				time.Sleep(10 * time.Second)
				c.mu.RLock()
				status := c.todoReducePool[reply.TaskNo]
				c.mu.RUnlock()
				if status != Finished {
					c.mu.Lock()
					c.todoReducePool[reply.TaskNo] = Todo
					c.mu.Unlock()
				}
			}()
		}
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
		c.todoMapPool[finishTaskNo] = Finished
		delete(c.todoMap, finishTaskNo)
		c.mu.Unlock()
	} else if finishTaskType == ReduceApplication {
		c.mu.Lock()
		c.todoReducePool[finishTaskNo] = Finished
		delete(c.todoReduce, finishTaskNo)
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
	c.mu.RLock()
	if len(c.todoMap) == 0 && len(c.todoReduce) == 0 {
		ret = true
	}
	c.mu.RUnlock()

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
	c.todoMapPool = make(map[int]int)
	c.todoMap = make(map[int]bool)
	for i := 0; i < len(files); i++ {
		c.todoMapPool[i] = Todo
		c.todoMap[i] = true
	}
	c.todoReducePool = make(map[int]int)
	c.todoReduce = make(map[int]bool)
	for i := 0; i < nReduce; i++ {
		c.todoReducePool[i] = Todo
		c.todoReduce[i] = true
	}
	c.files = files
	c.nReduce = nReduce
	c.server()
	return &c
}
