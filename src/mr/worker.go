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
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {

	for {
		// Your worker implementation here.
		ret, TaskType, TaskNo, filename, nReduce, numFiles := CallAllocateTask()
		if !ret {
			return false
		}
		// uncomment to send the Example RPC to the coordinator.
		// CallExample()

		if TaskType == MapApplication {
			ret = WorkerMap(mapf, TaskNo, filename, nReduce)
			if !ret {
				return false
			}
		} else if TaskType == ReduceApplication {
			ret = WorkerReduce(reducef, TaskNo, numFiles)
			if !ret {
				return false
			}
		}
	}

}

func WorkerMap(mapf func(string, string) []KeyValue, TaskNo int, filename string, nReduce int) bool {
	// read the file and call the application Map function
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	// print the intermediate output to file: mr-0-0
	// oname := "mr-0-0"
	// for outputId := 0; outputId < nReduce; outputId++ {
	// 	oname := "mr-" + strconv.Itoa(TaskNo) + "-" + strconv.Itoa(outputId)
	// 	ofile, _ := os.Create(oname)
	// 	ofile.Close()
	// }

	outputs := make([][]KeyValue, nReduce)
	for i := range outputs {
		outputs[i] = make([]KeyValue, 0)
	}
	for i := 0; i < len(intermediate); i++ {
		outputId := ihash(intermediate[i].Key) % nReduce
		outputs[outputId] = append(outputs[outputId], intermediate[i])
	}

	for outputId := 0; outputId < nReduce; outputId++ {
		oname := "mr-" + strconv.Itoa(TaskNo) + "-" + strconv.Itoa(outputId)
		// ofile, _ := os.OpenFile(oname, os.O_APPEND|os.O_WRONLY, 0644)
		// os.O_APPEND leads to error!
		ofile, _ := os.OpenFile(oname, os.O_CREATE|os.O_WRONLY, 0644)
		enc := json.NewEncoder(ofile)
		for _, kv := range outputs[outputId] {
			tmp := map[string]string{kv.Key: kv.Value}
			err := enc.Encode(&tmp)
			if err != nil {
				fmt.Printf("Error: %v", err)
				log.Fatalf("cannot encode %v", tmp)
			}
		}
		ofile.Close()
	}
	ret := CallFinishTask(MapApplication, TaskNo)
	if !ret {
		return false
	} else {
		return true
	}
}

func WorkerReduce(reducef func(string, []string) string, TaskNo int, numFiles int) bool {
	intermediate := []KeyValue{}
	for i := 0; i < numFiles; i++ {
		oname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(TaskNo)
		file, err := os.Open(oname)
		if err != nil {
			log.Fatalf("cannot open %v", oname)
		}
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			var tmp map[string]string
			if err := dec.Decode(&tmp); err != nil {
				break
			}
			for k, v := range tmp {
				kv.Key = k
				kv.Value = v
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
		// kva := mapf(filename, string(content))
		// intermediate = append(intermediate, kva...)
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(TaskNo)
	ofile, _ := os.Create(oname)

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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	ret := CallFinishTask(ReduceApplication, TaskNo)
	if !ret {
		return false
	} else {
		return true
	}
}

func CallAllocateTask() (bool, int, int, string, int, int) {
	args := AllocateTaskArgs{}
	reply := AllocateTaskReply{}
	ret := call("Coordinator.AllocateTask", &args, &reply)
	if !ret {
		return false, reply.TaskType, reply.TaskNo, reply.Filename, reply.NReduce, reply.NumFiles
	}
	// fmt.Printf("reply.Filename %v\n", reply.Filename)
	return true, reply.TaskType, reply.TaskNo, reply.Filename, reply.NReduce, reply.NumFiles
}

func CallFinishTask(TaskType int, TaskNo int) bool {
	args := FinishTaskArgs{}
	args.TaskType = TaskType
	args.TaskNo = TaskNo
	reply := FinishTaskReply{}
	ret := call("Coordinator.FinishTask", &args, &reply)
	if !ret {
		return false
	} else {
		return true
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
