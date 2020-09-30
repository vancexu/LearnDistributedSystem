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
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	for {
		request := GetTaskRequest{}
		response := GetTaskResponse{}
		isServerAlive := call("Master.GetTask", &request, &response)
		if !isServerAlive {
			break
		}

		fmt.Println("getTask: ", response.TaskType, response.Filename)
		if response.TaskType == TaskTypeMap {
			executeMap(&response, mapf)
		} else if response.TaskType == TaskTypeReduce {
			executeReduce(&response, reducef)
		} else if response.TaskType == TaskTypeNoTask {
			time.Sleep(3 * time.Second)
			continue
		}

		compRequest := CompleteTaskRequest{
			TaskType: response.TaskType,
			TaskID:   response.TaskID,
		}
		compResponse := CompleteTaskResponse{}
		call("Master.CompleteTask", &compRequest, &compResponse)
		fmt.Println("completeTask: ", compRequest.TaskType, compRequest.TaskID)
	}
}

func executeMap(resp *GetTaskResponse, mapf func(string, string) []KeyValue) {
	filename := resp.Filename
	nReduce := resp.NReduce
	taskID := resp.TaskID

	intermediate := make(map[int][]KeyValue)

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
	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		intermediate[bucket] = append(intermediate[bucket], kv)
	}
	// output to disk
	for key, val := range intermediate {
		ofileName := getFilename(taskID, key)
		ofile, err := json.MarshalIndent(val, "", " ")
		if err != nil {
			log.Fatalf("cannot marshall content %v: %v", key, val)
		}
		err = ioutil.WriteFile(ofileName, ofile, 0644)
		if err != nil {
			log.Fatalf("cannot write to file %v", ofile)
		}
	}
}

func getFilename(mapTaskID, reduceTaskID int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskID, reduceTaskID)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func executeReduce(resp *GetTaskResponse, reducef func(string, []string) string) {
	taskID := resp.TaskID
	mapTaskIDs := resp.MapTaskIDs

	var intermediate []KeyValue
	for _, mapTaskID := range mapTaskIDs {
		filename := getFilename(mapTaskID, taskID)
		file, err := ioutil.ReadFile(filename)
		if err != nil {
			log.Fatalf("cannot read file %v", filename)
		}
		var data []KeyValue
		err = json.Unmarshal([]byte(file), &data)
		if err != nil {
			log.Fatalf("cannot unmarshall content for file %v: %v", filename, data)
		}
		intermediate = append(intermediate, data...)
	}

	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", taskID)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot write file %v", oname)
	}

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
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
