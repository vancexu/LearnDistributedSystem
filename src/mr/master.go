package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type State int

type Task struct {
	State    State
	Filename string
}

const (
	TaskStateIdle State = iota
	TaskStateClaimed
	TaskStateCompleted
)

type Master struct {
	// Your definitions here.
	mu           sync.Mutex
	mapTasks     map[int]*Task
	curMapTaskID int
	nReduce      int
}

// Your code here -- RPC handlers for the worker to call.

// GetTask API for worker to get task
func (m *Master) GetTask(request *GetTaskRequest, response *GetTaskResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for taskID, task := range m.mapTasks {
		if task.State == TaskStateIdle {
			response.TaskType = TaskTypeMap
			response.TaskID = taskID
			response.Filename = task.Filename
			response.NReduce = m.nReduce
			// go checkWorker()
			return nil
		}
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	m.curMapTaskID = 0
	m.mapTasks = make(map[int]*Task)
	m.nReduce = nReduce
	for _, file := range files {
		m.mapTasks[m.curMapTaskID] = &Task{
			State:    TaskStateIdle,
			Filename: file,
		}
		m.curMapTaskID++
	}

	m.server()
	return &m
}
