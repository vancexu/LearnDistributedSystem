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
	mu              sync.Mutex
	mapTasks        map[int]*Task
	curMapTaskID    int
	nReduce         int
	reduceTasks     map[int]*Task
	curReduceTaskID int
}

// Your code here -- RPC handlers for the worker to call.

// GetTask API for worker to get task
func (m *Master) GetTask(request *GetTaskRequest, response *GetTaskResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	areMapTasksCompleted := true
	for taskID, task := range m.mapTasks {
		if task.State == TaskStateIdle {
			response.TaskType = TaskTypeMap
			response.TaskID = taskID
			response.Filename = task.Filename
			response.NReduce = m.nReduce
			// go checkWorker()
			task.State = TaskStateClaimed
			return nil
		}
		if task.State != TaskStateCompleted {
			areMapTasksCompleted = false
		}
	}

	if areMapTasksCompleted { // schedule reduce task
		var mapTaskIDs []int
		for taskID := range m.mapTasks {
			mapTaskIDs = append(mapTaskIDs, taskID)
		}

		for taskID, task := range m.reduceTasks {
			if task.State == TaskStateIdle {
				response.TaskType = TaskTypeReduce
				response.TaskID = taskID
				response.MapTaskIDs = mapTaskIDs

				task.State = TaskStateClaimed
				return nil
			}
		}

	}

	return nil
}

// CompleteTask API for complete task
func (m *Master) CompleteTask(request *CompleteTaskRequest, response *CompleteTaskResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	taskID := request.TaskID
	taskType := request.TaskType
	if taskType == TaskTypeMap {
		m.mapTasks[taskID].State = TaskStateCompleted
	} else if taskType == TaskTypeReduce {
		m.reduceTasks[taskID].State = TaskStateCompleted
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
		break // todo remove
	}
	m.curReduceTaskID = 0
	m.reduceTasks = make(map[int]*Task)
	for m.curMapTaskID < nReduce {
		m.reduceTasks[m.curReduceTaskID] = &Task{
			State: TaskStateIdle,
		}
		m.curReduceTaskID++
		break // todo remove
	}

	m.server()
	return &m
}
