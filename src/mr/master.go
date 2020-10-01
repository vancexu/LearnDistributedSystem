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
	mu             sync.Mutex
	mapTasks       map[int]*Task
	curMapTaskID   int
	nReduce        int
	reduceTasks    map[int]*Task
	numMapComplete int
}

// Your code here -- RPC handlers for the worker to call.

// GetTask API for worker to get task
func (m *Master) GetTask(request *GetTaskRequest, response *GetTaskResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.numMapComplete < len(m.mapTasks) {
		for taskID, task := range m.mapTasks {
			if task.State == TaskStateIdle {
				response.TaskType = TaskTypeMap
				response.TaskID = taskID
				response.Filename = task.Filename
				response.NReduce = m.nReduce

				task.State = TaskStateClaimed
				// fmt.Println("checking worker crash ", taskID, TaskTypeMap)
				go m.checkWorkerCrash(taskID, TaskTypeMap)
				return nil
			}
		}
	} else {
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
				// fmt.Println("checking worker crash ", taskID, TaskTypeMap)
				go m.checkWorkerCrash(taskID, TaskTypeReduce)
				return nil
			}
		}
	}

	response.TaskType = TaskTypeNoTask
	return nil
}

func (m *Master) checkWorkerCrash(taskID int, taskType TaskType) {
	time.Sleep(10 * time.Second)

	m.mu.Lock()
	defer m.mu.Unlock()
	if taskType == TaskTypeMap {
		if m.mapTasks[taskID].State == TaskStateClaimed {
			m.mapTasks[m.curMapTaskID] = &Task{
				State:    TaskStateIdle,
				Filename: m.mapTasks[taskID].Filename,
			}
			m.curMapTaskID++
			delete(m.mapTasks, taskID)
		}
	} else if taskType == TaskTypeReduce {
		if m.reduceTasks[taskID].State == TaskStateClaimed {
			m.reduceTasks[taskID].State = TaskStateIdle
		}
	}
	// fmt.Println("worker checked ", taskID, taskType)
}

// CompleteTask API for complete task
func (m *Master) CompleteTask(request *CompleteTaskRequest, response *CompleteTaskResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	taskID := request.TaskID
	taskType := request.TaskType
	if taskType == TaskTypeMap {
		if _, ok := m.mapTasks[taskID]; !ok {
			return nil
		}
		m.mapTasks[taskID].State = TaskStateCompleted
		m.numMapComplete++
	} else if taskType == TaskTypeReduce {
		if _, ok := m.reduceTasks[taskID]; !ok {
			return nil
		}
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
	ret := true

	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, task := range m.reduceTasks {
		if task.State != TaskStateCompleted {
			return false
		}
	}

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
	m.reduceTasks = make(map[int]*Task)
	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i] = &Task{
			State: TaskStateIdle,
		}
	}
	m.numMapComplete = 0

	m.server()
	return &m
}
