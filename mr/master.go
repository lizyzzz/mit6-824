package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	NO_TASK          = 0
	MAP_TASK         = 1
	REDUCE_TASK      = 2
	MAP_TASK_FAIL    = -1
	REDUCE_TASK_FAIL = -2
	SLEEP_TASK       = 3
)

type Master struct {
	// Your definitions here.
	isReduceDone   bool
	isMapDone      bool
	nReduce        int
	mapTask        map[string]int // value: task number (负数表示已分发)
	mapTaskLock    sync.Mutex
	reduceTask     map[string]int // value: task number (负数表示已分发)
	reduceBucket   map[int]bool   // 记录每个未完成的桶任务
	reduceTaskLock sync.Mutex

	// 后台线程: 如果 worker 没有在规定时间内完成则把任务恢复
	timeOutSec uint32
}

// 获取 reduce 任务时的转化
func (m *Master) StrToNumber(str string) int {
	// 将 'mr-X-Y' 中的 Y 返回
	idx := strings.LastIndex(str, "-")
	strNum := str[idx+1:]
	num, _ := strconv.Atoi(strNum)
	return num
}

// 获取中间文件名称
func NumberToStr(X, Y int) string {
	interFilename := "mr-" + strconv.Itoa(X) + "-" + strconv.Itoa(Y)
	return interFilename
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 请求任务的 rpc handler
func (m *Master) GetTask(taskReq *TaskRequest, taskResp *TaskResponse) error {
	tasks := []*AssignTask{}
	// 获取 map task
	m.mapTaskLock.Lock()
	for filename, num := range m.mapTask {
		if num > 0 {
			// 存在待分发的任务
			task := &AssignTask{
				FileName: filename,
				TaskId:   num,
			}
			tasks = append(tasks, task)
			break
		}
	}
	if len(tasks) > 0 {
		taskResp.TaskType = MAP_TASK // map 任务
		for _, task := range tasks {
			m.mapTask[task.FileName] = -task.TaskId // 任务已分发
			// fmt.Printf("task: %d has assign.\n", task.TaskId)
		}
		go m.recoverRequest(tasks, MAP_TASK) // 任务失败时重新分发
	}
	m.mapTaskLock.Unlock()

	// map task 已全部完成
	if len(tasks) == 0 && m.isMapDone {
		// 获取 reduce task
		number := -1
		// 查找待完成的 reduce task
		m.reduceTaskLock.Lock()
		for num, flag := range m.reduceBucket {
			if flag {
				// 存在待分发的任务
				number = num
				break
			}
		}

		if number > 0 {
			// 存在未完成的 reduce 任务
			taskResp.TaskType = REDUCE_TASK
			m.reduceBucket[number] = false // 任务已分发
			// reduce 任务会存在读多个文件
			for filename, num := range m.reduceTask {
				if num == number {
					task := &AssignTask{
						FileName: filename,
						TaskId:   num,
					}
					tasks = append(tasks, task)
				}
			}
			for _, task := range tasks {
				m.reduceTask[task.FileName] = -task.TaskId // 任务已分发
				// fmt.Printf("map tasks are done, reduce task has assign: %v\n", task.FileName)
			}
			if len(tasks) > 0 {
				go m.recoverRequest(tasks, REDUCE_TASK) // 任务失败时重新分发
			}
		}

		m.reduceTaskLock.Unlock()
	}

	if len(tasks) > 0 {
		taskResp.Tasks = tasks
	} else {
		// fmt.Println("no task assign")
		taskResp.TaskType = NO_TASK          // 无任务
		taskResp.TimeSleep = 3 * time.Second // 无任务时休眠 3 s
	}
	taskResp.NReduce = m.nReduce

	return nil
}

// 任务完成时的 rpc handler
func (m *Master) TaskFinished(finishReq *FinishedRequest, finishResp *FinishedResponse) error {
	switch finishReq.FinishedType {
	case MAP_TASK:
		// 完成 map 任务

		// 删除已完成的 map 任务
		m.mapTaskLock.Lock()
		for _, task := range finishReq.Tasks {
			delete(m.mapTask, task.FileName)
			// fmt.Printf("task: %v \n", *task)
			// fmt.Printf("map task %s is done, Bucket Num: %v\n", task.FileName, task.BucketNums)
		}
		if len(m.mapTask) == 0 {
			// fmt.Println("map task done")
			m.isMapDone = true
		}
		m.mapTaskLock.Unlock()

		// 添加待完成的 reduce 任务
		m.reduceTaskLock.Lock()
		for _, task := range finishReq.Tasks {
			for _, bucketNum := range task.BucketNums {
				interFilename := NumberToStr(task.TaskId, bucketNum) // 中间文件名
				// fmt.Printf("add reduce task: %s\n", interFilename)
				m.reduceBucket[bucketNum] = true        // 待分发
				m.reduceTask[interFilename] = bucketNum // 待分发
			}
		}
		m.reduceTaskLock.Unlock()

	case REDUCE_TASK:
		// 完成 reduce 任务

		// 删除已完成的 reduce 任务
		m.reduceTaskLock.Lock()
		delete(m.reduceBucket, finishReq.Tasks[0].TaskId)
		for _, task := range finishReq.Tasks {
			// fmt.Printf("reduce task %s is done, Bucket Num: %v\n", task.FileName, task.BucketNums)
			delete(m.reduceTask, task.FileName)
		}
		if len(m.reduceTask) == 0 {
			// fmt.Println("reduce task done")
			m.isReduceDone = true
		}
		m.reduceTaskLock.Unlock()
	case MAP_TASK_FAIL:
		// map 任务失败
		m.mapTaskLock.Lock()
		for _, task := range finishReq.Tasks {
			// fmt.Printf("map task %s is fail\n", task.FileName)
			m.mapTask[task.FileName] = task.TaskId // 待分发
		}
		m.mapTaskLock.Unlock()
	case REDUCE_TASK_FAIL:
		// reduce 任务失败
		m.reduceTaskLock.Lock()
		m.reduceBucket[finishReq.Tasks[0].TaskId] = true // 待分发
		for _, task := range finishReq.Tasks {
			// fmt.Printf("map task %s is fail\n", task.FileName)
			m.reduceTask[task.FileName] = task.TaskId // 待分发
		}
		m.reduceTaskLock.Unlock()
	case SLEEP_TASK:
		// 什么都不做
	}
	finishResp.CanExit = false
	// 全部任务已完成
	if m.isReduceDone {
		finishResp.CanExit = true
	}
	return nil
}

func (m *Master) recoverRequest(tasks []*AssignTask, taskType int) {
	// 休眠
	time.Sleep(time.Duration(m.timeOutSec) * time.Second)

	if taskType == MAP_TASK {
		m.mapTaskLock.Lock()
		ok := false
		for _, task := range tasks {
			_, ok := m.mapTask[task.FileName]
			if ok {
				break
			}
		}
		if ok {
			// 任务中的文件还存在, 任务已经失败
			for _, task := range tasks {
				m.mapTask[task.FileName] = task.TaskId // 待分发
			}
		}
		// 任务完成则什么都不做
		m.mapTaskLock.Unlock()
	} else if taskType == REDUCE_TASK {
		m.reduceTaskLock.Lock()
		_, ok := m.reduceBucket[tasks[0].TaskId]
		if ok {
			// 任务中的文件还存在, 任务已经失败
			m.reduceBucket[tasks[0].TaskId] = true
			for _, task := range tasks {
				m.reduceTask[task.FileName] = task.TaskId // 待分发
			}
		}
		// 任务完成则什么都不做
		m.reduceTaskLock.Unlock()
	}
}

// start a thread that listens for RPCs from worker.go
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

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	// reduce 任务已完成, 可以结束进程
	ret = m.isReduceDone
	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.isMapDone = false
	m.isReduceDone = false
	m.nReduce = nReduce
	m.reduceTask = make(map[string]int)
	m.reduceBucket = make(map[int]bool)
	m.timeOutSec = 10                // 默认是 10s
	m.mapTask = make(map[string]int) // 初始化 maptask
	for idx, file := range files {
		m.mapTask[file] = idx + 1 // 初始状态是任务id, 正数待分发
	}
	m.server()
	return &m
}
