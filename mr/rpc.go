package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// 定义 master 和 worker 之间的 rpc 信息格式

// 分发的任务
type AssignTask struct {
	// 文件名
	FileName string
	// 任务 id
	TaskId int
}

// 完成的任务
type FinishTask struct {
	// 文件名
	FileName string
	// 任务 id
	TaskId int
	// 任务完成时的输出文件名后缀 Y 列表
	BucketNums []int
}

// 请求: 获取任务的请求
type TaskRequest struct {
	// 空结构体
}

// 响应: 任务消息体
type TaskResponse struct {
	// 任务类型
	// 0: 无任务, 请过 TimeSleep 秒再请求
	// 1: map 任务, 任务为 Tasks
	// 2: reduce 任务, 任务为 Tasks
	TaskType int

	// 任务存在时, 表示该任务列表
	Tasks []*AssignTask
	// 任务不存在时, 表示休眠的时间
	TimeSleep time.Duration
	// map 任务的桶数量
	NReduce int
}

// 请求: 任务完成时的请求
type FinishedRequest struct {
	// 任务完成的类型
	// 1: 完成 map 任务, 任务为 Tasks
	// 2: 完成 reduce 任务, 任务为 Tasks
	// -1: 完成 map 任务失败, 任务为 Tasks
	// -2: 完成 reduce 任务失败, 任务为 Tasks
	// 3： 完成 休眠 任务
	FinishedType int

	// 该次执行完成的任务列表
	Tasks []*FinishTask
}

// 响应: 任务完成时的响应
type FinishedResponse struct {
	// true 表示所有任务都完成, 程序应该退出
	// false 还需继续请求任务
	CanExit bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
