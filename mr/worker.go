package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		// 循环工作
		err := DoWork(mapf, reducef)
		if err != nil {
			// 任务完成或连接断开时结束程序
			// 注意写文件或读文件错误时这里的 err == nil, 不会结束程序, 只会向 master 报告任务失败
			// fmt.Println(err)
			return
		}
	}
}

// 通用 rpc 调用接口
func callFuncWithName(rpcname string, args interface{}, reply interface{}) error {
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return err
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		return err
	}

	return nil
}

func DoWork(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) error {
	// 获取任务
	task := &TaskResponse{}
	err := callFuncWithName("Master.GetTask", &TaskRequest{}, task)
	if err != nil {
		return err
	}
	// fmt.Printf("recv task:%v\n", task.Tasks[0])
	// 执行任务
	finishStatus := &FinishedRequest{
		FinishedType: SLEEP_TASK,
		Tasks:        make([]*FinishTask, 0),
	}
	switch task.TaskType {
	case NO_TASK:
		// 暂时没有任务休眠
		time.Sleep(task.TimeSleep)
		// 任务完成
		finishStatus.FinishedType = SLEEP_TASK
	case MAP_TASK:
		// 执行 map 函数
		finishTasks, err := doMap(mapf, task.Tasks, task.NReduce)
		if err != nil {
			// 任务失败
			finishStatus.FinishedType = MAP_TASK_FAIL
		} else {
			// 任务完成
			finishStatus.FinishedType = MAP_TASK
		}
		finishStatus.Tasks = finishTasks
	case REDUCE_TASK:
		// 执行 reduce 函数
		finishTasks, err := doReduce(reducef, task.Tasks)
		if err != nil {
			// 任务失败
			finishStatus.FinishedType = REDUCE_TASK_FAIL
		} else {
			// 任务完成
			finishStatus.FinishedType = REDUCE_TASK
		}
		finishStatus.Tasks = finishTasks
	}

	// 反馈任务
	finishResponse := &FinishedResponse{}
	err = callFuncWithName("Master.TaskFinished", finishStatus, finishResponse)
	if err != nil {
		return err
	}
	if finishResponse.CanExit {
		// 任务已全部完成, 可以结束程序
		return errors.New("All task done!")
	}

	return nil
}

// 执行 map 函数
func doMap(mapf func(string, string) []KeyValue, tasks []*AssignTask, nReduce int) ([]*FinishTask, error) {
	finishTasks := []*FinishTask{}
	// 对每一个任务执行 mapf 结果存放在 keyVals 中
	for _, task := range tasks {
		// 任务完成状态
		finishTask := &FinishTask{
			FileName:   task.FileName,
			TaskId:     task.TaskId,
			BucketNums: make([]int, 0),
		}

		// 根据文件名读取文件
		file, err := os.Open(task.FileName)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		content, err := io.ReadAll(file)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		file.Close()

		// 执行 mapf
		keyVals := mapf(task.FileName, string(content))

		// 先分类根据 key 定位到 region , 可以减少打开文件的次数
		// 因为后续会根据 key 生成形如 mr-X-Y 的文件
		// regionMap 的 key 是 Y
		regionMap := make(map[int][]KeyValue, 0)
		for _, kv := range keyVals {
			Y := ihash(kv.Key)%nReduce + 1
			regionMap[Y] = append(regionMap[Y], kv)
		}

		// 写到不同的 mr-X-Y 文件
		for Y, kvs := range regionMap {
			// 中间文件命名规则: mr-X-Y (Y从 1 开始)
			// 以临时文件的形式写入, 再重命名, 防止崩溃时文件对其他工作节点可见
			oname := NumberToStr(task.TaskId, Y)

			// 序列化为 json
			data, err := json.Marshal(kvs)
			if err != nil {
				fmt.Println("json marshal fail")
				return nil, err
			}
			// 创建文件, 并写入文件
			// ofile, err := os.Create(oname)
			tmpFile, err := os.CreateTemp(".", oname)
			if err != nil {
				fmt.Println("map create file fail")
				return nil, err
			}
			_, err = tmpFile.Write(data)
			if err != nil {
				fmt.Println("map write file fail")
				return nil, err
			}
			// 重命名为非临时文件, 原子的
			os.Rename(tmpFile.Name(), oname)
			tmpFile.Close()
			// 添加 BucketNum
			finishTask.BucketNums = append(finishTask.BucketNums, Y)
		}
		finishTasks = append(finishTasks, finishTask)
	}

	return finishTasks, nil
}

// 执行 reduce 函数
func doReduce(reducef func(string, []string) string, tasks []*AssignTask) ([]*FinishTask, error) {
	finishTasks := []*FinishTask{}
	// 读取全部文件
	keyVals := []KeyValue{}
	for _, task := range tasks {
		// 读取文件
		file, err := os.Open(task.FileName)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		content, err := io.ReadAll(file)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		file.Close()

		// json 反序列化
		keyVal := []KeyValue{}
		err = json.Unmarshal(content, &keyVal)
		if err != nil {
			fmt.Println("json unmarshal fail")
			return nil, err
		}

		// 添加到 keyVals 中
		keyVals = append(keyVals, keyVal...)

		finishTask := &FinishTask{
			FileName:   task.FileName,
			TaskId:     task.TaskId,
			BucketNums: []int{task.TaskId},
		}
		finishTasks = append(finishTasks, finishTask)
	}

	// 排序后调用 reducef
	sort.Sort(ByKey(keyVals))

	// reduce 输出文件名 mr-out-Y (Y从 0 开始)
	// 以临时文件的形式写入, 再重命名, 防止崩溃时文件对其他工作节点可见
	oname := "mr-out-" + strconv.Itoa(tasks[0].TaskId-1)
	// 创建文件
	// ofile, err := os.Create(oname)
	tmpFile, err := os.CreateTemp(".", oname)
	if err != nil {
		fmt.Println("reduce create file fail")
		return nil, err
	}
	// 处理结果
	i := 0
	for i < len(keyVals) {
		j := i + 1
		for j < len(keyVals) && keyVals[j].Key == keyVals[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, keyVals[k].Value)
		}
		output := reducef(keyVals[i].Key, values)
		// 按行写到文件
		fmt.Fprintf(tmpFile, "%v %v\n", keyVals[i].Key, output)

		i = j
	}
	// 重命名为非临时文件
	os.Rename(tmpFile.Name(), oname)

	tmpFile.Close()

	return finishTasks, nil
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
