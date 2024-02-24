- [MapReduce](#mapreduce)
- [Lab1: MapReduce](#lab1-mapreduce)
  - [Worker](#worker)
  - [Master](#master)
  - [lock-base](#lock-base)
  - [注意点](#注意点)

## MapReduce  
* 大规模数据集（大于1TB）的并行运算

## Lab1: MapReduce
lab1 的主要目的是实现并行计算框架 MapReduce, master 进程负责统筹 `map/reduce` 任务的执行。  
### Worker
worker 的逻辑比较简单, 循环向 master 获取任务, 任务类型有如下三种：  
* `休眠任务`(**NO_TASK**): 由于有些 map 任务已经被分发完，但还没有到 reduce 任务, 需要休眠一定的时间再请求任务(失败的 map 任务或 reduce任务 或 失败的 reduce 任务)  
* `map任务`(**MAP_TASK**): map 任务即调用 `mapf` function 对文件中的内容做 key-value 的映射, 根据 key 做 hash(key) 映射到不同的目标 region, 把结果保存到不同的中间文件, 如 `mr-X-Y`, X 是文件id, Y 是 hash(key) 后的 id, 该步骤后, 把一个文件中的 key-value 通过 hash(key) 分到了不同的 region.  
* `reduce任务`(**REDUCE_TASK**): reduce 任务即调用 `reducef` function 对相同的 Y 结尾的文件执行 reduce, 并写到结果文件, 这样就有了 Y 个结果文件, 最后统计这 Y 个文件的内容即可获得结果。  
* **由于map任务有可能会 crash, 所以在写到中间文件时, 以临时文件的形式, 在写完后再通过 Rename 原子操作重命名**   

任务结束后需要向master请求任务完成rpc, 让 master 整理下一步。  
```Go
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
```
### Master 
master 负责分发任务, 并且注意当任务在规定时间内不能完成时, 需要重新恢复任务.
### lock-base
实现细节基于锁实现, 没有使用通道.
### 注意点
* 坑1: RPC 消息结构体有两个 string 对象时，只有一个 string 能正常传输
* 坑2: linux sort 命令排序规则基于 locale, `export LC_ALL=C` 可以解决
* 坑3: 局部作用域定义覆盖问题
```Golang
ok := false
for _, task := range tasks {
  // 这样会在内作用域重定义一个 ok, 使得外部 ok 永远为 false
	// _, ok := m.mapTask[task.FileName] 
	_, ok = m.mapTask[task.FileName]
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
```