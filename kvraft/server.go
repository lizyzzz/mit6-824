package kvraft

import (
	"bytes"
	"labgob"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"labrpc"
	"raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	OP_TYPE_GET    = "Get"
	OP_TYPE_PUT    = "Put"
	OP_TYPE_APPEND = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Key   string
	Value string
	Type  string // 操作类型 "Put" "Append" "Get"

	CmdIndex int // 写入日志的 index
	Term     int // 写入日志的 term

	ClientId int64
	SeqId    int64
}

// 操作的上下文
type OpContext struct {
	op *Op

	commitedChan chan int // 阻塞等待通知结果的 channel

	wrongLeader bool // leader是否被更换
	ignored     bool // seq id 已经过期, 该日志被忽略

	// Get 操作的结果
	keyExist bool
	value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	kvStore map[string]string  // 状态存储
	cmdMap  map[int]*OpContext // commandIndex -> 请求上下文
	seqMap  map[int64]int64    // clientId -> clientSeq(记录上次执行的操作序号, 保证幂等)

	lastAppliedIndex int // 最后一个应用到状态机的 index
}

// Get rpc handler
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	reply.Err = OK

	op := &Op{
		Key:      args.Key,
		Type:     OP_TYPE_GET,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}

	// 交给 raft 做一致性检查
	var isLeader bool
	op.CmdIndex, op.Term, isLeader = kv.rf.Start(op)

	if !isLeader {
		// 非leader
		reply.Err = ErrWrongLeader
		return
	}

	opCtx := &OpContext{
		op:           op,
		commitedChan: make(chan int),
	}

	// 保存上下文
	kv.mu.Lock()
	// 保存 rpc 上下文, 等待commit, leader 变更可能会导致上下文被覆盖, 不过被覆盖的 rpc 会因为超时退出
	kv.cmdMap[op.CmdIndex] = opCtx
	kv.mu.Unlock()

	// 调用结束时清理内存
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if ctx, ok := kv.cmdMap[op.CmdIndex]; ok {
			if ctx == opCtx {
				delete(kv.cmdMap, op.CmdIndex)
			}
		}
	}()

	// 等待接收commit通知
	ticker := time.NewTicker(2000 * time.Millisecond)
	defer ticker.Stop()

	select {
	case <-opCtx.commitedChan:
		// 操作被提交
		if opCtx.wrongLeader {
			// 相同 index 的位置, term 发生改变, 说明当前 server 已经不是 leader
			reply.Err = ErrWrongLeader
		} else if !opCtx.keyExist {
			// key 不存在
			reply.Err = ErrNoKey
		} else {
			// 读请求达成共识
			reply.Value = opCtx.value
		}
	case <-ticker.C:
		// 超时让 client 重试
		reply.Err = ErrWrongLeader
	}

}

// PutAppend rpc handler
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	reply.Err = OK

	op := &Op{
		Key:      args.Key,
		Value:    args.Value,
		Type:     args.Op,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}

	// 交给 raft 做一致性检查
	var isLeader bool
	op.CmdIndex, op.Term, isLeader = kv.rf.Start(op)

	if !isLeader {
		// 非leader
		reply.Err = ErrWrongLeader
		return
	}

	opCtx := &OpContext{
		op:           op,
		commitedChan: make(chan int),
	}

	// 保存上下文
	kv.mu.Lock()
	// 保存 rpc 上下文, 等待commit, leader 变更可能会导致上下文被覆盖, 不过被覆盖的 rpc 会因为超时退出
	kv.cmdMap[op.CmdIndex] = opCtx
	kv.mu.Unlock()

	// 调用结束时清理内存
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if ctx, ok := kv.cmdMap[op.CmdIndex]; ok {
			if ctx == opCtx {
				delete(kv.cmdMap, op.CmdIndex)
			}
		}
	}()

	// 等待接收commit通知
	ticker := time.NewTicker(2000 * time.Millisecond)
	defer ticker.Stop()

	select {
	case <-opCtx.commitedChan:
		// 操作被提交
		if opCtx.wrongLeader {
			// 相同 index 的位置, term 发生改变, 说明当前 server 已经不是 leader
			reply.Err = ErrWrongLeader
		}
		// else if opCtx.ignored {
		// 	// 说明 seqId 落后了, 请求被忽略, 直接回复 OK 即可
		// 	break
		// }
	case <-ticker.C:
		// 超时让 client 重试
		reply.Err = ErrWrongLeader
	}

}

// 循环接收 applyCh 的消息 goroutine
func (kv *KVServer) applyMsgLoop() {
	for !kv.killed() {
		msg := <-kv.applyCh
		cmd := msg.Command
		cmdIndex := msg.CommandIndex

		kv.mu.Lock()

		// 类型转换为操作日志
		op := cmd.(*Op)

		opCtx, existOp := kv.cmdMap[cmdIndex]
		prevSeq, existSeq := kv.seqMap[op.ClientId]
		kv.seqMap[op.ClientId] = op.SeqId // 接收到 msg, 说明已被提交, 更新 client 最新被提交的操作

		if existOp {
			// 存在在等待结果的 rpc, 判断状态是否与写入的时候一致
			// 如果不一致, 说明 leader 被更换了, 该 server 不再是 leader 了
			if op.Term != opCtx.op.Term {
				opCtx.wrongLeader = true
			}
		}

		// 如果是写请求, 应用状态机
		if op.Type == OP_TYPE_PUT || op.Type == OP_TYPE_APPEND {
			if !existSeq || op.SeqId > prevSeq {
				// 序列号比之前的更新, 接收这个变更
				if op.Type == OP_TYPE_PUT {
					kv.kvStore[op.Key] = op.Value
				} else if op.Type == OP_TYPE_APPEND {
					if val, ok := kv.kvStore[op.Key]; ok {
						// 已存在的 append
						kv.kvStore[op.Key] = val + op.Value
					} else {
						// 不存在的 put
						kv.kvStore[op.Key] = op.Value
					}
				}
			} else if existOp {
				// op.SeqId < prevSeq
				// 序列号落后, 该操作被忽略
				opCtx.ignored = true
			}
		} else {
			// 读请求
			if existOp {
				// 如果是 wrong
				opCtx.value, opCtx.keyExist = kv.kvStore[op.Key]
			}
		}

		DPrintf("raft node[%d] applyMsgLoop kvStore[%v]", kv.me, kv.kvStore)

		// 唤醒阻塞的 rpc
		if existOp {
			// 这里发送可能会没有线程接收(因为超时退出了)
			// opCtx.commitedChan <- 1
			// 使用 close
			close(opCtx.commitedChan)
		}

		kv.mu.Unlock()
	}
}

// 循环检查是否需要 snapshot 的 goroutine
func (kv *KVServer) snapshotLoop() {
	for !kv.killed() {
		var snapshot []byte
		var lastIncludedIndex int

		// 如果日志长度超过了 maxraftstate, 则进行快照
		if kv.maxraftstate != -1 && kv.rf.ExceedLogSize(kv.maxraftstate) {
			// 进行快照时需要上锁
			kv.mu.Lock()
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.kvStore)
			e.Encode(kv.seqMap) // 当前各客户端最大请求序号, 也要进行快照, 防止 leader 宕机后马上重启安装快照丢失了各客户端的请求序号(保证幂等性)
			snapshot = w.Bytes()
			lastIncludedIndex = kv.lastAppliedIndex
			DPrintf("kvserver[%d] dump snapshot, snapshotSize[%d] lastAppliedIndex[%d]", kv.me, len(snapshot), kv.lastAppliedIndex)
			kv.mu.Unlock()
		}

		// 在释放锁后通知 raft 层截断, 否则有死锁
		if snapshot != nil {
			// 通知 raft 截断已经应用到状态机的日志(这些日志都已经提交, 可以放心操作)
			kv.rf.TakeSnapshot(snapshot, lastIncludedIndex)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(&Op{})

	kv := &KVServer{
		me:               me,
		applyCh:          make(chan raft.ApplyMsg),
		maxraftstate:     maxraftstate,
		kvStore:          make(map[string]string),
		cmdMap:           make(map[int]*OpContext),
		seqMap:           make(map[int64]int64),
		lastAppliedIndex: 0,
	}

	// You may need initialization code here.

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyMsgLoop()
	go kv.snapshotLoop()

	return kv
}
