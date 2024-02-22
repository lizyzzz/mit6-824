package kvraft

import (
	"context"
	"crypto/rand"
	"labrpc"
	"math/big"
	"sync"
	"sync/atomic"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	mu sync.Mutex

	leaderIndex int // 上一次的 leader 索引

	clientId int64 // 唯一的 client id
	seqId    int64 // 发送的操作序列号
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	// ck := new(Clerk)
	// ck.servers = servers
	// You'll have to add code here.

	timeStamp := time.Now().UnixMilli()

	ck := &Clerk{
		servers:     servers,
		leaderIndex: 0,
		clientId:    nrand() + timeStamp, // 唯一性
		seqId:       0,
	}

	return ck
}

func (ck *Clerk) currentLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.leaderIndex
}

func (ck *Clerk) changeLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.leaderIndex++
	ck.leaderIndex = ck.leaderIndex % len(ck.servers)
	return ck.leaderIndex
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1), // 原子递增序列号
	}

	DPrintf("client[%d] start get key[%s], seq[%d]", ck.clientId, key, args.SeqId)

	for {
		reply := GetReply{}
		// 先尝试上一次的 leader
		timeOut, ok := ck.sendGetWithTimeOut(ck.currentLeader(), &args, &reply, 3000*time.Millisecond)

		if timeOut {
			// 超时
			continue
		} else {
			if ok {
				// 收到响应
				switch reply.Err {
				case OK:
					return reply.Value
				case ErrNoKey:
					return ""
				case ErrWrongLeader:
					// 切换 leader
					DPrintf("client[%d] Get key err:%s", ck.clientId, reply.Err)
					ck.changeLeader()
				}
			}
		}
	}
}

// 对指定的 server 调用 Get rpc
func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	return ok
}

// 发送带超时的 Get 操作, 返回的第一个参数是否超时, 第二个参数表示是否收到响应
func (ck *Clerk) sendGetWithTimeOut(server int, args *GetArgs, reply *GetReply, ms time.Duration) (bool, bool) {
	ctx, cancle := context.WithTimeout(context.Background(), ms)
	defer cancle()

	taskDone := make(chan bool, 1)
	go func() {
		ok := ck.sendGet(server, args, reply)
		taskDone <- ok
	}()

	select {
	case ok := <-taskDone:
		// 任务完成
		return false, ok
	case <-ctx.Done():
		// 任务超时
		return true, false
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1), // 原子递增序列号
	}

	DPrintf("client[%d] start PutAppend key[%s]-value[%s], seq[%d]", ck.clientId, key, value, args.SeqId)

	for {
		reply := PutAppendReply{}
		// 先尝试上一次的 leader
		timeOut, ok := ck.sendPutAppendWithTimeOut(ck.currentLeader(), &args, &reply, 3000*time.Millisecond)

		if timeOut {
			// 超时
			continue
		} else {
			if ok {
				// 收到响应
				switch reply.Err {
				case OK:
					return
				case ErrWrongLeader:
					// 切换 leader
					DPrintf("client[%d] PutAppend key err:%s", ck.clientId, reply.Err)
					ck.changeLeader()
				}
			}
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// 对指定的 server 调用 Get rpc
func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok
}

// 发送带超时的 Get 操作, 返回的第一个参数是否超时, 第二个参数表示是否收到响应
func (ck *Clerk) sendPutAppendWithTimeOut(server int, args *PutAppendArgs, reply *PutAppendReply, ms time.Duration) (bool, bool) {
	ctx, cancle := context.WithTimeout(context.Background(), ms)
	defer cancle()

	taskDone := make(chan bool, 1)
	go func() {
		ok := ck.sendPutAppend(server, args, reply)
		taskDone <- ok
	}()

	select {
	case ok := <-taskDone:
		// 任务完成
		return false, ok
	case <-ctx.Done():
		// 任务超时
		return true, false
	}
}
