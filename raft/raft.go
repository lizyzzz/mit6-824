package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"context"
	"fmt"
	"labgob"
	"labrpc"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// 日志条目
type LogEntry struct {
	LogTerm       int         // 日志发生的任期
	LogIndex      int         // 日志的 index(从 1 开始)
	Command       interface{} // command 命令
	CommandIndex  int         // 外部命令的 index
	IsInternalLog bool        // 是否是内部log(如果是内部log, 提交后不会返回)
}

// leader 结构体
type Leader struct {
	nextIndexs  []int // leader 维护每个服务器的下一个日志 index
	matchIndexs []int // leader 维护每个服务器的已复制的最高 index
}

// 服务器角色类型
const (
	FOLLOWER  = 1
	CANDICATE = 2
	LEADER    = 3
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 持久化状态
	currentTerm int         // 当前任期
	votedFor    int         // 投票的目标
	logs        []*LogEntry // 日志条目

	// 易失状态
	commitIndex int           // 已知的已提交的日志条目的 最大索引
	lastApplied int           // 应用到状态机的日志条目的 最大索引
	applyChan   chan ApplyMsg // 通知应用层把已提交的命令应用到状态机
	// replicationCount map[int]int // index -> count: 索引为 index 的日志被接收的副本个数(用来判断是否可以提交了)

	role int // 服务器角色

	// 选举超时的相关变量
	timeOutMS    int32     // 选举超时时间 ms
	heartBeatCnt int32     // 超时时间内的心跳计数(原子变量)
	quitElection chan bool // 选举超时线程退出 channel

	heartBeatCh chan bool // 选举发生的信号通道(heartBeat 正常发送 true)

	leaderPtr *Leader // leader 指针, 如果该服务器认为自己是 leader , 则该指针非空
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.role == LEADER)
	rf.mu.Unlock()

	return term, isleader
}

// 改变状态成为 follower
// 需要持有锁
func (rf *Raft) becomeFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.leaderPtr = nil
	if rf.role != FOLLOWER {
		atomic.StoreInt32(&rf.heartBeatCnt, 0)
		rf.role = FOLLOWER
		go rf.TimeOutToElection()
	}
}

// 改变状态成为 leader
// 需要持有锁
func (rf *Raft) becomeLeader() {
	rf.role = LEADER
	rf.leaderPtr = &Leader{
		nextIndexs:  make([]int, len(rf.peers)),
		matchIndexs: make([]int, len(rf.peers)),
	}

	// TODO: 初始化 nextIndexs matchIndexs
	for i := 0; i < len(rf.peers); i++ {
		rf.leaderPtr.nextIndexs[i] = len(rf.logs)
		rf.leaderPtr.matchIndexs[i] = 0
	}
	// 第一次 heartbeat , 发送一个 no-op 的 log entry 以确定 leader 新上任时有哪些 entry 已经被提交

	// 追加一个 no-op 到 logs 中
	index := len(rf.logs)
	noOpLog := &LogEntry{
		LogTerm:       rf.currentTerm,
		LogIndex:      index,
		Command:       nil,                                  // 空 command
		CommandIndex:  rf.logs[len(rf.logs)-1].CommandIndex, // CommandIndex 不变
		IsInternalLog: true,                                 // 内部 log
	}
	rf.logs = append(rf.logs, noOpLog)
	// rf.replicationCount[index] = 1 // 计数重置为 1

	// 开启 heartbeat
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go rf.HeartBeatToServer(i)
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	// 持久化一些变量
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		fmt.Println(err)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		fmt.Println(err)
	}
	if err := e.Encode(rf.logs); err != nil {
		fmt.Println(err)
	}

	data := w.Bytes()

	rf.persister.SaveRaftState(data)
	// fmt.Println("encode success")
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	// 读取一些变量
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []*LogEntry
	if err := d.Decode(&currentTerm); err != nil {
		fmt.Printf("decode failed from readPersist(): currentTerm. err: %v\n", err)
	} else if err := d.Decode(&votedFor); err != nil {
		fmt.Printf("decode failed from readPersist(): votedFor. err: %v\n", err)
	} else if err := d.Decode(&logs); err != nil {
		fmt.Printf("decode failed from readPersist(): logs. err: %v\n", err)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		// fmt.Printf("decode successed from readPersist()\n")
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandicateTerm int // 候选人的当前任期
	CandicateId   int // 候选人的 id
	LastLogIndex  int // 候选人最后一条日志条目的 index(用于比较日志的新旧)
	LastLogTerm   int // 候选人最后一条日志条目的 term(用于比较日志的新旧)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	FollowerTerm int  // follower 当前的任期, 供 candicate 按需更新自己的 term
	VoteGranted  bool // true 表示接收到投票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 给 candicate 投票

	rf.mu.Lock()
	// 如果候选者任期更大, 应该更新任期和 votedFor
	if args.CandicateTerm > rf.currentTerm {
		rf.becomeFollower(args.CandicateTerm)
		// fmt.Printf("%d find high candicate, become follower\n", rf.me)
		// 保存持久化变量
		rf.persist()
	}
	curRole := rf.role
	curTerm := rf.currentTerm
	lastLogIdx := rf.logs[len(rf.logs)-1].LogIndex
	lastLogTerm := rf.logs[len(rf.logs)-1].LogTerm
	// if len(rf.logs) > 0 {
	// 	lastLogIdx = rf.logs[len(rf.logs)-1].LogIndex
	// 	lastLogTerm = rf.logs[len(rf.logs)-1].LogTerm
	// }
	rf.mu.Unlock()

	reply.VoteGranted = false
	reply.FollowerTerm = curTerm
	if args.CandicateTerm < curTerm {
		// 候选者任期落后, 拒绝投票
		// fmt.Printf("%d reject vote to %d because of small term\n", rf.me, args.CandicateId)
		return
	}

	// 选举限制(防止缺少log entry 的后选择被选择为 leader: 如果日志落后则拒绝投票)
	if (args.LastLogTerm < lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIdx) {
		// 日志落后, 拒绝投票
		// fmt.Printf("%d reject vote to %d because of small log\n", rf.me, args.CandicateId)
		return
	}

	switch curRole {
	case FOLLOWER:
		rf.mu.Lock()
		if rf.votedFor == -1 || rf.votedFor == args.CandicateId {
			// 还没投票 或 候选者没有收到需要重复投票
			rf.votedFor = args.CandicateId
			reply.VoteGranted = true
			rf.heartBeatCh <- true // 作出选举, 重置超时
			// 保存持久化变量
			rf.persist()
			// fmt.Printf("%d(role: %d) vote to %d\n", rf.me, rf.role, args.CandicateId)
		}
		rf.mu.Unlock()
	case CANDICATE:
		// 不做操作
		// fmt.Printf("%d reject vote to %d because of as candicate\n", rf.me, args.CandicateId)
	case LEADER:
		// fmt.Printf("%d reject vote to %d because of as leader\n", rf.me, args.CandicateId)
	}

	// return

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

// 在一定时间内成功收到响应 返回true, 否则返回 false
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 在指定时间内执行, 超时则返回 false, 第一个参数表示是否超时
func (rf *Raft) sendRequestVoteWithTimeOut(server int, args *RequestVoteArgs, reply *RequestVoteReply, ms time.Duration) (bool, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), ms)
	defer cancel()

	taskDone := make(chan bool, 1)
	go func() {
		ok := rf.sendRequestVote(server, args, reply)
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

// AppendEntries RPC

// AppendEntries 请求参数
type AppendEntriesArgs struct {
	// TODO: 可以增加 msgType 表示该消息的类型(heartbeat, add log entries)
	LeaderTerm   int         // leader 的当前任期
	LeaderId     int         // leader 的 id
	PrevLogIndex int         // 新日志条目的前一个日志条目的 index
	PrevLogTerm  int         // 新日志条目的前一个日志条目的 term
	Entries      []*LogEntry // 要保存的日志条目, 如果是 heartbeat , 长度为 0
	LeaderCommit int         // leader 的 commitIndex
}

// AppendEntries 响应参数
type AppendEntriesReply struct {
	FollowerTerm int  // follower 的当前 term , leader 按需要更新
	Success      bool // true 如果 follower 包含相匹配的 PrevLogIndex 和 PrevLogTerm

	// 增加三个字段用于崩溃快速恢复

	Xterm  int // 发生冲突的 term (prevLogIndex 存在时非0)
	Xindex int // Xterm 下的第一个 entry 的索引 (prevLogIndex 存在时非-1)
	Xlen   int // 日志长度 (prevLogIndex 存在时非0)
}

// 心跳, 日志复制
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	role := rf.role
	// 重置一些状态
	if args.LeaderTerm >= rf.currentTerm {
		// leader 任期更高, 更新状态
		rf.becomeFollower(args.LeaderTerm)
		// if args.LeaderTerm > rf.currentTerm {
		// 	fmt.Printf("%d find high term leader become follower\n", rf.me)
		// }
		// 保存持久化变量
		if args.LeaderTerm > rf.currentTerm {
			rf.persist()
		}
		reply.FollowerTerm = rf.currentTerm
		reply.Success = true
	} else {
		// leader 的 term 更小, 直接返回
		reply.FollowerTerm = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	// 新 leader 的第一条 heartbeat 或正常的 heartbeat
	switch role {
	case FOLLOWER, CANDICATE:
		// 重置心跳超时定时器 或 停止正在进行的选举
		rf.heartBeatCh <- true
	case LEADER:
		// 不能发送 heartbeatCh 因为 leader 阶段没有接收 heartbeatCh
	}

	// fmt.Printf("follower %d recv heartbeat from leader %d, entries: %v, lenOfEntries: %d, leadercommitIndex: %d\n", rf.me, args.LeaderId, args.Entries, len(args.Entries), args.LeaderCommit)

	// 检查日志是否冲突
	if args.Entries != nil {
		// fmt.Printf("not nil\n")
		// follower 的 logs 只在该线程下改变, 不用持有锁
		if len(rf.logs)-1 >= args.PrevLogIndex {
			// 该 preLogindex 存在, 比较 term
			if rf.logs[args.PrevLogIndex].LogTerm == args.PrevLogTerm {
				// term 也相同, 日志不冲突
				// 更新 logs
				rf.logs = rf.logs[:args.PrevLogIndex+1]
				rf.logs = append(rf.logs, args.Entries...)
				reply.Success = true
				// 保存持久化变量
				rf.persist()
				// fmt.Printf("follower %d update log, lenOfLog: %d\n", rf.me, len(rf.logs))
			} else {
				// term 不相同
				reply.Success = false
				reply.Xterm = rf.logs[args.PrevLogIndex].LogTerm // 冲突日志的 Logterm
				for i := args.PrevLogIndex - 1; i >= 0; i-- {
					if rf.logs[i].LogTerm != rf.logs[i+1].LogTerm {
						reply.Xindex = i + 1 // xterm 的第一个 entry 索引
						break
					}
				}
				// 删除该entry后的所有日志(保留args.PrevLogIndex以前的日志)
				rf.logs = rf.logs[:args.PrevLogIndex]
				// 保存持久化变量
				rf.persist()
				return // 不进行更新 commitIndex, 因为日志还没有同步, 会把错误的操作更新到状态机
			}
		} else {
			// 该 prevLogindex 不存在, 不做处理, 返回 false
			reply.Success = false
			reply.Xterm = -1
			reply.Xlen = len(rf.logs) // 日志长度
			return                    // 不进行更新 commitIndex, 因为日志还没有同步, 会把错误的操作更新到状态机
		}
	}

	// 更新 commitIndex, 并把提交应用到状态机 (日志已同步或者正常心跳(说明日志已同步)都可以进行更新)
	if args.LeaderCommit > rf.commitIndex {
		// fmt.Printf("follower %d args.LeaderCommit:%d > rf.commitIndex:%d, lenOfLog:%d\n", rf.me, args.LeaderCommit, rf.commitIndex, len(rf.logs))
		i := rf.commitIndex + 1
		for ; i <= args.LeaderCommit && i < len(rf.logs); i++ {
			// fmt.Printf("follower %d log[%d].IsInternalLog: %v\n", rf.me, i, rf.logs[i].IsInternalLog)
			if !rf.logs[i].IsInternalLog {
				// 非内部 log
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i].Command,
					CommandIndex: rf.logs[i].CommandIndex,
				}
				rf.applyChan <- applyMsg
				// fmt.Printf("follower %d apply command: %v, commandIndex: %d\n", rf.me, applyMsg.Command, applyMsg.CommandIndex)
			}
			rf.commitIndex = i
			rf.lastApplied = i
		}
	}

	// return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 在指定时间内执行, 超时则返回 false, 第一个参数表示是否超时
func (rf *Raft) sendAppendEntriesWithTimeOut(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, ms time.Duration) (bool, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), ms)
	defer cancel()

	taskDone := make(chan bool, 1)
	go func() {
		ok := rf.sendAppendEntries(server, args, reply)
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

// 选举操作函数
// 返回值是: true 赢得了选举
func (rf *Raft) StartElection() {
	rf.quitElection <- true // 退出 follower 的超时选举线程

	for {

		// 更新任期和角色
		rf.mu.Lock()
		// fmt.Printf("%d(role: %d) start election\n", rf.me, rf.role)
		rf.currentTerm++
		rf.role = CANDICATE
		rf.votedFor = rf.me // 投票给自己
		term := rf.currentTerm
		lastLogTerm := rf.logs[len(rf.logs)-1].LogTerm
		lastLogIdx := rf.logs[len(rf.logs)-1].LogIndex
		// if len(rf.logs) > 0 {
		// 	lastLogTerm = rf.logs[len(rf.logs)-1].LogTerm
		// 	lastLogIdx = rf.logs[len(rf.logs)-1].LogIndex
		// }
		rf.mu.Unlock()

		// 选举阶段超时 定时器
		ticker := time.NewTicker(300 * time.Millisecond)
		// 接受投票结果的 channel
		voteCh := make(chan *RequestVoteReply, len(rf.peers))

		// 发送投票请求(并行发送)
		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			// 开启线程请求投票
			go func(voteCh chan *RequestVoteReply, server int) {
				args := &RequestVoteArgs{
					CandicateTerm: term,
					CandicateId:   rf.me,
					LastLogIndex:  lastLogIdx,
					LastLogTerm:   lastLogTerm,
				}
				reply := &RequestVoteReply{
					VoteGranted: false,
				}
				ok := false
				for !ok {
					rf.mu.Lock()
					role := rf.role
					curTerm := rf.currentTerm
					rf.mu.Unlock()
					if role != CANDICATE || curTerm != term {
						// 退出请求投票
						// fmt.Printf("%d quit send requestVote to %d\n", rf.me, server)
						return
					}

					var timeOut bool
					// 反复请求
					// fmt.Printf("%d send requestVote to %d\n", rf.me, server)
					timeOut, ok = rf.sendRequestVoteWithTimeOut(server, args, reply, 100*time.Millisecond)

					if !timeOut {
						// fmt.Printf("candicate %d recv requestVote from %d, ok: %v\n", rf.me, server, ok)
						if ok {
							break
						}
					}
					// else {
					// 	// fmt.Printf("candicate %d requestVote to %d timeout\n", rf.me, server)
					// }
				}

				voteCh <- reply
			}(voteCh, i)
		}

		// 选举获得投票计数
		getVote := 1

	Loop:
		for {
			select {
			case <-ticker.C:
				// 超时, 后续一段时间后继续开始选举
				// fmt.Printf("%d loss election because of timeout\n", rf.me)
				break Loop
			case reply := <-voteCh:
				// 投票结果回应
				if reply.VoteGranted {
					// 获得投票
					getVote++
					if getVote >= (len(rf.peers)+1)/2 {
						// 赢得投票, 转化为 leader, 并且开启 heartbeat 线程
						rf.mu.Lock()
						rf.becomeLeader()
						rf.mu.Unlock()
						rf.persist()
						// fmt.Printf("%d win election\n", rf.me)
						return
					}
				} else {
					if reply.FollowerTerm > rf.currentTerm {
						// 发现任期更大的服务器, 切换为 follower
						rf.mu.Lock()
						rf.becomeFollower(reply.FollowerTerm)
						rf.mu.Unlock()
						rf.persist()
						// fmt.Printf("%d loss election because of other has high term, become follower\n", rf.me)
						return // 停止选举
					}
					// 其他情况不用处理, 继续等待接收
				}

			case <-rf.heartBeatCh:
				// fmt.Printf("%d loss election because of other become leader\n", rf.me)
				return
			}

		}

		time.Sleep(100 * time.Millisecond) // 100 ms 后开始新一轮选举
		if rf.killed() {
			// fmt.Printf("%d election quit because of killed\n", rf.me)
			return
		}
	}

}

// 超时提出选举的后台线程函数
// leader 应该没有这个后台线程?(切换为 leader/candicate 时退出该线程)
func (rf *Raft) TimeOutToElection() {
	// 定时器
	ticker := time.NewTicker(time.Duration(rf.timeOutMS) * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			if rf.killed() {
				// fmt.Printf("%d quit TimeOutToElection because of killed\n", rf.me)
				return
			}

			cnt := atomic.LoadInt32(&rf.heartBeatCnt)
			if cnt > 0 {
				// 没有超时, 重新计数
				atomic.StoreInt32(&rf.heartBeatCnt, 0)
			} else {
				// 超时, 开启选举
				go rf.StartElection()
			}

		case <-rf.heartBeatCh:
			// 收到 heartbeat, 计数 +1
			cnt := atomic.LoadInt32(&rf.heartBeatCnt)
			cnt++
			atomic.StoreInt32(&rf.heartBeatCnt, cnt)

		case <-rf.quitElection:
			// fmt.Printf("%d quit TimeOutToElection\n", rf.me)
			return // 退出线程
		}
	}
}

// HeartBeatToServer 线程
func (rf *Raft) HeartBeatToServer(peerIndex int) {
	for {
		if rf.killed() {
			// fmt.Printf("%d heartbeat to %d quit because of killed\n", rf.me, peerIndex)
			return
		}

		rf.mu.Lock()
		role := rf.role
		if role != LEADER {
			// 收到了 term 更大的心跳或 requestVote, 已切换为follower
			rf.mu.Unlock()
			return
		}
		args := &AppendEntriesArgs{
			LeaderTerm: rf.currentTerm,
			LeaderId:   rf.me,
			// 以下为 log entries 的参数
			PrevLogIndex: -1,
			PrevLogTerm:  -1,
			Entries:      nil,
			LeaderCommit: rf.commitIndex, // 如果是第一次 heartbeat, commitIndex == 0
		}
		if len(rf.logs)-1 >= rf.leaderPtr.nextIndexs[peerIndex] {
			// 有需要提交的日志
			// fmt.Printf("peer:%d, len(logs):%d, index:%d\n", peerIndex, len(rf.logs), rf.leaderPtr.nextIndexs[peerIndex])
			args.PrevLogIndex = rf.logs[rf.leaderPtr.nextIndexs[peerIndex]-1].LogIndex
			args.PrevLogTerm = rf.logs[rf.leaderPtr.nextIndexs[peerIndex]-1].LogTerm
			args.Entries = rf.logs[rf.leaderPtr.nextIndexs[peerIndex]:] // 剩下的全部日志
		}
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		// 如果有需要复制的 log entry, 则以更短的时间间隔发送 appendentry

		// fmt.Printf("leader %d send heartbeat to follower %d\n", rf.me, peerIndex)
		// ok := rf.sendAppendEntries(peerIndex, args, reply)
		timeOut, ok := rf.sendAppendEntriesWithTimeOut(peerIndex, args, reply, 50*time.Millisecond)
		if timeOut {
			// 任务超时
			// fmt.Printf("leader %d send heartbeat to follower %d timeout\n", rf.me, peerIndex)
			continue
		} else {
			// fmt.Printf("leader %d heartbeat recv ok from follower %d, ok: %v\n", rf.me, peerIndex, ok)
			if ok {
				if reply.Success {
					if args.Entries != nil {
						// follower 已复制了日志
						// 更新 nextIndex 和 matIndex (只有在当前线程改变不需要持有锁)
						rf.mu.Lock() // Lock() 防止 leaderptr 被置空
						if rf.role != LEADER {
							rf.mu.Unlock()
							return // leader 已被弃用
						}
						rf.leaderPtr.nextIndexs[peerIndex] = args.Entries[len(args.Entries)-1].LogIndex + 1
						rf.leaderPtr.matchIndexs[peerIndex] = args.Entries[len(args.Entries)-1].LogIndex
						// fmt.Printf("get success from %d, replication logIndex: %d, commitIndex: %d\n", peerIndex, args.Entries[len(args.Entries)-1].LogIndex, rf.commitIndex)
						// 尝试更新 commitIndex, 并应用命令到状态机
						if args.Entries[len(args.Entries)-1].LogIndex > rf.commitIndex {
							// 注意只能提交本任期内的log
							for i := args.Entries[len(args.Entries)-1].LogIndex; i > rf.commitIndex && rf.logs[i].LogTerm == rf.currentTerm; i-- {
								cnt := 2 // leader + 当前回复的 follower
								for j := 0; j < len(rf.peers); j++ {
									if j == peerIndex || j == rf.me {
										continue
									}
									if rf.leaderPtr.matchIndexs[j] >= i {
										cnt++
									}
								}
								if cnt >= (len(rf.peers)+1)/2 {
									// 大多数都已复制了这个 log, 更新 commitIndex
									// 往 applyCh 发送,更新 lastApplied
									for k := rf.commitIndex + 1; k <= i; k++ {
										if !rf.logs[k].IsInternalLog {
											// 非内部log需要应用到状态机
											applyMsg := ApplyMsg{
												CommandValid: true,
												Command:      rf.logs[k].Command,
												CommandIndex: rf.logs[k].CommandIndex,
											}
											rf.applyChan <- applyMsg
											// fmt.Printf("leader %d apply command: %v, commandIndex: %d\n", rf.me, applyMsg.Command, applyMsg.CommandIndex)
										}
									}
									rf.commitIndex = i
									rf.lastApplied = i
									break
								}
							}
						}
						rf.mu.Unlock()
					}
				} else {
					if args.LeaderTerm < reply.FollowerTerm {
						// leaderTerm < replyterm
						// 转换为 follower
						rf.mu.Lock()
						rf.becomeFollower(reply.FollowerTerm)
						rf.mu.Unlock()
						// 保存持久化变量
						rf.persist()
						// fmt.Printf("leader %d find high term, become follower\n", rf.me)
						break // 跳出循环不再发送 heartbeat
					} else {
						// 日志冲突导致的失败, 递减 nextIndex 后重试
						// rf.mu.Lock()
						// if rf.role != LEADER {
						// 	rf.mu.Unlock()
						// 	return // leader 已被弃用
						// }
						// rf.leaderPtr.nextIndexs[peerIndex]--
						// rf.mu.Unlock()

						// 日志冲突导致的失败, 快速定位冲突日志
						rf.mu.Lock()
						if rf.role != LEADER {
							rf.mu.Unlock()
							return // leader 已被弃用
						}
						if reply.Xterm != -1 {
							// 查找是否有 Xterm
							for i := rf.logs[len(rf.logs)-1].LogIndex; i > 0; i-- {
								if rf.logs[i].LogTerm == reply.Xterm {
									// 存在冲突 Xterm, 直接在 冲突term开始的位置 备份
									rf.leaderPtr.nextIndexs[peerIndex] = reply.Xindex
									break
								} else if rf.logs[i].LogTerm < reply.Xterm {
									// 不存在冲突 Xterm, 直接从 (冲突term开始的位置-1) 备份
									rf.leaderPtr.nextIndexs[peerIndex] = reply.Xindex - 1
									break
								}
							}
						} else {
							// prevLogIndex 不存在, 直接从日志最后一个开始备份
							rf.leaderPtr.nextIndexs[peerIndex] = rf.logs[reply.Xlen].LogIndex
						}
						rf.mu.Unlock()

						time.Sleep(10 * time.Millisecond) // 10ms 后重试
						continue
					}
				}
			}
			// 100 ms 发送一次
			time.Sleep(100 * time.Millisecond)
		}

	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.

// 如果当前不是 leader 直接返回 false
// 如果当前是leader, 返回当前 index, term, true, 并往 把 comand 加到log中, 等待开启一致性协议
// 如果 raft 被终止, 应该优雅返回
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		// 如果 raft 被终止, 应该优雅返回
		return index, term, isLeader
	}
	isLeader = (rf.role == LEADER)
	if !isLeader {
		// 不是 Leader 直接返回 false
		return index, term, isLeader
	}
	index = rf.logs[len(rf.logs)-1].CommandIndex + 1 // commandIndex + 1
	logIndex := len(rf.logs)
	term = rf.currentTerm
	newLog := &LogEntry{
		LogTerm:       term,
		LogIndex:      logIndex,
		Command:       command,
		CommandIndex:  index,
		IsInternalLog: false, // 非内部 log
	}
	// 追加到 logs 中
	rf.logs = append(rf.logs, newLog)
	// fmt.Printf("start append command: %v at index: %d, commandIndex: %d\n", command, logIndex, index)
	// rf.replicationCount[index] = 1
	// 保存持久化变量
	rf.persist()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

	// 回收实例
	// fmt.Printf("%d call Kill ... \n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// fmt.Printf("%d init ...\n", rf.me)
	rf.applyChan = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1 // -1 表示没有投票
	rf.logs = make([]*LogEntry, 0)
	// 哨兵节点
	dummyLog := &LogEntry{
		LogTerm:       0,
		LogIndex:      0,
		Command:       nil,
		CommandIndex:  0,
		IsInternalLog: true,
	}
	rf.logs = append(rf.logs, dummyLog)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.role = FOLLOWER

	// 选举超时相关变量
	rf.timeOutMS = int32(300 + rf.me*50)
	atomic.StoreInt32(&rf.heartBeatCnt, 0)
	rf.quitElection = make(chan bool)

	rf.heartBeatCh = make(chan bool, 8)

	rf.leaderPtr = nil

	// 启动超时选举的协程
	go rf.TimeOutToElection()

	// initialize from state persisted before a crash
	// fmt.Printf("%d initialize\n", rf.me)
	rf.readPersist(persister.ReadRaftState())

	return rf
}
