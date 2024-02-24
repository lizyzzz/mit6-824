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
	CommandValid bool // True 表示是日志条目, False 表示是 日志快照
	Command      interface{}
	CommandIndex int
	LogIndex     int

	// 快照相关变量
	Snapshot             []byte
	LastIncludedIndex    int
	LastIncludedTerm     int
	LastIncludedCmdIndex int
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
	currentTerm          int        // 当前任期
	votedFor             int        // 投票的目标
	logs                 []LogEntry // 日志条目
	lastIncludedIndex    int        // 快照最后一个日志的 index
	lastIncludedTerm     int        // 快照最后一个日志的 term
	lastIncludedCmdIndex int        // 快照最后一个日志的 cmdIndex

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
	rf.persist()
}

// 改变状态成为 leader
// 需要持有锁
func (rf *Raft) becomeLeader() {
	rf.role = LEADER
	rf.leaderPtr = &Leader{
		nextIndexs:  make([]int, len(rf.peers)),
		matchIndexs: make([]int, len(rf.peers)),
	}

	for i := 0; i < len(rf.peers); i++ {
		rf.leaderPtr.nextIndexs[i] = rf.getLastLog().LogIndex + 1
		rf.leaderPtr.matchIndexs[i] = 0
	}
	// 第一次 heartbeat , 发送一个 no-op 的 log entry 以确定 leader 新上任时有哪些 entry 已经被提交

	// 追加一个 no-op 到 logs 中
	index := rf.getLastLog().LogIndex + 1
	noOpLog := LogEntry{
		LogTerm:       rf.currentTerm,
		LogIndex:      index,
		Command:       nil,                          // 空 command
		CommandIndex:  rf.getLastLog().CommandIndex, // CommandIndex 不变
		IsInternalLog: true,                         // 内部 log
	}
	rf.logs = append(rf.logs, noOpLog)
	DPrintf("server %d append Command: %v, Commandindex: %d, LogIndex: %d, lenOfLog: %d, term: %d",
		rf.me, noOpLog.Command, index, noOpLog.LogIndex, len(rf.logs), rf.currentTerm)

	rf.persist()
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
	data := rf.encodeRaftState()

	rf.persister.SaveRaftState(data)
}

// encode raft 的 state
func (rf *Raft) encodeRaftState() []byte {
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
	// 持久化日志快照相关的状态
	if err := e.Encode(rf.lastIncludedIndex); err != nil {
		fmt.Println(err)
	}
	if err := e.Encode(rf.lastIncludedTerm); err != nil {
		fmt.Println(err)
	}
	if err := e.Encode(rf.lastIncludedCmdIndex); err != nil {
		fmt.Println(err)
	}

	data := w.Bytes()
	return data
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintf("%d without state", rf.me)
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
	var logs []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	var lastIncludedCmdIndex int
	if err := d.Decode(&currentTerm); err != nil {
		fmt.Printf("decode failed from readPersist(): currentTerm. err: %v\n", err)
	} else if err := d.Decode(&votedFor); err != nil {
		fmt.Printf("decode failed from readPersist(): votedFor. err: %v\n", err)
	} else if err := d.Decode(&logs); err != nil {
		fmt.Printf("decode failed from readPersist(): logs. err: %v\n", err)
	} else if err := d.Decode(&lastIncludedIndex); err != nil {
		fmt.Printf("decode failed from readPersist(): lastIncludedIndex. err: %v\n", err)
	} else if err := d.Decode(&lastIncludedTerm); err != nil {
		fmt.Printf("decode failed from readPersist(): lastIncludedTerm. err: %v\n", err)
	} else if err := d.Decode(&lastIncludedCmdIndex); err != nil {
		fmt.Printf("decode failed from readPersist(): lastIncludedCmdIndex. err: %v\n", err)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastIncludedCmdIndex = lastIncludedCmdIndex
		DPrintf("%d readPersist lenOfLogs: %d", rf.me, len(rf.logs))
	}
}

func (rf *Raft) logIndex2LogPos(logIndex int) int {
	return logIndex - rf.lastIncludedIndex
}

// 获取最后一条日志
func (rf *Raft) getLastLog() LogEntry {
	return rf.logs[len(rf.logs)-1]
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
		// DPrintf("%d find high candicate, become follower", rf.me)
	}
	curRole := rf.role
	curTerm := rf.currentTerm
	lastLogIdx := rf.getLastLog().LogIndex
	lastLogTerm := rf.getLastLog().LogTerm
	rf.mu.Unlock()

	reply.VoteGranted = false
	reply.FollowerTerm = curTerm
	if args.CandicateTerm < curTerm {
		// 候选者任期落后, 拒绝投票
		DPrintf("server %d reject vote to %d because of small term", rf.me, args.CandicateId)
		return
	}

	// 选举限制(防止缺少log entry 的后选择被选择为 leader: 如果日志落后则拒绝投票)
	if (args.LastLogTerm < lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIdx) {
		// 日志落后, 拒绝投票
		DPrintf("server %d reject vote to %d because of small log", rf.me, args.CandicateId)
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
			DPrintf("server %d(role: %d) vote to %d", rf.me, rf.role, args.CandicateId)
		}
		rf.mu.Unlock()
	case CANDICATE:
		// 不做操作
		// DPrintf("%d reject vote to %d because of as candicate", rf.me, args.CandicateId)
	case LEADER:
		// DPrintf("%d reject vote to %d because of as leader", rf.me, args.CandicateId)
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

// 在指定时间内执行, 超时则返回 false, 第一个参数表示是否超时, 第二个参数表示是否收到响应
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

	LeaderTerm   int        // leader 的当前任期
	LeaderId     int        // leader 的 id
	PrevLogIndex int        // 新日志条目的前一个日志条目的 index
	PrevLogTerm  int        // 新日志条目的前一个日志条目的 term
	Entries      []LogEntry // 要保存的日志条目, 如果是 heartbeat , 长度为 0
	LeaderCommit int        // leader 的 commitIndex
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
	defer rf.mu.Unlock()
	role := rf.role
	// 重置一些状态
	if args.LeaderTerm >= rf.currentTerm {
		// leader 任期更高, 更新状态
		rf.becomeFollower(args.LeaderTerm)
		reply.FollowerTerm = rf.currentTerm
		reply.Success = true
	} else {
		// leader 的 term 更小, 直接返回
		reply.FollowerTerm = rf.currentTerm
		reply.Success = false
		return
	}

	// 新 leader 的第一条 heartbeat 或正常的 heartbeat
	switch role {
	case FOLLOWER, CANDICATE:
		// 重置心跳超时定时器 或 停止正在进行的选举
		rf.heartBeatCh <- true
	case LEADER:
		// 不能发送 heartbeatCh 因为 leader 阶段没有接收 heartbeatCh
	}

	// DPrintf("follower %d recv heartbeat from leader %d, entries: %v, lenOfEntries: %d, leadercommitIndex: %d", rf.me, args.LeaderId, args.Entries, len(args.Entries), args.LeaderCommit)

	// 检查日志是否冲突
	if args.Entries != nil {
		// PrevLogIndex 在快照内, 定位到最新的 logIndex
		if args.PrevLogIndex < rf.lastIncludedIndex {
			reply.Success = false
			reply.Xterm = -1 // 此时 args.PrevLogIndex 不存在
			reply.Xlen = 0
			return
		}

		if rf.getLastLog().LogIndex >= args.PrevLogIndex {
			// 该 preLogindex 存在, 比较 term
			DPrintf("follower %d before update, args.PrevLogIndex: %d, lastLogIndex: %d, lastIncludedIndex: %d, lenOfLog: %d",
				rf.me, args.PrevLogIndex, rf.getLastLog().LogIndex, rf.lastIncludedIndex, len(rf.logs))
			logPos := rf.logIndex2LogPos(args.PrevLogIndex)
			if rf.logs[logPos].LogTerm == args.PrevLogTerm {
				// term 也相同, 日志不冲突
				// 更新 logs
				rf.logs = rf.logs[:logPos+1]
				rf.logs = append(rf.logs, args.Entries...)
				reply.Success = true
				// 保存持久化变量
				rf.persist()
				DPrintf("server %d delete after index %d logs, append len: %d logs, lenOfLog: %d",
					rf.me, args.PrevLogIndex, len(args.Entries), len(rf.logs))
			} else {
				// term 不相同
				reply.Success = false
				reply.Xterm = rf.logs[logPos].LogTerm // 冲突日志的 Logterm
				for i := 1; i <= logPos; i++ {
					if rf.logs[i].LogTerm == reply.Xterm {
						reply.Xindex = rf.logs[i].LogIndex // xterm 的第一个 entry 索引
						break
					}
				}
				// 删除该entry后的所有日志(保留args.PrevLogIndex以前的日志)
				rf.logs = rf.logs[:logPos]
				DPrintf("server %d delete after index %d logs, lenOfLog: %d", rf.me, args.PrevLogIndex, len(rf.logs))
				DPrintf("server %d reply logX: %v", rf.me, reply)
				// 保存持久化变量
				rf.persist()
				return // 不进行更新 commitIndex, 因为日志还没有同步, 会把错误的操作更新到状态机
			}
		} else {
			// 该 prevLogindex 不存在, 不做处理, 返回 false
			reply.Success = false
			reply.Xterm = -1
			reply.Xlen = rf.getLastLog().LogIndex // 回复最后一个 log index
			DPrintf("server %d reply logX: %v", rf.me, reply)
			return // 不进行更新 commitIndex, 因为日志还没有同步, 会把错误的操作更新到状态机
		}
	}

	// 更新 commitIndex, 并把提交应用到状态机 (日志已同步或者正常心跳(说明日志已同步)都可以进行更新)
	if args.LeaderCommit > rf.commitIndex {
		DPrintf("server %d args.LeaderCommit:%d > rf.commitIndex:%d, lenOfLog:%d", rf.me, args.LeaderCommit, rf.commitIndex, len(rf.logs))
		i := rf.commitIndex + 1
		for ; i <= args.LeaderCommit && i <= rf.getLastLog().LogIndex; i++ {
			// DPrintf("follower %d log[%d].IsInternalLog: %v", rf.me, i, rf.logs[i].IsInternalLog)
			if !rf.logs[rf.logIndex2LogPos(i)].IsInternalLog {
				// 非内部 log
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[rf.logIndex2LogPos(i)].Command,
					CommandIndex: rf.logs[rf.logIndex2LogPos(i)].CommandIndex,
					LogIndex:     rf.logs[rf.logIndex2LogPos(i)].LogIndex,
				}
				rf.applyChan <- applyMsg
				DPrintf("server %d apply command: %v, commandIndex: %d, logIndex: %d, logPos: %d, term: %d",
					rf.me, applyMsg.Command, applyMsg.CommandIndex, rf.logs[rf.logIndex2LogPos(i)].LogIndex, rf.logIndex2LogPos(i), rf.logs[rf.logIndex2LogPos(i)].LogTerm)
			}
			rf.commitIndex = i
			rf.lastApplied = i
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 在指定时间内执行, 超时则返回 false, 第一个参数表示是否超时, 第二个参数表示是否收到响应
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
		if rf.role == LEADER {
			rf.mu.Unlock()
			return
		}
		DPrintf("server %d(role: %d) start election", rf.me, rf.role)
		rf.currentTerm++
		rf.role = CANDICATE
		rf.votedFor = rf.me // 投票给自己
		// 保存持久化变量
		rf.persist()
		term := rf.currentTerm
		lastLogTerm := rf.getLastLog().LogTerm
		lastLogIdx := rf.getLastLog().LogIndex
		rf.mu.Unlock()

		// 选举阶段超时 定时器
		ticker := time.NewTicker(400 * time.Millisecond)
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

				ok := false
				for !ok {
					rf.mu.Lock()
					role := rf.role
					curTerm := rf.currentTerm
					rf.mu.Unlock()
					if role != CANDICATE || curTerm != term {
						// 退出请求投票
						// DPrintf("%d quit send requestVote to %d", rf.me, server)
						return
					}

					var timeOut bool
					// 反复请求
					// DPrintf("%d send requestVote to %d", rf.me, server)
					reply := &RequestVoteReply{
						VoteGranted: false,
					}
					timeOut, ok = rf.sendRequestVoteWithTimeOut(server, args, reply, 50*time.Millisecond)

					if !timeOut {
						// DPrintf("candicate %d recv requestVote from %d, ok: %v", rf.me, server, ok)
						if ok {
							voteCh <- reply
							break
						}
					}
				}

			}(voteCh, i)
		}

		// 选举获得投票计数
		getVote := 1

	Loop:
		for {
			select {
			case <-ticker.C:
				// 超时, 后续一段时间后继续开始选举
				// DPrintf("%d loss election because of timeout", rf.me)
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
						DPrintf("server %d win election", rf.me)
						return
					}
				} else {
					if reply.FollowerTerm > rf.currentTerm {
						// 发现任期更大的服务器, 切换为 follower
						rf.mu.Lock()
						rf.becomeFollower(reply.FollowerTerm)
						rf.mu.Unlock()
						DPrintf("server %d loss election because of other has high term, become follower", rf.me)
						return // 停止选举
					}
					// 其他情况不用处理, 继续等待接收
				}

			case <-rf.heartBeatCh:
				DPrintf("server %d loss election because of other become leader", rf.me)
				return
			}

		}

		if rf.killed() {
			// DPrintf("%d election quit because of killed", rf.me)
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
			atomic.AddInt32(&rf.heartBeatCnt, 1)

		case <-rf.quitElection:
			return // 退出线程
		}
	}
}

// HeartBeatToServer 线程
func (rf *Raft) HeartBeatToServer(peerIndex int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != LEADER {
			// 收到了 term 更大的心跳或 requestVote, 已切换为follower
			rf.mu.Unlock()
			return
		}
		flag := rf.leaderPtr.nextIndexs[peerIndex] <= rf.lastIncludedIndex
		rf.mu.Unlock()

		if flag {
			// 需要发送 InstallSnapshot
			rf.doInstallSnapshotRPC(peerIndex)
		} else {
			// 需要发送 AppendEntries
			rf.doAppendEntriesRPC(peerIndex)
		}
	}
}

// doAppendEntriesRPC 的逻辑
// TODO: 优化心跳和发送日志, 可以以 100ms 定时器为 case1, start(cmd) 新增日志发送提醒channel为 case2, 加快达成共识的速度
func (rf *Raft) doAppendEntriesRPC(peerIndex int) {
	rf.mu.Lock()
	args := &AppendEntriesArgs{
		LeaderTerm: rf.currentTerm,
		LeaderId:   rf.me,
		// 以下为 log entries 的参数
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		Entries:      nil,
		LeaderCommit: rf.commitIndex, // 如果是第一次 heartbeat, commitIndex == 0
	}
	if rf.getLastLog().LogIndex >= rf.leaderPtr.nextIndexs[peerIndex] {
		// 有需要提交的日志
		if rf.leaderPtr.nextIndexs[peerIndex] <= rf.lastIncludedIndex {
			// 该日志已经被压缩了
			rf.mu.Unlock()
			return
		}
		logPos := rf.logIndex2LogPos(rf.leaderPtr.nextIndexs[peerIndex])
		DPrintf("server %d before doAppendEntriesRPC to server %d, logPos: %d, lastIncludedIndex: %d, lenOfLog: %d",
			rf.me, peerIndex, logPos, rf.lastIncludedIndex, len(rf.logs))
		args.PrevLogIndex = rf.logs[logPos-1].LogIndex
		args.PrevLogTerm = rf.logs[logPos-1].LogTerm
		args.Entries = rf.logs[logPos:] // 剩下的全部日志
		DPrintf("server %d doAppendEntriesRPC to server %d, logPos: %d, PrevLogIndex: %d, PrevLogTerm: %d",
			rf.me, peerIndex, logPos, args.PrevLogIndex, args.PrevLogTerm)
	}
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}

	// 如果有需要复制的 log entry, 则以更短的时间间隔发送 appendentry
	timeOut, ok := rf.sendAppendEntriesWithTimeOut(peerIndex, args, reply, 50*time.Millisecond)
	if timeOut {
		// 任务超时
		time.Sleep(10 * time.Millisecond) // 10ms 后重发
		return
	} else {
		// DPrintf("leader %d heartbeat recv ok from follower %d, ok: %v", rf.me, peerIndex, ok)
		if ok {
			if reply.Success {
				if args.Entries != nil {
					// follower 已复制了日志
					rf.mu.Lock() // Lock() 防止 leaderptr 被置空
					if rf.currentTerm != args.LeaderTerm {
						rf.mu.Unlock()
						return // leader 已被弃用
					}
					rf.leaderPtr.nextIndexs[peerIndex] = args.Entries[len(args.Entries)-1].LogIndex + 1
					rf.leaderPtr.matchIndexs[peerIndex] = args.Entries[len(args.Entries)-1].LogIndex
					DPrintf("get success from %d, replication logIndex: %d, nextIndex: %d",
						peerIndex, args.Entries[len(args.Entries)-1].LogIndex, rf.leaderPtr.nextIndexs[peerIndex])
					// 尝试更新 commitIndex, 并应用命令到状态机
					if args.Entries[len(args.Entries)-1].LogIndex > rf.commitIndex {
						// 注意只能提交本任期内的log
						i := args.Entries[len(args.Entries)-1].LogIndex
						iPos := rf.logIndex2LogPos(i)
						for i > rf.commitIndex && rf.logs[iPos].LogTerm == rf.currentTerm {
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
									if !rf.logs[rf.logIndex2LogPos(k)].IsInternalLog {
										// 非内部log需要应用到状态机
										applyMsg := ApplyMsg{
											CommandValid: true,
											Command:      rf.logs[rf.logIndex2LogPos(k)].Command,
											CommandIndex: rf.logs[rf.logIndex2LogPos(k)].CommandIndex,
											LogIndex:     rf.logs[rf.logIndex2LogPos(k)].LogIndex,
										}
										rf.applyChan <- applyMsg
										DPrintf("server %d apply command: %v, commandIndex: %d, logIndex: %d, logPos: %d, term: %d,",
											rf.me, applyMsg.Command, applyMsg.CommandIndex, rf.logs[rf.logIndex2LogPos(k)].LogIndex, rf.logIndex2LogPos(k), rf.logs[rf.logIndex2LogPos(k)].LogTerm)
									}
								}
								rf.commitIndex = i
								rf.lastApplied = i
								break
							}
							// 注意循环条件变化
							i--
							iPos = rf.logIndex2LogPos(i)
						}
					}
					rf.mu.Unlock()
					return // 这是日志附加, 立即执行下一个 appendentries, 保证后来的日志能尽快达成共识
				} else {
					// 附加日志为空说明这是心跳, 100ms 后再发
					rf.mu.Lock() // Lock() 防止 leaderptr 被置空
					if rf.currentTerm != args.LeaderTerm {
						rf.mu.Unlock()
						return // leader 已被弃用
					}
					if rf.leaderPtr.nextIndexs[peerIndex] <= rf.getLastLog().LogIndex {
						rf.mu.Unlock()
						return // 还有待达成共识的日志, 立即发送
					}
					rf.mu.Unlock()
					time.Sleep(100 * time.Millisecond) // 心跳 100ms
				}
			} else {
				if args.LeaderTerm < reply.FollowerTerm {
					// leaderTerm < replyterm
					// 转换为 follower
					rf.mu.Lock()
					rf.becomeFollower(reply.FollowerTerm)
					rf.mu.Unlock()
					return // 直接退出不再发送 heartbeat
				} else {
					// 日志冲突导致的失败, 快速定位冲突日志
					rf.mu.Lock()
					if rf.currentTerm != args.LeaderTerm {
						rf.mu.Unlock()
						return // leader 已被弃用
					}
					if reply.Xterm != -1 {
						// 查找是否有 Xterm
						for i := rf.getLastLog().LogIndex; i >= rf.logs[0].LogIndex; i-- {
							iPos := rf.logIndex2LogPos(i)
							if rf.logs[iPos].LogTerm == reply.Xterm {
								// 存在冲突 Xterm, 直接在 冲突term开始的位置 备份
								rf.leaderPtr.nextIndexs[peerIndex] = i + 1
								break
							} else if rf.logs[iPos].LogTerm < reply.Xterm {
								// 不存在冲突 Xterm, 直接从 Xindex 备份
								rf.leaderPtr.nextIndexs[peerIndex] = reply.Xindex
								break
							}
						}
					} else {
						// prevLogIndex 不存在, 直接从日志最后一个开始备份
						rf.leaderPtr.nextIndexs[peerIndex] = reply.Xlen + 1
					}
					DPrintf("server %d (leader: %d) nextIndex: %d", peerIndex, rf.me, rf.leaderPtr.nextIndexs[peerIndex])
					rf.mu.Unlock()
					return
				}
			}
		}
	}
}

// doInstallSnapshotRPC 的逻辑
func (rf *Raft) doInstallSnapshotRPC(peerIndex int) {
	rf.mu.Lock()
	DPrintf("server %d doInstallSnapshotRPC starts, to peerId[%d]", rf.me, peerIndex)

	args := &InstallSnapshotArgs{
		Term:                 rf.currentTerm,
		LeaderId:             rf.me,
		LastIncludedIndex:    rf.lastIncludedIndex,
		LastIncludedTerm:     rf.lastIncludedTerm,
		LastIncludedCmdIndex: rf.lastIncludedCmdIndex,
		Offset:               0,
		Data:                 rf.persister.ReadSnapshot(),
		Done:                 true,
	}
	rf.mu.Unlock()
	reply := &InstallSnapshotReply{}

	// 请求时不要持有锁
	timeOut, ok := rf.sendInstallSnapshotWithTimeOut(peerIndex, args, reply, 50*time.Millisecond)

	if timeOut {
		// 任务超时
		time.Sleep(10 * time.Millisecond)
		return // 10ms 后重试
	} else if ok {
		// 收到响应
		rf.mu.Lock()
		if rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return // leader 被弃用
		}

		if reply.Term > rf.currentTerm {
			// 发现更高的 term, 成为 follower
			rf.becomeFollower(reply.Term)
			rf.mu.Unlock()
			return
		} else {
			// 成功接收
			rf.leaderPtr.nextIndexs[peerIndex] = args.LastIncludedIndex + 1 // 定位到已应用的下一个 index
			rf.leaderPtr.matchIndexs[peerIndex] = args.LastIncludedIndex
			// 不用尝试更新 commitIndex, 因为只有已经应用到 kv 服务层的日志才会被压缩, 这些日志都是已经被提交了的
			rf.mu.Unlock()
			// 成功接收不要 sleep, 为了后面的log能尽快达成共识
			// time.Sleep(100 * time.Millisecond)
		}
	} else {
		// 没有收到响应, 10ms 后重试
		time.Sleep(10 * time.Millisecond)
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
	cmdIndex := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		// 如果 raft 被终止, 应该优雅返回
		return cmdIndex, term, isLeader
	}
	isLeader = (rf.role == LEADER)
	if !isLeader {
		// 不是 Leader 直接返回 false
		return cmdIndex, term, isLeader
	}
	cmdIndex = rf.getLastLog().CommandIndex + 1 // commandIndex + 1
	logIndex := rf.getLastLog().LogIndex + 1
	term = rf.currentTerm
	newLog := LogEntry{
		LogTerm:       term,
		LogIndex:      logIndex,
		Command:       command,
		CommandIndex:  cmdIndex,
		IsInternalLog: false, // 非内部 log
	}
	// 追加到 logs 中
	rf.logs = append(rf.logs, newLog)
	// DPrintf("start append command: %v at index: %d, commandIndex: %d", command, logIndex, index)
	// 保存持久化变量
	rf.persist()
	DPrintf("server %d append Command: %v, Commandindex: %d, LogIndex: %d, lenOfLog: %d, term: %d",
		rf.me, command, cmdIndex, logIndex, len(rf.logs), rf.currentTerm)
	return cmdIndex, term, isLeader
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
	// DPrintf("%d call Kill ... ", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ------- 日志压缩相关函数 -----------------------

// InstallSnapshot 请求参数
type InstallSnapshotArgs struct {
	Term                 int    // leader 的任期
	LeaderId             int    // leader id
	LastIncludedIndex    int    // 快照的最后一个日志索引
	LastIncludedTerm     int    // 快照的最后一个日志任期
	LastIncludedCmdIndex int    // 快照的最后一个日志的 cmdIndex
	Offset               int    // 数据块偏移量
	Data                 []byte // 快照数据块
	Done                 bool   // 是否是最后一个块
}

type InstallSnapshotReply struct {
	Term int // follower 当前的任期, 供 leader 更新
}

// InstallSnapshot rpc handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// follower 的 term 更大, 拒绝请求
	if args.Term < rf.currentTerm {
		return
	}

	// leader 的任期更大, 成为 follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		// 此次快照落后, 忽略它
		return
	} else {
		// leader 快照比本地快照长

		// 快照外还有日志, 判断是否需要保留
		if args.LastIncludedIndex < rf.getLastLog().LogIndex {
			if rf.logs[rf.logIndex2LogPos(args.LastIncludedIndex)].LogTerm != args.LastIncludedTerm {
				// 日志存在冲突, 扔掉快照外的所有日志, 释放内存
				rf.logs = make([]LogEntry, 0)
				// 注意添加哨兵节点
				rf.logs = append(rf.logs, LogEntry{
					LogTerm:       args.LastIncludedTerm,
					LogIndex:      args.LastIncludedIndex,
					Command:       nil,
					CommandIndex:  args.LastIncludedCmdIndex,
					IsInternalLog: true,
				})
			} else {
				// 不冲突, 保留后续日志(注意保留哨兵节点)
				afterLog := make([]LogEntry, rf.getLastLog().LogIndex-args.LastIncludedIndex+1)
				copy(afterLog, rf.logs[rf.logIndex2LogPos(args.LastIncludedIndex):])
				rf.logs = afterLog
				rf.logs[0].IsInternalLog = true
				rf.logs[0].Command = nil
			}
		} else {
			// 快照比本地日志长, 直接全部清空, 注意保留哨兵节点
			rf.logs = make([]LogEntry, 0)
			// 注意添加哨兵节点
			rf.logs = append(rf.logs, LogEntry{
				LogTerm:       args.LastIncludedTerm,
				LogIndex:      args.LastIncludedIndex,
				Command:       nil,
				CommandIndex:  args.LastIncludedCmdIndex,
				IsInternalLog: true,
			})
		}
	}

	// 更新快照元数据
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastIncludedCmdIndex = args.LastIncludedCmdIndex

	// 持久化状态和快照
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), args.Data)
	// 提交到 kv 层
	rf.installSnapshotToApplication()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// 在指定时间内执行, 超时则返回 false, 第一个参数表示是否超时, 第二个参数表示是否收到响应
func (rf *Raft) sendInstallSnapshotWithTimeOut(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply, ms time.Duration) (bool, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), ms)
	defer cancel()

	taskDone := make(chan bool, 1)
	go func() {
		ok := rf.sendInstallSnapshot(server, args, reply)
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

// 判断日志是否需要压缩
func (rf *Raft) ExceedLogSize(logSize int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.persister.RaftStateSize() >= logSize
}

// 执行快照由 kv 服务层调用, 删除掉被压缩的日志并保存快照
func (rf *Raft) TakeSnapshot(snapshot []byte, lastIncludedIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 此快照比最近一次快照落后, 忽略这个快照
	if lastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	DPrintf("server[%d] TakeSnapshot begins, IsLeader[%v] snapshotLastIndex[%d] lastIncludedIndex[%d] lastIncludedTerm[%d] lastIncludedCmdIndex[%d]",
		rf.me, rf.role == LEADER, lastIncludedIndex, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.lastIncludedCmdIndex)

	// 更新快照元信息(最后才能更新 lastIncludedIndex)
	rf.lastIncludedTerm = rf.logs[rf.logIndex2LogPos(lastIncludedIndex)].LogTerm
	rf.lastIncludedCmdIndex = rf.logs[rf.logIndex2LogPos(lastIncludedIndex)].CommandIndex

	// 压缩日志
	afterLog := make([]LogEntry, 0)
	// 这里位置 0 保存了 lastEntry 作为哨兵节点
	afterLog = append(afterLog, rf.logs[rf.logIndex2LogPos(lastIncludedIndex):]...)
	// 位置 0 始终是哨兵节点(Command == nil)
	afterLog[0].Command = nil

	rf.logs = afterLog

	// 最后才能更新 lastIncludedIndex
	rf.lastIncludedIndex = lastIncludedIndex
	// 持久化 snapshot 和 raft state
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot)

	DPrintf("server[%d] TakeSnapshot end, IsLeader[%v] snapshotLastIndex[%d] lastIncludedIndex[%d] lastIncludedTerm[%d] lastIncludedCmdIndex[%d]",
		rf.me, rf.role == LEADER, lastIncludedIndex, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.lastIncludedCmdIndex)
}

// 把快照应用到 kv 层
func (rf *Raft) installSnapshotToApplication() {
	applyMsg := ApplyMsg{
		CommandValid:         false,
		Snapshot:             rf.persister.ReadSnapshot(),
		LastIncludedIndex:    rf.lastIncludedIndex,    // 首次是 从持久化状态读取的快照的最后一个log index
		LastIncludedTerm:     rf.lastIncludedTerm,     // 首次是 从持久化状态读取的快照的最后一个log term
		LastIncludedCmdIndex: rf.lastIncludedCmdIndex, // 首次是 从持久化状态读取的快照的最后一个log cmdIndex
	}

	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex

	DPrintf("server %d installSnapshotToApplication, SnapshotSize[%d], lastIncludedIndex[%d], lastIncludedTerm[%d]",
		rf.me, len(applyMsg.Snapshot), applyMsg.LastIncludedIndex, applyMsg.LastIncludedTerm)

	rf.applyChan <- applyMsg
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
	rf := &Raft{
		peers:                peers,
		persister:            persister,
		me:                   me,
		applyChan:            applyCh,
		currentTerm:          0,
		votedFor:             -1, // -1 表示没有投票
		logs:                 make([]LogEntry, 0),
		commitIndex:          0,
		lastApplied:          0,
		role:                 FOLLOWER,
		quitElection:         make(chan bool),
		heartBeatCh:          make(chan bool, 8),
		leaderPtr:            nil,
		lastIncludedIndex:    0,
		lastIncludedTerm:     0,
		lastIncludedCmdIndex: 0,
	}

	// Your initialization code here (2A, 2B, 2C).

	// 选举超时相关变量
	rf.timeOutMS = int32(200 + rf.me*50)
	atomic.StoreInt32(&rf.heartBeatCnt, 0)

	// initialize from state persisted before a crash
	DPrintf("%d initialize", rf.me)
	rf.readPersist(persister.ReadRaftState())

	// 哨兵节点(必须读取了持久化状态再创建哨兵节点)
	if len(rf.logs) == 0 {
		dummyLog := LogEntry{
			LogTerm:       rf.lastIncludedTerm,
			LogIndex:      rf.lastIncludedIndex,
			Command:       nil,
			CommandIndex:  rf.lastIncludedCmdIndex,
			IsInternalLog: true,
		}
		rf.logs = append(rf.logs, dummyLog)
	}

	// 首次启动需要恢复 snapshot
	rf.installSnapshotToApplication()

	// 启动超时选举的协程
	go rf.TimeOutToElection()

	return rf
}
