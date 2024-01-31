module raft

go 1.20

require labrpc v0.0.0

require labgob v0.0.0 // indirect

replace (
	labgob => ../labgob
	labrpc => ../labrpc
)
