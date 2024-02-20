module raft

go 1.20

require labrpc v0.0.0

require (
	github.com/cihub/seelog v0.0.0-20170130134532-f561c5e57575 // direct
	labgob v0.0.0 // direct
)

replace (
	labgob => ../labgob
	labrpc => ../labrpc
)
