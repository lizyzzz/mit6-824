module kvraft

go 1.20

require (
	labgob v0.0.0 // direct
	labrpc v0.0.0 // direct
	models v0.0.0 // direct
	porcupine v0.0.0 // direct
	raft v0.0.0 // direct
)

require github.com/cihub/seelog v0.0.0-20170130134532-f561c5e57575 // indirect

replace (
	labgob => ../labgob
	labrpc => ../labrpc
	models => ../models
	porcupine => ../porcupine
	raft => ../raft
)
