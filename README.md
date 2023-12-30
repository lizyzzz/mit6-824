# mit6-824
mit6.824学习笔记及源码

### lab
* lab1: MapReduce  
* lab2: Raft for fault tolerant  
* lab3: K/V server base Raft  
* lab4: Sharded key value service sharding  

#### 系统设计的目标
* Performance --- scalability: 扩展性  
* Fault Tolerance --- Availability: 可用性  
* Fault Tolerance --- Recoverability: 可恢复性  
#### 一个常见的话题
* Consistency: 一致性  

#### MapReduce  
* 大规模数据集（大于1TB）的并行运算