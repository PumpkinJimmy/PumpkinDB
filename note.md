## Raft方法论
1. Leader Election
   - 选择一个节点称为leader
   - 检测到leader失效则选举出新的Leader

2. Log Replication
   - leader就接受客户端的命令，并追加到日志中
   - leader将日志复制到其他服务器
3. Safety
   - 保持日志一致
   - 只有持有最新日志的节点才有资格竞选leader

## Term
Term理解为“朝代”

- 每个term只有一位leader
- 每个节点需要维护当前所处term
- 每次RPC都必须携带term信息
- 如果收到的RPC中term新于当前维护的term，则节点term更新，并称为follower
- 如果收到的RPC中term老于当前维护的term，则报错

## Leader election
- 节点的三个状态
  - Follower: 被动，但是要求Leader周期的heartbeat信号
  - Candidate: 收不到Leader的heartbeat，竞选成为Leader，**向所有成员广播RequestVote**
  - Leader：收到超过半数的投票，自动成为Leader，**负责处理写请求发送AppendEntries**
- 一些细节
  - 成为Candidate同时立刻给自己投票
  - 网络分区可能导致**分票，分区内所有成员在限时之内都收不到超半数选票，则本朝共识失败，直接进入下一轮**（也就是说一些小的网络分区中可能永远产生不了Leader，这正是我们期望的）
  - 在处于Candidate状态时，如果收到Leader的RPC，则自动降级回Follower
  - **超时时间从$[T,2T]$区间中随机选。其中要求T远大于广播时间**

## Normal Operation: Log Replicating
1. 客户端向Leader发起请求
2. Leader将操作加入日志
3. **广播AppendEntries**请求，**正常工作的Follower复制该日志，并回复Leader**
4. 操作一旦*提交*（得到大多数响应）
   - Leader执行操作，返回给客户端
   - Leader记录当前成功提交的最大的日志Index
   - Follower执行操作

细节：
- 如何处理宕机/回复迟到的Follower：**重传直到它回复（注意需要并发）**

关于日志：
- 整个协议都假设日志存储在稳定存储器上的（也即是在故障中不会损毁的存储器，典型的如磁盘）
- 一条日志记录包括
  - term
  - command

## Log Matching
- 若两个副本上存储的日志的日志index和term都相等，则日志内容相等
- 若两个副本上的某日志记录相等，则其前面的日志也全部相等
- 若某日志记录已提交，则其前面的所有记录也已提交

如果一个Leader节点在一些命令还未提交之前就了宕机了，此时Follower可能还未进行日志复制。此时如果另外一个节点成为了新的Leader，此时老Follower就会存在与新Leader冲突的日志记录

定义：两个日志记录是*匹配*的，当且仅当它们的日志index和term都相等

解决的思想是**找到follower上最新的匹配的记录，然后将该记录后面不匹配的记录强制转换为follower上的记录**

这里的实现是：**在AppendEntries中Follower检查日志是否匹配，若不匹配，则拒绝该日志；Leader被拒绝后重试更小的日志**

## gRPC Async API
```python
server = grpc.aio.server(...)
await server.start()
await server.wait_until_termination()
```