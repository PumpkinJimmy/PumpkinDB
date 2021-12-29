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