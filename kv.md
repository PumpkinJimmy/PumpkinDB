分布式键值存储

要求：
1. 分布式架构：
   - 集中架构：leader election, consensus
   - 非集中式：DHT
2. 支持操作：PUT, GET, DEL
3. 分布式通信：RPC（直接上gRPC就好了）
4. 多用户：Lock的实现
5. 最终一致性：单调写
6. 容错与共识：Raft