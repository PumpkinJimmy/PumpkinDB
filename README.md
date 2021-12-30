## PumpkinDB

中山大学分布式系统期末项目：分布式键值数据库。

使用Python语言实现。

Feature (finished):
1. Simple GET-SET console

Feature (to be finished):
1. 集中式架构
2. 支持操作：PUT, GET, DEL
3. 分布式通信：RPC
4. 多用户支持
5. 最终一致性：单调写。需要实现客户端的exactly once语义
6. 容错与共识：Raft

Dependencies:
- grpc
- prompt-toolkit