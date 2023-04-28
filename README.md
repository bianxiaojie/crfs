# crfs
基于craq和hdfs改写的分布式文件系统

## Task 1 实现craq服务
### Task 1-1 实现链表配置的动态更新
#### 要求  
1.  当节点与zookeeper集群之间没有连接时，不断尝试与zookeeper集群建立连接。  
2.  建立连接成功后，节点向zookeeper集群创建一个临时文件，存储节点的地址。  
其他在该路径下创建临时文件的节点与当前节点共同构成一个链表，节点在链表  
中的顺序由文件名的大小决定。  
3.  创建文件成功后，从zookeeper集群读取同一文件夹的所有文件名，对文件  
名进行排序，得到链表的节点顺序，找到当前节点在链表中的位置，找到节  
点的前驱、后继和尾节点。
4.  从zookeeper集群读取前驱、后继和尾节点的地址。
5.  存储第3步和第4步获取的链表信息。
6.  监听第3步读取的文件夹的变化。  
#### 提示
- 需要实现的代码：crfs/node/node.go中的watchChain的2,3,4,5,6。
- 需要阅读的代码：crfs/node/node.go, /crfs/zk/zk.go。
- 第2步在创建文件时需要同时指定zk.FlagEphemeral和zk.FlagSequence标志，  
节点路径为chainPath + "/" + prefix
- 第3步和第4步要向zookeeper集群发送多个请求，因此必须所有的请求都执行成功，  
才能更新本地的链表信息。
- 第5步在更新链表信息时需要加锁。
- 第6步可以借助watch机制实现，即在第3步调用zkConn.Children时将watch指定为true，  
然后在完成第5步后，阻塞读取zkConn.Children返回的watcher。
#### 测试
在crfs/node路径下执行go test -run TestChain命令，以下输出表示通过测试：  
Test (Chain): basic chain test ...  
  ... Passed --   1.0    50    8312    0  
Test (Chain): varied chain test ...  
  ... Passed --   6.1   185   30559    0  
Test (Chain): test chain varied more ...  
  ... Passed --  15.6   865  142117    0  
PASS  
ok      crfs/node       22.655s  
每个测试下的4个数字分别表示运行时长，rpc总数，rpc传输的字节总数和提交的日志数
### Task 1-2 实现一致性协议
#### 要求  
(1)实现日志的复制  
流程概述：对于每一个节点，记录后继节点的名称和最新完成复制的日志索引，当后继节点的日志记录落后当前节点的日志记录时，将所有新日志发送给后继节点。  
注意：每当后继节点变化时，都应当重新向后继节点查询已完成复制的最新索引日志。复制日志成功时也要更新该索引值。  
提示：首先，补全kickOffCopy函数中的步骤1和步骤2。然后，根据注释实现QueryLastIndex和SendLogs函数。  
(2)实现日志的提交  
流程概述：对于尾节点，可以直接在本地提交日志。但有两个前提条件，首先尾节点要从zookeeper中获取到提交锁，其次，尾节点在更新本地的commitIndex之前，要先将新的commitIndex更新到zookeeper中。  
提示：补全kickOffCommit函数中的步骤2.1-4。
(3)实现日志的确认
流程概述：对于每一个节点，记录前驱节点的名称和最新确认的日志索引，当节点提交新的日志时，向前驱节点发送确认消息，让前驱节点提交新的日志。
注意：每当前驱节点变化时，都应当重新向前驱节点查询已提交的最新日志索引。确认成功时也要更新该索引值。  
提示：首先，补全kickOffAck函数中的步骤1和步骤2。然后，根据注释实现Ack。
(4)实现日志的执行  
流程概述：每当有日志提交时，自增node.lastApplied，然后将索引为node.lastApplied的日志发送到node.applyCh中。  
提示：补全kickOffApply函数的步骤1和步骤2。
#### 测试  
在crfs/node路径下执行go test -run TestAgreement命令，以下输出表示通过测试：
Test (Agreement): basic agreement test ...  
  ... Passed --   1.2    75   12468    3  
Test (Agreement): concurrent agreement test ...  
  ... Passed --   1.4    66   11492   30  
Test (Agreement): agreement test with crash and restart ...  
  ... Passed --   9.1   514   85243   40  
Test (Agreement): agreement test with disconnect ...  
  ... Passed --   3.6   192   31713   20  
Test (Agreement): agreement test where committed node crashed ...  
  ... Passed --   5.1   171   28488   30  
Test (Agreement): agreement test with header partition ...  
  ... Passed --   1.6   104   16599    3  
Test (Agreement): agreement test with tail partition ...  
  ... Passed --   3.6   123   19709    3  
Test (Agreement): concurrent agreement test (unrealiable) ...  
  ... Passed --   3.8   133   23977  160  
PASS  
ok      crfs/node       29.435s  