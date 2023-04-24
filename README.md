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
  ... Passed --   1.0    31    5352  
Test (Chain): varied chain test  ...  
  ... Passed --   6.0   168   23143  
PASS  
ok      crfs/node       7.007s  
每个测试下的3个数字分别表示运行时长，rpc总数，rpc传输的字节总数