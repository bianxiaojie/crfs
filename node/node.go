package node

import (
	"bytes"
	"crfs/persister"
	"crfs/rpc"
	izk "crfs/zk" // 封装了zookeeper连接和客户端的接口
	"encoding/gob"
	"log"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-zookeeper/zk"
)

const (
	PosPrev NeighbourPos = iota // 前驱节点标记
	PosNext                     // 后继节点标记
	PosTail                     // 尾节点标记
)

// 用于Node.neighbours的key
type NeighbourPos int

// 邻居节点,包括三种类型,分别为前驱节点,后继节点和尾节点
type Neighbour struct {
	name    string     // 邻居节点名
	address string     // 邻居节点地址
	client  rpc.Client // 邻居节点rpc服务的客户端
}

// 在Node提交某个Log后,将Log中的Command封装成ApplyMsg发送到applyCh中
type ApplyMsg struct {
	Command      interface{}
	CommandIndex int
}

// 日志
type Log struct {
	Command interface{}
}

// 节点,可以看成一台服务器,对应zookeeper中一个临时文件,基本架构如下:
//
//	                              [chainPath]
//	                             /           \
//	[服务器1]   ---创建--->   [文件3]       [文件5]   <---创建---   [服务器2]
//
// 服务器与他在zookeeper创建的文件生命期相同,在上图中
// 服务器1与zookeeper建立连接后,就会创建相应的文件,即文件3
// 服务器1与zookeeper断开连接后,文件3就会被zookeeper自动删除
type Node struct {
	mu sync.Mutex // 排他锁,用于互斥变量的修改和访问

	// 常量:
	zkConn     izk.ZKConn      // zkConn: 与zookeeper服务集群建立的连接
	clients    rpc.Clients     // 用于创建邻居节点rpc服务的客户端
	chainPath  string          // 链表在zookeeper中的文件夹路径,形如/chain/chain1
	prefix     string          // 节点名的前缀
	address    string          // 节点地址
	commitPath string          // 尾节点提交的日志索引在zookeeper中的文件路径,形如/commit/chain1
	applyCh    chan<- ApplyMsg // 执行channel,用于接收已经提交的Log中的Command
	persister  persister.Persister

	// 原子变量:
	// 节点是否死亡
	dead int32

	// 互斥变量:
	// name: 节点在zookeeper相应的链表路径(/chain/chain1)下创建的文件的名称,由两部分构成${prefix}${index}
	// 其中prefix是一个固定字符串,可以自由选择,但必须是常量;index是一个递增的正整数,由zookeeper在创建文件时自动生成
	// 当节点与zookeeper连接成功时,应当在链表路径(/chain/chain1)下创建一个文件,表示节点加入链表中,并将name更新为创建的文件名
	// 当节点与zookeeper断开连接时,zookeeper将会自动删除该文件,节点同时需要将name更新为nil,表示节点不在链表中
	// 如何创建临时文件(自动删除)和序列文件(文件名追加递增的正整数的)可见ZKConn.Get
	name string

	// 邻居节点,示例:
	// prev := neighbours[PosPrev]
	neighbours map[NeighbourPos]Neighbour

	Logs        []Log // 日志列表
	CommitIndex int   // 已提交的最新日志的索引
	lastApplied int   // 发送到applyCh的最新日志的索引,初始值为CommitIndex

}

func MakeNode(zkConn izk.ZKConn, clients rpc.Clients, chainPath string, prefix string, address string, commitPath string, persister persister.Persister, applyCh chan<- ApplyMsg) *Node {
	node := &Node{}
	node.zkConn = zkConn
	node.clients = clients
	node.chainPath = chainPath
	node.prefix = prefix
	node.address = address
	node.commitPath = commitPath
	node.persister = persister
	node.applyCh = applyCh

	node.neighbours = make(map[NeighbourPos]Neighbour, 3)

	node.Logs = make([]Log, 0)
	node.CommitIndex = -1
	node.lastApplied = -1

	node.readPersist(persister.ReadNodeState())

	go node.watchChain()
	go node.kickOffCopy()
	go node.kickOffAck()
	go node.kickOffApply()

	return node
}

func (node *Node) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(node.CommitIndex)
	// 只持久化提交的日志
	e.Encode(node.Logs[:node.CommitIndex+1])
	data := w.Bytes()
	node.persister.SaveNodeState(data)
}

func (node *Node) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var commitIndex int
	var logs []Log

	if d.Decode(&commitIndex) != nil ||
		d.Decode(&logs) != nil {
		log.Fatalln("decode error")
	}

	node.CommitIndex = commitIndex
	node.Logs = logs
}

// 监控链表,每当链表发生变化时,重新从zookeeper获取最新的链表信息
func (node *Node) watchChain() {
	for {
		// 如果服务器被Kill,应当停止对链表的监控
		if node.killed() {
			break
		}

		// 避免单线程长时间占用CPU
		time.Sleep(10 * time.Millisecond)

		// 如果连接已关闭,则退出
		zkConn := node.zkConn
		if zkConn.Closed() {
			node.Kill()
			break
		}

		// 1.如果name == "",建立临时文件,存储自己的address,每个节点只能创建一次临时文件
		node.mu.Lock()
		name := node.name
		node.mu.Unlock()
		if name == "" {
			n, err := zkConn.Create(node.chainPath+"/"+node.prefix, []byte(node.address), zk.FlagEphemeral|zk.FlagSequence)
			if err != nil {
				continue
			}

			node.mu.Lock()
			node.name = n
			name = node.name
			node.mu.Unlock()
		}

		// 读数据之前先进行同步
		if _, err := zkConn.Sync(node.chainPath); err != nil {
			continue
		}

		// 2.读取node.chainPath文件夹,通过分析自己在链表中的位置,获取前驱节点,后继节点和尾节点
		chain, _, eventCh, err := zkConn.Children(node.chainPath, true)
		if err != nil {
			continue
		}
		sort.SliceStable(chain, func(i, j int) bool {
			if len(chain[i]) != len(chain[j]) {
				return len(chain[i]) < len(chain[j])
			}
			return chain[i] < chain[j]
		})
		var prev, next, tail string
		i := 0
		for ; i < len(chain); i++ {
			if name == chain[i] {
				break
			}
		}
		if i == len(chain) {
			go func() { <-eventCh }()
			continue
		}
		if i > 0 {
			prev = chain[i-1]

		}
		if i < len(chain)-1 {
			next = chain[i+1]
		}
		tail = chain[len(chain)-1]

		// 2.头节点需要检查自己是否包含zookeeper中存储的提交日志索引
		// 通过zkConn.Get(node.commitPath, false)获取提交的日志索引,与node.CommitIndex
		if prev == "" {
			data, _, _, err := zkConn.Get(node.commitPath, false)
			if err != nil {
				go func() { <-eventCh }()
				continue
			}
			commitIndex, err := strconv.Atoi(string(data))
			if err != nil {
				log.Fatalln(err)
			}
			if node.CommitIndex < commitIndex {
				node.Kill()
				go func() { <-eventCh }()
				break
			}
		}

		// 3.分别读取前驱节点,后继节点和尾节点的address
		var prevAddress, nextAddress, tailAddress string
		if prev != "" {
			data, _, _, err := zkConn.Get(node.chainPath+"/"+prev, false)
			if err != nil {
				go func() { <-eventCh }()
				continue
			}
			prevAddress = string(data)
		}
		if next != "" {
			data, _, _, err := zkConn.Get(node.chainPath+"/"+next, false)
			if err != nil {
				go func() { <-eventCh }()
				continue
			}
			nextAddress = string(data)
		}
		data, _, _, err := zkConn.Get(node.chainPath+"/"+tail, false)
		if err != nil {
			go func() { <-eventCh }()
			continue
		}
		tailAddress = string(data)

		// 4.创建客户端
		var prevClient, nextClient, tailClient rpc.Client
		if prev != "" {
			prevClient, err = node.clients.MakeClient(prevAddress)
			if err != nil {
				go func() { <-eventCh }()
				continue
			}
		}
		if next != "" {
			nextClient, err = node.clients.MakeClient(nextAddress)
			if err != nil {
				go func() { <-eventCh }()
				continue
			}
		}
		if tail != name {
			tailClient, err = node.clients.MakeClient(tailAddress)
			if err != nil {
				go func() { <-eventCh }()
				continue
			}
		}

		// 5.更新node.neighbours
		node.mu.Lock()
		node.neighbours[PosPrev] = Neighbour{name: prev, address: prevAddress, client: prevClient}
		node.neighbours[PosNext] = Neighbour{name: next, address: nextAddress, client: nextClient}
		node.neighbours[PosTail] = Neighbour{name: tail, address: tailAddress, client: tailClient}
		node.mu.Unlock()

		// 尾节点需要启动在本地提交日志的线程
		if next == "" {
			go node.kickOffCommit()
		}

		// 6.等待node.chainPath文件夹的变化
		<-eventCh
	}
}

// 不断将新日志复制给后继节点
func (node *Node) kickOffCopy() {
	// 缓存的后继节点
	cachedNext := ""
	// 缓存的已经发送给cachedNext的最新日志的索引
	cachedLastCopied := -1

	for {
		if node.killed() {
			break
		}

		// 避免单线程长时间占用CPU
		time.Sleep(10 * time.Millisecond)

		node.mu.Lock()
		next := node.neighbours[PosNext]

		// 尾节点没有后继节点,无需复制日志
		if next.name == "" {
			node.mu.Unlock()
			continue
		}

		// 如果后继节点变化,更新cachedNext和cachedLastCopied
		if next.name != cachedNext {
			args := LastIndexArgs{NodeName: node.name}
			var reply LastIndexReply
			node.mu.Unlock()

			// 节点不匹配,重置cachedNext和cachedLastCopied
			if !next.client.Call("Node.QueryLastIndex", &args, &reply) || reply.Mismatch {
				cachedNext = ""
				cachedLastCopied = -1
				continue
			}
			// 更新cachedNext和cachedLastCopied
			cachedNext = next.name
			cachedLastCopied = reply.LastIndex
			continue
		}

		// 后继节点已经与当前节点同步,无需发送日志
		if len(node.Logs) <= cachedLastCopied+1 {
			node.mu.Unlock()
			continue
		}

		// 1.将索引在cachedLastCopied+1及之后的日志发送给后继节点
		args := SendLogsArgs{
			NodeName:  node.name,
			NextIndex: cachedLastCopied + 1,
			Logs:      node.Logs[cachedLastCopied+1:],
		}
		var reply SendLogsReply
		node.mu.Unlock()

		if !next.client.Call("Node.SendLogs", &args, &reply) || reply.Mismatch {
			cachedNext = ""
			cachedLastCopied = -1
			continue
		}

		// 2.更新cachedNext和cachedLastCopied
		cachedNext = next.name
		cachedLastCopied = reply.LastIndex
	}
}

type LastIndexArgs struct {
	NodeName string
}

type LastIndexReply struct {
	Mismatch  bool
	LastIndex int
}

// 查询当前节点最新的日志索引
func (node *Node) QueryLastIndex(args *LastIndexArgs, reply *LastIndexReply) {
	node.mu.Lock()
	defer node.mu.Unlock()

	// 检查发送rpc的节点是否是前驱节点
	// 如果是前驱节点,将reply.LastIndex设为当前节点最后一个日志的索引,即len(node.Logs)-1
	// 如果不是前驱节点,将reply.Mismatch设为true
	prev := node.neighbours[PosPrev]
	if prev.name != args.NodeName {
		reply.Mismatch = true
		return
	}

	reply.LastIndex = len(node.Logs) - 1
}

type SendLogsArgs struct {
	NodeName  string
	NextIndex int
	Logs      []Log
}

type SendLogsReply struct {
	Mismatch  bool
	LastIndex int
}

// 处理前驱节点发送的日志
func (node *Node) SendLogs(args *SendLogsArgs, reply *SendLogsReply) {
	node.mu.Lock()
	defer node.mu.Unlock()

	// 检查发送rpc的节点是否是前驱节点
	// 如果是前驱节点,并且当前节点的日志中包含索引等于NextIndex-1的日志,则将args.Logs中当前节点没有的日志添加到node.Logs的末尾
	// 如果不是前驱节点,将reply.Mismatch设为true
	prev := node.neighbours[PosPrev]
	if prev.name != args.NodeName {
		reply.Mismatch = true
		return
	}

	// 前驱节点发送的日志与当前节点的日志不匹配
	if args.NextIndex+len(args.Logs) <= len(node.Logs) || len(node.Logs) < args.NextIndex {
		reply.LastIndex = len(node.Logs) - 1
		return
	}

	// 复制日志
	node.Logs = append(node.Logs, args.Logs[len(node.Logs)-args.NextIndex:]...)

	reply.LastIndex = len(node.Logs) - 1
}

// 直接在本地提交日志,只有尾节点才能调用,且同时只能有一个节点调用该函数
func (node *Node) kickOffCommit() {
	// 1.尝试获取提交锁,确保只有一个节点能够在本地提交并更新zookeeper中存储的提交索引
	for {
		if node.killed() {
			return
		}

		time.Sleep(10 * time.Millisecond)

		node.mu.Lock()
		if node.neighbours[PosNext].name != "" {
			node.mu.Unlock()
			return
		}
		node.mu.Unlock()

		if _, err := node.zkConn.Create(node.commitPath+"Lock", nil, zk.FlagEphemeral); err == nil {
			break
		}

		ok, _, eventCh, err := node.zkConn.Exists(node.commitPath+"Lock", true)
		if err != nil {
			continue
		}
		if !ok {
			go func() { <-eventCh }()
			continue
		}
		<-eventCh
	}

	// 2.不断在本地提交日志,并将提交索引更新到zookeeper中
	for {
		if node.killed() {
			break
		}

		// 避免单线程长时间占用CPU
		time.Sleep(10 * time.Millisecond)

		// 当前节点不再是尾节点
		node.mu.Lock()
		next := node.neighbours[PosNext]
		if next.name != "" {
			node.mu.Unlock()
			break
		}

		// 没有可提交的日志
		if len(node.Logs)-1 <= node.CommitIndex {
			node.mu.Unlock()
			continue
		}

		commitIndex := len(node.Logs) - 1
		node.mu.Unlock()

		// 2.1从zookeeper读取node.commitPath文件的内容
		data, version, _, err := node.zkConn.Get(node.commitPath, false)
		if err != nil {
			continue
		}
		// 2.2将commitPath文件存储的数据(即提交索引)转化为数字
		storedCommitIndex, err := strconv.Atoi(string(data))
		if err != nil {
			log.Fatalln(err)
		}

		// 2.3如果节点最新的日志的索引大于commitPath,则将该索引存储到commitPath文件中
		if storedCommitIndex < commitIndex {
			if _, err := node.zkConn.Set(node.commitPath, []byte(strconv.Itoa(commitIndex)), version); err != nil {
				continue
			}
		}

		// 2.4如果最新的日志的索引大于node.CommitIndex,则将node.CommitIndex更新为该索引
		node.mu.Lock()
		if commitIndex > node.CommitIndex {
			node.CommitIndex = commitIndex
			node.persist()
		}
		node.mu.Unlock()
	}

	// 3.释放锁
	node.zkConn.Delete(node.commitPath+"Lock", 0)
}

// 不断向前驱节点确认最新提交的日志
func (node *Node) kickOffAck() {
	// 缓存的前驱节点
	cachedPrev := ""
	// 缓存的cachedPrev确认的最新日志的索引
	cachedLastAck := -1

	for {
		if node.killed() {
			break
		}

		// 避免单线程长时间占用CPU
		time.Sleep(10 * time.Millisecond)

		node.mu.Lock()
		prev := node.neighbours[PosPrev]

		// 头节点没有前驱节点,无需向前确认
		if prev.name == "" {
			node.mu.Unlock()
			continue
		}

		// 如果前驱节点变化,更新cachedPrev和cachedLastAck
		if prev.name != cachedPrev {
			cachedPrev = prev.name
			cachedLastAck = -1
		}

		// 前驱节点已经与当前节点同步,无需发送确认
		if node.CommitIndex <= cachedLastAck {
			node.mu.Unlock()
			continue
		}

		// 1.发送确认消息给前驱节点
		args := AckArgs{
			NodeName:    node.name,
			CommitIndex: node.CommitIndex,
		}
		var reply AckReply
		node.mu.Unlock()

		if !prev.client.Call("Node.Ack", &args, &reply) || reply.Mismatch {
			cachedPrev = ""
			cachedLastAck = -1
			continue
		}

		// 2.更新cachedNext和cachedLastCopied
		cachedPrev = prev.name
		cachedLastAck = reply.CommitIndex
	}
}

type AckArgs struct {
	NodeName    string
	CommitIndex int
}

type AckReply struct {
	Mismatch    bool
	CommitIndex int
}

func (node *Node) Ack(args *AckArgs, reply *AckReply) {
	node.mu.Lock()
	defer node.mu.Unlock()

	// 检查发送rpc的节点是否是前驱节点
	// 如果是前驱节点,并且当前节点包含待提交的日志,即满足args.CommitIndex在(node.CommitIndex, len(node.Logs))范围内
	// 则将node.CommitIndex更新为args.CommitIndex
	// 如果不是前驱节点,将reply.Mismatch设为true
	next := node.neighbours[PosNext]
	if next.name != args.NodeName {
		reply.Mismatch = true
		return
	}

	// 更新CommitIndex
	if node.CommitIndex < args.CommitIndex && args.CommitIndex < len(node.Logs) {
		node.CommitIndex = args.CommitIndex
		node.persist()
	}

	reply.CommitIndex = node.CommitIndex
}

func (node *Node) kickOffApply() {
	for {
		if node.killed() {
			break
		}

		// 避免单线程长时间占用CPU
		time.Sleep(10 * time.Millisecond)

		node.mu.Lock()
		// 没有可以发送到状态机的日志
		if node.CommitIndex <= node.lastApplied {
			node.mu.Unlock()
			continue
		}

		// 1.自增node.lastApplied
		node.lastApplied++
		applyMsg := ApplyMsg{
			Command:      node.Logs[node.lastApplied].Command,
			CommandIndex: node.lastApplied,
		}
		node.mu.Unlock()

		// 2.将日志的Command封装在ApplyMsg中发送到node.applyCh中
		node.applyCh <- applyMsg
	}
}

// 返回节点名称,注意加锁
func (node *Node) GetName() string {
	node.mu.Lock()
	defer node.mu.Unlock()

	return node.name
}

// 返回邻居节点名,按照前驱节点,后继节点,尾节点的顺序放到数组中返回
func (node *Node) GetNeighbours() [3]string {
	node.mu.Lock()
	defer node.mu.Unlock()

	neighbours := [3]string{
		node.neighbours[PosPrev].name,
		node.neighbours[PosNext].name,
		node.neighbours[PosTail].name,
	}
	return neighbours
}

// 如果当前节点是链表的头节点,将Command添加到日志列表中,并返回
// 该方法是异步的,即Start返回时,不能保证日志已经复制到链表中的
// 每一个节点中,日志的复制在Node.kickOffAgreement线程中进行
func (node *Node) Start(command interface{}) (int, bool) {
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.neighbours[PosPrev].name != "" {
		return -1, false
	}

	node.Logs = append(node.Logs, Log{Command: command})

	return len(node.Logs) - 1, true
}

// Kill当前节点,当连接断开时调用,用于清理资源
func (node *Node) Kill() {
	atomic.StoreInt32(&node.dead, 1)

	node.mu.Lock()
	// 关闭连接
	if !node.zkConn.Closed() {
		node.mu.Unlock()

		node.zkConn.Close()

		node.mu.Lock()
	}
	// 清空邻居节点
	neighbours := node.neighbours
	node.neighbours = make(map[NeighbourPos]Neighbour, 3)
	node.mu.Unlock()

	// 关闭客户端连接
	for _, neighbour := range neighbours {
		if neighbour.client != nil {
			neighbour.client.Close()
		}
	}
}

// 检查当前节点是否已被Kill,所有无限循环的函数每隔一段时间都应当检查该条件,如Node.watchChain
func (node *Node) killed() bool {
	return atomic.LoadInt32(&node.dead) == 1
}
