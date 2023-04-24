package node

import (
	izk "crfs/zk" // 封装了zookeeper连接和客户端的接口
	"sort"
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
	name    string // 邻居节点名
	address string // 邻居节点地址
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

	// 常量
	zkClient  izk.ZKClient // zookeeper客户端,用于创建zkConn
	chainPath string       // 链表在zookeeper中的路径,形如/chain/chain1
	prefix    string       // 节点名的前缀
	address   string       // 节点地址

	// 原子变量
	// 节点是否死亡
	dead int32

	// 互斥变量
	// name: 节点在zookeeper相应的链表路径(/chain/chain1)下创建的文件的名称,由两部分构成${prefix}${index}
	// 其中prefix是一个固定字符串,可以自由选择,但必须是常量;index是一个递增的正整数,由zookeeper在创建文件时自动生成
	// 当节点与zookeeper连接成功时,应当在链表路径(/chain/chain1)下创建一个文件,表示节点加入链表中,并将name更新为创建的文件名
	// 当节点与zookeeper断开连接时,zookeeper将会自动删除该文件,节点同时需要将name更新为nil,表示节点不在链表中
	// 如何创建临时文件(自动删除)和序列文件(文件名追加递增的正整数的)可见ZKConn.Get
	name string

	// zkConn: 与zookeeper服务集群建立的连接
	// 通过zkClient创建,初始值为nil,表示没有连接
	// 每当zkConn与zookeeper服务集群断开连接时,都应重新创建新的连接,并覆盖该变量:
	// 1 zkConn, err := node.zkClient.Connect(servers, sessionTimeout)
	// 2 if err != nil {
	// 3     processErr(err)
	// 4 } else {
	// 5     node.mu.Lock()
	// 6     node.zkConn = zkConn
	// 7     node.mu.Unlock()
	// 8 }
	// 注意,zkConn是互斥变量,因此访问和修改时应当加互斥锁
	zkConn izk.ZKConn

	// 邻居节点,示例:
	// prev := neighbours[PosPrev]
	neighbours map[NeighbourPos]Neighbour
}

func makeNode(zkClient izk.ZKClient, chainPath string, prefix string, address string) *Node {
	node := &Node{}
	node.zkClient = zkClient
	node.chainPath = chainPath
	node.prefix = prefix
	node.address = address
	node.neighbours = make(map[NeighbourPos]Neighbour, 3)

	go node.watchChain()

	return node
}

// 尝试与zookeeper建立连接,支持多线程调用
// 但为了简化代码,建议只在单线程,即在watchChain中调用该函数
func (node *Node) tryConnect() {
	// 如果连接存在,则不会重新建立新的连接
	node.mu.Lock()
	if node.zkConn != nil && !node.zkConn.Closed() {
		node.mu.Unlock()
		return
	}
	node.mu.Unlock()

	// 建立连接
	zkConn, err := node.zkClient.Connect()
	if err != nil {
		return
	}

	// 如果在建立连接的过程中,有其他线程成功建立连接,则关闭当前线程建立的连接,并返回
	node.mu.Lock()
	if node.zkConn != nil && !node.zkConn.Closed() {
		node.mu.Unlock()
		zkConn.Close()
		return
	}
	node.zkConn = zkConn
	node.mu.Unlock()
}

// 监控链表,每当链表发生变化时,重新从zookeeper获取最新的链表信息
func (node *Node) watchChain() {
	for {
		// 1.尝试建立连接
		// 如果服务器被Kill,应当停止对链表的监控
		if node.killed() {
			return
		}

		// 避免单线程长时间占用CPU
		time.Sleep(10 * time.Millisecond)

		// 如果连接不存在或已关闭,则尝试连接,直到连接存在
		node.mu.Lock()
		zkConn := node.zkConn
		if zkConn != nil && zkConn.Closed() {
			node.zkConn = nil
			node.name = ""
			node.neighbours = make(map[NeighbourPos]Neighbour, 3)
		}
		node.mu.Unlock()
		if zkConn == nil || zkConn.Closed() {
			node.tryConnect()
			continue
		}

		// 2.如果name == "",建立临时文件,存储自己的address
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

		// 3.读取node.chainPath文件夹,通过分析自己在链表中的位置,获取前驱节点,后继节点和尾节点
		// 读取文件夹
		chain, _, eventCh, err := zkConn.Children(node.chainPath, true)
		if err != nil {
			continue
		}

		// 获取邻居节点
		sort.Strings(chain)
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

		// 4.分别读取前驱节点,后继节点和尾节点的address
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

		// 5.更新node.neighbours
		node.mu.Lock()
		node.neighbours[PosPrev] = Neighbour{name: prev, address: prevAddress}
		node.neighbours[PosNext] = Neighbour{name: next, address: nextAddress}
		node.neighbours[PosTail] = Neighbour{name: tail, address: tailAddress}
		node.mu.Unlock()

		// 6.等待node.chainPath文件夹的变化
		<-eventCh
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

// Kill当前节点,会在测试代码中调用
func (node *Node) Kill() {
	atomic.StoreInt32(&node.dead, 1)

	node.mu.Lock()
	defer node.mu.Unlock()

	// 关闭连接
	if node.zkConn != nil {
		node.zkConn.Close()
	}
	node.zkConn = nil
	// 清空邻居节点
	node.neighbours = make(map[NeighbourPos]Neighbour, 3)
}

// 检查当前节点是否已被Kill,所有无限循环的函数每隔一段时间都应当检查该条件,如Node.watchChain
func (node *Node) killed() bool {
	return atomic.LoadInt32(&node.dead) == 1
}
