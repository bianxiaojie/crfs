package node

import (
	"crfs/persister"
	"crfs/rpc"
	izk "crfs/zk"
	"fmt"
	"log"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
)

const (
	zoo            = "zoo"
	sessionTimeout = time.Second
)

func IPAddOne(ip [4]byte) [4]byte {
	ipUint32 := uint32(ip[0])<<24 | uint32(ip[1])<<16 | uint32(ip[2])<<8 | uint32(ip[3])
	ipUint32++

	var newIp [4]byte
	newIp[0] = byte(ipUint32 >> 24)
	newIp[1] = byte(ipUint32 >> 16)
	newIp[2] = byte(ipUint32 >> 8)
	newIp[3] = byte(ipUint32)

	return newIp
}

func IPToString(ip [4]byte) string {
	return fmt.Sprintf("%d.%d.%d.%d", ip[0], ip[1], ip[2], ip[3])
}

type config struct {
	mu        sync.Mutex
	t         *testing.T
	net       *rpc.Network
	zkServer  *izk.FakeZKServer
	addresses []string
	connected []bool // 是否与zookeeper正在连接
	logs      []map[int]interface{}
	nodes     []*Node
	saved     []persister.Persister

	t0        time.Time // 测试代码调用cfg.begin()的时间点
	rpcs0     int       // 测试开始时rpcTotal()的值
	bytes0    int64
	maxIndex  int
	maxIndex0 int
}

func makeConfig(t *testing.T, n int, unreliable bool) *config {
	runtime.GOMAXPROCS(4)

	cfg := &config{}

	cfg.t = t

	cfg.maxIndex = -1

	net := rpc.MakeNetwork()
	cfg.net = net

	zkServer := izk.MakeFakeZKServer()
	cfg.zkServer = zkServer
	svc := rpc.MakeService(zkServer)
	srv := rpc.MakeServer()
	srv.AddService(svc)
	net.AddServer(zoo, srv)

	clientAddress := "config"
	cfg.net.MakeEnd(clientAddress + "-" + zoo)
	cfg.net.Connect(clientAddress+"-"+zoo, zoo)
	cfg.net.Enable(clientAddress+"-"+zoo, true)
	zkClient, err := makeZKClient(net, clientAddress)
	if err != nil {
		t.Fatal(err)
	}
	zkConn, err := makeZKConn(zkClient)
	if err != nil {
		t.Fatal(err)
	}
	zkConn.Create("/chain", nil, 0)
	zkConn.Create("/commit", nil, 0)
	zkConn.Create("/commit/chain", []byte(strconv.Itoa(-1)), 0)
	zkConn.Close()

	// 初始化节点地址
	cfg.addresses = make([]string, n)
	ip := [4]byte{192, 168, 0, 1}
	for i := 0; i < n; i++ {
		cfg.addresses[i] = IPToString(ip)
		ip = IPAddOne(ip)
	}

	// 初始化端点
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				cfg.net.MakeEnd(cfg.addresses[i] + "-" + cfg.addresses[j])
				cfg.net.Connect(cfg.addresses[i]+"-"+cfg.addresses[j], cfg.addresses[j])
			}
		}
		cfg.net.MakeEnd(cfg.addresses[i] + "-" + zoo)
		cfg.net.Connect(cfg.addresses[i]+"-"+zoo, zoo)
	}

	cfg.connected = make([]bool, n)
	cfg.nodes = make([]*Node, n)
	cfg.saved = make([]persister.Persister, n)
	cfg.logs = make([]map[int]interface{}, n)
	for i := range cfg.logs {
		cfg.logs[i] = make(map[int]interface{})
	}

	for i := 0; i < n; i++ {
		cfg.startOne(i)
	}

	cfg.net.Reliable(!unreliable)

	return cfg
}

func (cfg *config) startOne(i int) {
	cfg.crashOne(i)

	nodeAddress := cfg.addresses[i]
	// 连接其他节点
	for j := 0; j < len(cfg.nodes); j++ {
		if i != j {
			cfg.enable(i, j, true)
			cfg.enable(j, i, true)
		}
	}

	// 连接zookeeper
	cfg.enableZookeeper(i, true)
	zkClient, err := makeZKClient(cfg.net, nodeAddress)
	if err != nil {
		cfg.t.Fatal(err)
	}
	zkConn, err := makeZKConn(zkClient)
	if err != nil {
		cfg.t.Fatal(err)
	}
	clients := rpc.MakeFakeClients(cfg.net, nodeAddress)

	// 确保同一个Persister只有一个节点在使用
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	} else {
		cfg.saved[i] = persister.MakeMemoryPersister()
	}

	applyCh := make(chan ApplyMsg)
	go func() {
		for m := range applyCh {
			err_msg := ""
			v := m.Command
			cfg.mu.Lock()
			for j := 0; j < len(cfg.logs); j++ {
				if old, oldok := cfg.logs[j][m.CommandIndex]; oldok && old != v {
					// 其他节点在同一索引提交了不同的日志
					err_msg = fmt.Sprintf("服务器%v在索引%vapply的日志%v和服务器服务器%vapply的日志%v不同",
						i, m.CommandIndex, m.Command, j, old)
				}
			}
			_, prevok := cfg.logs[i][m.CommandIndex-1]
			cfg.logs[i][m.CommandIndex] = v
			if m.CommandIndex > cfg.maxIndex {
				cfg.maxIndex = m.CommandIndex
			}
			cfg.mu.Unlock()

			if m.CommandIndex > 1 && !prevok {
				err_msg = fmt.Sprintf("服务器%vapply%v跳过了部分日志", i, m.CommandIndex)
			}

			if err_msg != "" {
				log.Fatalf("apply错误: %v\n", err_msg)
			}
		}
	}()
	node := MakeNode(zkClient, zkConn, clients, "/chain", "", nodeAddress, "/commit/chain", cfg.saved[i], applyCh)

	cfg.nodes[i] = node

	// 创建服务
	svc := rpc.MakeService(node)
	srv := rpc.MakeServer()
	srv.AddService(svc)
	cfg.net.AddServer(nodeAddress, srv)
}

func (cfg *config) crashOne(i int) {
	for j := 0; j < len(cfg.nodes); j++ {
		if i != j {
			cfg.enable(i, j, false)
			cfg.enable(j, i, false)
		}
	}
	cfg.net.DeleteServer(cfg.addresses[i])

	cfg.enableZookeeper(i, false)

	// 避免crash的节点继续更新Persister的值
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	}

	if cfg.nodes[i] != nil {
		cfg.nodes[i].Kill()
		cfg.nodes[i] = nil
	}
}

func (cfg *config) enable(i, j int, enabled bool) {
	cfg.net.Enable(cfg.addresses[i]+"-"+cfg.addresses[j], enabled)
}

func (cfg *config) enableZookeeper(i int, enabled bool) {
	cfg.net.Enable(cfg.addresses[i]+"-"+zoo, enabled)
	cfg.connected[i] = enabled
}

func makeZKClient(net *rpc.Network, address string) (izk.ZKClient, error) {
	clients := rpc.MakeFakeClients(net, address)
	client, err := clients.MakeClient(zoo)
	if err != nil {
		return nil, err
	}

	return izk.MakeFakeZKClient(client, []string{zoo}, sessionTimeout), nil
}

func makeZKConn(zkClient izk.ZKClient) (izk.ZKConn, error) {
	start := time.Now()
	for time.Since(start) <= sessionTimeout {
		if conn, err := zkClient.Connect(); err == nil {
			return conn, nil
		}
		time.Sleep(10 * time.Millisecond)
	}

	return nil, zk.ErrNoServer
}

type nodeInfo struct {
	index int
	name  string
	prev  string
	next  string
}

func (cfg *config) checkChain(servers ...int) []int {
	re, err := regexp.Compile(`^\d+$`)
	if err != nil {
		cfg.t.Fatal(err)
	}

	nis := make([]nodeInfo, len(servers))
	for i, nodeIndex := range servers {
		node := cfg.nodes[nodeIndex]
		name := node.GetName()
		if !re.MatchString(name) {
			cfg.t.Fatalf("节点名格式错误,应当是数字字符串,但实际的值为: %s\n", name)
		}
		prev, next := node.GetNeighbours()
		nis[i] = nodeInfo{
			index: nodeIndex,
			name:  name,
			prev:  prev,
			next:  next,
		}
	}

	sort.SliceStable(nis, func(i, j int) bool {
		if len(nis[i].name) != len(nis[j].name) {
			return len(nis[i].name) < len(nis[j].name)
		}
		return nis[i].name < nis[j].name
	})

	indices := make([]int, len(servers))
	chain := make([]string, len(servers))
	for i, ni := range nis {
		indices[i] = ni.index
		chain[i] = ni.name
	}

	for i, ni := range nis {
		var prev, next string
		if i > 0 {
			prev = nis[i-1].name
		}
		if prev != ni.prev {
			cfg.t.Fatalf("链表为: %v, 节点%s的前驱节点应该为: %s, 但实际存储的前驱节点为: %s\n", chain, ni.name, prev, ni.prev)
		}
		if i < len(nis)-1 {
			next = nis[i+1].name
		}
		if next != ni.next {
			cfg.t.Fatalf("链表为: %v, 节点%s的后继节点应该为: %s, 但实际存储的后继节点为: %s\n", chain, ni.name, next, ni.next)
		}
	}

	return indices
}

func (cfg *config) checkCrash(servers ...int) {
	for _, nodeIndex := range servers {
		if cfg.nodes[nodeIndex] != nil && !cfg.nodes[nodeIndex].Killed() {
			cfg.t.Fatalf("节点%s应当已经被Killed, 但节点还是存活\n", cfg.nodes[nodeIndex].address)
		}
	}
}

func (cfg *config) nCommitted(index int, expectedServers ...int) (int, interface{}) {
	count := 0
	var cmd interface{} = nil
	for i := 0; i < len(expectedServers); i++ {
		cfg.mu.Lock()
		cmd1, ok := cfg.logs[expectedServers[i]][index]
		cfg.mu.Unlock()

		if ok {
			if count > 0 && cmd != cmd1 {
				cfg.t.Fatalf("节点在索引%v提交的日志不一致: %v, %v\n", index, cmd, cmd1)
			}
			count += 1
			cmd = cmd1
		}
	}
	return count, cmd
}

func (cfg *config) one(cmd interface{}, header int, retry bool, expectedServers ...int) int {
	t0 := time.Now()
	for time.Since(t0).Seconds() < 10 {
		index, ok := cfg.nodes[header].Start(cmd)
		if !ok {
			cfg.t.Fatalf("服务器%d不是header\n", header)
		}
		t1 := time.Now()
		for time.Since(t1).Seconds() < 2 {
			nd, cmd1 := cfg.nCommitted(index, expectedServers...)
			if nd == len(expectedServers) {
				if cmd1 == cmd {
					return index
				}
			}
			time.Sleep(30 * time.Millisecond)
		}
		if !retry {
			cfg.t.Fatalf("one(%v)没有达成一致\n", cmd)
		}
		time.Sleep(60 * time.Millisecond)
	}
	cfg.t.Fatalf("one(%v)没有达成一致\n", cmd)
	return -1
}

func (cfg *config) begin(description string) {
	fmt.Printf("%s ...\n", description)
	cfg.t0 = time.Now()
	cfg.rpcs0 = cfg.net.GetTotalCount()
	cfg.bytes0 = cfg.net.GetTotalBytes()
	cfg.maxIndex0 = cfg.maxIndex
}

func (cfg *config) end() {
	t := time.Since(cfg.t0).Seconds()              // 运行时间
	nrpc := cfg.net.GetTotalCount() - cfg.rpcs0    // rpc总数
	nbytes := cfg.net.GetTotalBytes() - cfg.bytes0 // rpc发送的字节总数
	ncmds := cfg.maxIndex - cfg.maxIndex0          // 提交的总日志数

	fmt.Printf("  ... Passed --")
	fmt.Printf("  %4.1f  %4d %7d %4d\n", t, nrpc, nbytes, ncmds)
}

func (cfg *config) cleanup() {
	for _, s := range cfg.nodes {
		if s != nil {
			s.Kill()
		}
	}
	cfg.zkServer.Kill()
	cfg.net.Cleanup()
}
