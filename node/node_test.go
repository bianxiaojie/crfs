package node

import (
	"crfs/rpc"
	izk "crfs/zk"
	"fmt"
	"regexp"
	"sort"
	"testing"
	"time"
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
	net      *rpc.Network
	zkServer *izk.FakeZKServer
	zkClient *izk.FakeZKClient
	nodes    []*Node
	t        *testing.T

	t0     time.Time // time at which test_test.go called cfg.begin()
	rpcs0  int       // rpcTotal() at start of test
	bytes0 int64
}

func makeConfig(n int, t *testing.T) *config {
	cfg := &config{}

	net := rpc.MakeNetwork()
	cfg.net = net

	zkServer := izk.MakeFakeZKServer()
	cfg.zkServer = zkServer

	svc := rpc.MakeService(zkServer)
	srv := rpc.MakeServer()
	srv.AddService(svc)
	net.AddServer(zoo, srv)

	configAddress := "config"
	configEndname := configAddress + "-" + zoo
	net.MakeEnd(configEndname)
	net.Connect(configEndname, zoo)
	net.Enable(configEndname, true)
	clients := rpc.MakeFakeClients(net, configAddress)
	client, err := clients.MakeClient(zoo)
	if err != nil {
		t.Fatal(err)
	}
	configFzkc := izk.MakeFakeZKClient(client, []string{zoo}, sessionTimeout)
	cfg.zkClient = configFzkc

	configConn, err := configFzkc.Connect()
	if err != nil {
		t.Fatal(err)
	}
	configConn.Create("/chain", nil, 0)
	configConn.Close()

	cfg.nodes = make([]*Node, n)
	ip := [4]byte{192, 168, 0, 1}
	for i := 0; i < n; i++ {
		nodeAddress := IPToString(ip)

		nodeEndname := nodeAddress + "-" + zoo
		net.MakeEnd(nodeEndname)
		net.Connect(nodeEndname, zoo)
		net.Enable(nodeEndname, true)
		clients := rpc.MakeFakeClients(net, nodeAddress)
		client, err := clients.MakeClient(zoo)
		if err != nil {
			t.Fatal(err)
		}
		nodeFzkc := izk.MakeFakeZKClient(client, []string{zoo}, sessionTimeout)
		node := makeNode(nodeFzkc, "/chain", "", nodeAddress)
		cfg.nodes[i] = node

		ip = IPAddOne(ip)
	}

	cfg.t = t

	return cfg
}

func (cfg *config) enable(i int, enabled bool) {
	endname := cfg.nodes[i].address + "-" + zoo
	cfg.net.Enable(endname, enabled)
}

type nodeInfo struct {
	name       string
	neighbours [3]string
}

func (cfg *config) checkChain(indices ...int) {
	re, err := regexp.Compile(`^\d+$`)
	if err != nil {
		cfg.t.Fatal(err)
	}

	nis := make([]nodeInfo, len(indices))
	for i, nodeIndex := range indices {
		node := cfg.nodes[nodeIndex]
		name := node.GetName()
		if !re.MatchString(name) {
			cfg.t.Fatalf("节点名格式错误,应当是数字字符串,但实际的值为: %s\n", name)
		}
		neighbours := node.GetNeighbours()
		nis[i] = nodeInfo{name: name, neighbours: neighbours}
	}

	sort.SliceStable(nis, func(i, j int) bool {
		return nis[i].name < nis[j].name
	})

	chain := make([]string, len(indices))
	for i, ni := range nis {
		chain[i] = ni.name
	}

	for i, ni := range nis {
		neighbours := ni.neighbours
		var prev, next, tail string
		if i > 0 {
			prev = nis[i-1].name
		}
		if prev != neighbours[0] {
			cfg.t.Fatalf("链表为: %v, 节点%s的前驱节点应该为: %s, 但实际存储的前驱节点为: %s\n", chain, ni.name, prev, neighbours[0])
		}
		if i < len(nis)-1 {
			next = nis[i+1].name
		}
		if next != neighbours[1] {
			cfg.t.Fatalf("链表为: %v, 节点%s的后继节点应该为: %s, 但实际存储的后继节点为: %s\n", chain, ni.name, next, neighbours[1])
		}
		tail = nis[len(nis)-1].name
		if tail != neighbours[2] {
			cfg.t.Fatalf("链表为: %v, 尾节点应该为: %s, 但实际存储的尾节点为: %s\n", chain, tail, neighbours[2])
		}
	}
}

func (cfg *config) begin(description string) {
	fmt.Printf("%s ...\n", description)
	cfg.t0 = time.Now()
	cfg.rpcs0 = cfg.net.GetTotalCount()
	cfg.bytes0 = cfg.net.GetTotalBytes()
}

func (cfg *config) end() {
	t := time.Since(cfg.t0).Seconds()              // real time
	nrpc := cfg.net.GetTotalCount() - cfg.rpcs0    // number of RPC sends
	nbytes := cfg.net.GetTotalBytes() - cfg.bytes0 // number of bytes

	fmt.Printf("  ... Passed --")
	fmt.Printf("  %4.1f  %4d %7d\n", t, nrpc, nbytes)
}

func (cfg *config) cleanup() {
	for _, s := range cfg.nodes {
		s.Kill()
	}
	cfg.zkServer.Kill()
	cfg.net.Cleanup()
}

func TestChainBasic(t *testing.T) {
	nnodes := 3
	cfg := makeConfig(nnodes, t)
	defer cfg.cleanup()

	cfg.begin("Test (Chain): basic chain test")
	time.Sleep(sessionTimeout)

	cfg.checkChain(0, 1, 2)

	cfg.end()
}

func TestChainVaried(t *testing.T) {
	nnodes := 3
	cfg := makeConfig(nnodes, t)
	defer cfg.cleanup()

	cfg.begin("Test (Chain): varied chain test ")
	time.Sleep(sessionTimeout)
	cfg.checkChain(0, 1, 2)

	cfg.enable(0, false)
	time.Sleep(2 * sessionTimeout)
	cfg.checkChain(1, 2)

	cfg.enable(1, false)
	time.Sleep(2 * sessionTimeout)
	cfg.checkChain(2)

	cfg.enable(0, true)
	cfg.enable(1, true)
	time.Sleep(sessionTimeout)
	cfg.checkChain(0, 1, 2)

	cfg.end()
}
