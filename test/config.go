package test

import (
	"crfs/chunkserver"
	"crfs/client"
	"crfs/common"
	"crfs/master"
	"crfs/persister"
	"crfs/rpc"
	izk "crfs/zk"
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	masterAddress  = "master"
	zooAddress     = "zoo"
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

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

type config struct {
	mu           sync.Mutex
	t            *testing.T
	net          *rpc.Network
	master       *master.Master
	zkServer     *izk.FakeZKServer
	chunkServers []*chunkserver.ChunkServer
	clients      map[*client.Client]string
	ips          []string
	saved        []persister.Persister

	t0    time.Time
	rpcs0 int
	ops   int32
}

func makeConfig(t *testing.T, n int, unreliable bool) *config {
	runtime.GOMAXPROCS(4)

	cfg := &config{}

	cfg.t = t

	net := rpc.MakeNetwork()
	cfg.net = net

	done := make(chan bool)
	master := master.MakeMaster("", 24*time.Hour, 3*24*time.Hour, done)
	go func() {
		<-done
	}()
	cfg.master = master
	svc := rpc.MakeService(master)
	srv := rpc.MakeServer()
	srv.AddService(svc)
	net.AddServer(masterAddress, srv)

	zkServer := izk.MakeFakeZKServer()
	cfg.zkServer = zkServer
	svc = rpc.MakeService(zkServer)
	srv = rpc.MakeServer()
	srv.AddService(svc)
	net.AddServer(zooAddress, srv)

	// 初始化节点地址
	cfg.ips = make([]string, n)
	ip := [4]byte{192, 168, 0, 1}
	for i := 0; i < n; i++ {
		cfg.ips[i] = IPToString(ip)
		ip = IPAddOne(ip)
	}

	// 初始化端点
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				cfg.net.MakeEnd(fmt.Sprintf("%s-%s:%d", cfg.ips[i], cfg.ips[j], common.NodePort))
				cfg.net.Connect(fmt.Sprintf("%s-%s:%d", cfg.ips[i], cfg.ips[j], common.NodePort), cfg.ips[j])
			}
		}
		cfg.net.MakeEnd(cfg.ips[i] + "-" + masterAddress)
		cfg.net.Connect(cfg.ips[i]+"-"+masterAddress, masterAddress)
		cfg.net.MakeEnd(cfg.ips[i] + "-" + zooAddress)
		cfg.net.Connect(cfg.ips[i]+"-"+zooAddress, zooAddress)
	}

	cfg.chunkServers = make([]*chunkserver.ChunkServer, n)
	cfg.clients = make(map[*client.Client]string)
	cfg.saved = make([]persister.Persister, n)

	for i := 0; i < n; i++ {
		cfg.startOne(i)
	}

	cfg.net.Reliable(!unreliable)

	return cfg
}

func (cfg *config) startOne(i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	// 连接其他服务器
	for j := 0; j < len(cfg.chunkServers); j++ {
		if i != j {
			cfg.net.Enable(fmt.Sprintf("%s-%s:%d", cfg.ips[i], cfg.ips[j], common.NodePort), true)
			cfg.net.Enable(fmt.Sprintf("%s-%s:%d", cfg.ips[j], cfg.ips[i], common.NodePort), true)
		}
	}

	// 连接zookeeper
	cfg.net.Enable(cfg.ips[i]+"-"+zooAddress, true)
	zkClient, err := makeZKClient(cfg.net, cfg.ips[i])
	if err != nil {
		cfg.t.Fatal(err)
	}

	clients := rpc.MakeFakeClients(cfg.net, cfg.ips[i])

	// 连接master
	cfg.net.Enable(cfg.ips[i]+"-"+masterAddress, true)
	masterClient, err := clients.MakeClient(masterAddress)
	if err != nil {
		cfg.t.Fatal(err)
	}

	// 确保同一个Persister只有一个节点在使用
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	} else {
		cfg.saved[i] = persister.MakeMemoryPersister()
	}

	var allocateServerReply master.AllocateServerReply
	for {
		args := master.AllocateServerArgs{
			ChunkServer: cfg.ips[i],
		}
		var reply master.AllocateServerReply

		if masterClient.Call("Master.AllocateServer", &args, &reply) {
			allocateServerReply = reply
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	chainPath := common.ZKChainNode + "/" + allocateServerReply.ChainName
	commitPath := common.ZKCommitNode + "/" + allocateServerReply.ChainName

	done := make(chan bool)
	cs := chunkserver.MakeChunkServer(masterClient, zkClient, clients, cfg.ips[i], chainPath, commitPath, 24*time.Hour, cfg.saved[i], done)
	go func() {
		<-done
	}()
	cfg.chunkServers[i] = cs

	// 创建服务
	nodeSvc := rpc.MakeService(cs.Node)
	chunkServerSvc := rpc.MakeService(cs)
	srv := rpc.MakeServer()
	srv.AddService(nodeSvc)
	srv.AddService(chunkServerSvc)
	cfg.net.AddServer(cfg.ips[i], srv)
}

func (cfg *config) crashOne(i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	for j := 0; j < len(cfg.chunkServers); j++ {
		if i != j {
			cfg.net.Enable(fmt.Sprintf("%s-%s:%d", cfg.ips[i], cfg.ips[j], common.NodePort), false)
			cfg.net.Enable(fmt.Sprintf("%s-%s:%d", cfg.ips[j], cfg.ips[i], common.NodePort), false)
		}
	}
	cfg.net.DeleteServer(cfg.ips[i])

	cfg.net.Enable(cfg.ips[i]+"-"+zooAddress, false)
	cfg.net.Enable(cfg.ips[i]+"-"+masterAddress, false)

	// 避免crash的节点继续更新Persister的值
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	}

	if cfg.chunkServers[i] != nil {
		cfg.chunkServers[i].Kill()
		cfg.chunkServers[i] = nil
	}
}

func makeZKClient(net *rpc.Network, address string) (izk.ZKClient, error) {
	clients := rpc.MakeFakeClients(net, address)
	client, err := clients.MakeClient(zooAddress)
	if err != nil {
		return nil, err
	}

	return izk.MakeFakeZKClient(client, []string{zooAddress}, sessionTimeout), nil
}

func (cfg *config) makeClient() *client.Client {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	clientAddress := randstring(16)
	rpcClients := rpc.MakeFakeClients(cfg.net, clientAddress)
	for j := 0; j < len(cfg.chunkServers); j++ {
		cfg.net.MakeEnd(fmt.Sprintf("%s-%s:%d", clientAddress, cfg.ips[j], common.ChunkPort))
		cfg.net.Connect(fmt.Sprintf("%s-%s:%d", clientAddress, cfg.ips[j], common.ChunkPort), cfg.ips[j])
		cfg.net.Enable(fmt.Sprintf("%s-%s:%d", clientAddress, cfg.ips[j], common.ChunkPort), true)
	}
	cfg.net.MakeEnd(clientAddress + "-" + masterAddress)
	cfg.net.Connect(clientAddress+"-"+masterAddress, masterAddress)
	cfg.net.Enable(clientAddress+"-"+masterAddress, true)

	masterClient, err := rpcClients.MakeClient(masterAddress)
	if err != nil {
		cfg.t.Fatal(err)
	}

	cl := client.MakeClient(masterClient, rpcClients)
	cfg.clients[cl] = clientAddress

	return cl
}

func (cfg *config) deleteClient(cl *client.Client) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	delete(cfg.clients, cl)
	cl.Close()
}

func (cfg *config) begin(description string) {
	fmt.Printf("%s ...\n", description)
	cfg.t0 = time.Now()
	cfg.rpcs0 = cfg.net.GetTotalCount()
	atomic.StoreInt32(&cfg.ops, 0)
}

func (cfg *config) op() {
	atomic.AddInt32(&cfg.ops, 1)
}

func (cfg *config) end() {
	t := time.Since(cfg.t0).Seconds()
	npeers := len(cfg.chunkServers)
	nrpc := cfg.net.GetTotalCount() - cfg.rpcs0
	ops := atomic.LoadInt32(&cfg.ops)

	fmt.Printf("  ... Passed --")
	fmt.Printf("  %4.1f  %d %5d %4d\n", t, npeers, nrpc, ops)
}

func (cfg *config) cleanup() {
	for _, s := range cfg.chunkServers {
		if s != nil {
			s.Kill()
		}
	}
	cfg.zkServer.Kill()
	cfg.net.Cleanup()
}
