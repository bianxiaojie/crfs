package chunkserver

import (
	"crfs/persister"
	"crfs/rpc"
	izk "crfs/zk"
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"math/big"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
)

const (
	zoo            = "zoo"
	sessionTimeout = time.Second
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 60)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

type clerk struct {
	servers   []*rpc.ClientEnd
	id        int64
	requestid int64
}

func makeClerk(servers []*rpc.ClientEnd) *clerk {
	c := &clerk{}
	c.servers = servers
	c.id = nrand()
	return c
}

func (c *clerk) write(chunkName string, offset int, data []byte) persister.Err {
	c.requestid++

	var err persister.Err
	i := 0
	for {
		args := WriteArgs{
			ChunkName: chunkName,
			Offset:    offset,
			Data:      data,
			ClerkId:   c.id,
			RequestId: c.requestid,
		}
		var reply WriteReply

		if c.servers[i].Call("ChunkServer.Write", &args, &reply) && (reply.Err == persister.Success || reply.Err == persister.OutOfChunk) {
			DPrintf("[client %d] finishes write: %v, requestId: %d\n", c.id, reply.Err, c.requestid)

			err = reply.Err
			break
		}

		DPrintf("[client %d] fails to write: %v, requestId: %d\n", c.id, reply.Err, c.requestid)

		i = (i + 1) % len(c.servers)
		if i == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	return err
}

func (c *clerk) append(chunkName string, data []byte) (int, persister.Err) {
	c.requestid++

	var offset int
	var err persister.Err
	i := 0
	for {
		args := AppendArgs{
			ChunkName: chunkName,
			Data:      data,
			ClerkId:   c.id,
			RequestId: c.requestid,
		}
		var reply AppendReply

		if c.servers[i].Call("ChunkServer.Append", &args, &reply) && (reply.Err == persister.Success || reply.Err == persister.OutOfChunk) {
			DPrintf("[client %d] finishes append: %v, requestId: %d\n", c.id, reply.Err, c.requestid)

			offset = reply.Offset
			err = reply.Err
			break
		}

		DPrintf("[client %d] fails to append: %v, requestId: %d\n", c.id, reply.Err, c.requestid)

		i = (i + 1) % len(c.servers)
		if i == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	return offset, err
}

func (c *clerk) read(chunkName string, offset int, size int) ([]byte, persister.Err) {
	c.requestid++

	var data []byte
	var err persister.Err
	i := 0
	for {
		args := ReadArgs{
			ChunkName: chunkName,
			Offset:    offset,
			Size:      size,
			ClerkId:   c.id,
			RequestId: c.requestid,
		}
		var reply ReadReply

		if c.servers[i].Call("ChunkServer.Read", &args, &reply) && (reply.Err == persister.Success || reply.Err == persister.OutOfChunk) {
			DPrintf("[client %d] finishes read: %v, requestId: %d\n", c.id, reply.Err, c.requestid)

			data = reply.Data
			err = reply.Err
			break
		}

		DPrintf("[client %d] fails to read: %v, requestId: %d\n", c.id, reply.Err, c.requestid)

		i = (i + 1) % len(c.servers)
		if i == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	return data, err
}

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
	zkServer     *izk.FakeZKServer
	chunkServers []*ChunkServer
	clerks       map[*clerk][]string
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

	zkServer := izk.MakeFakeZKServer()
	cfg.zkServer = zkServer
	svc := rpc.MakeService(zkServer)
	srv := rpc.MakeServer()
	srv.AddService(svc)
	net.AddServer(zoo, srv)

	clientIP := "config"
	cfg.net.MakeEnd(clientIP + "-" + zoo)
	cfg.net.Connect(clientIP+"-"+zoo, zoo)
	cfg.net.Enable(clientIP+"-"+zoo, true)
	zkClient, err := makeZKClient(net, clientIP)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := makeZKConn(zkClient)
	if err != nil {
		t.Fatal(err)
	}
	conn.Create("/chain", nil, 0)
	conn.Create("/commit", nil, 0)
	conn.Create("/commit/chain", []byte(strconv.Itoa(-1)), 0)
	conn.Close()

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
				cfg.net.MakeEnd(cfg.ips[i] + "-" + cfg.ips[j] + ":8999")
				cfg.net.Connect(cfg.ips[i]+"-"+cfg.ips[j]+":8999", cfg.ips[j])
			}
		}
		cfg.net.MakeEnd(cfg.ips[i] + "-" + zoo)
		cfg.net.Connect(cfg.ips[i]+"-"+zoo, zoo)
	}

	cfg.chunkServers = make([]*ChunkServer, n)
	cfg.clerks = make(map[*clerk][]string)
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
			cfg.net.Enable(cfg.ips[i]+"-"+cfg.ips[j]+":8999", true)
			cfg.net.Enable(cfg.ips[j]+"-"+cfg.ips[i]+":8999", true)
		}
	}

	// 连接zookeeper
	cfg.net.Enable(cfg.ips[i]+"-"+zoo, true)
	zkClient, err := makeZKClient(cfg.net, cfg.ips[i])
	if err != nil {
		cfg.t.Fatal(err)
	}
	zkConn, err := makeZKConn(zkClient)
	if err != nil {
		cfg.t.Fatal(err)
	}

	clients := rpc.MakeFakeClients(cfg.net, cfg.ips[i])

	// 确保同一个Persister只有一个节点在使用
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	} else {
		cfg.saved[i] = persister.MakeMemoryPersister()
	}

	cs := MakeChunkServer(zkClient, zkConn, clients, "/chain", "", cfg.ips[i], "/commit/chain", cfg.saved[i])
	cfg.chunkServers[i] = cs

	// 创建服务
	nodeSvc := rpc.MakeService(cs.node)
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
			cfg.net.Enable(cfg.ips[i]+"-"+cfg.ips[j]+":8999", false)
			cfg.net.Enable(cfg.ips[j]+"-"+cfg.ips[i]+":8999", false)
		}
	}
	cfg.net.DeleteServer(cfg.ips[i])

	cfg.net.Enable(cfg.ips[i]+"-"+zoo, false)

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

func (cfg *config) makeClient() *clerk {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	ends := make([]*rpc.ClientEnd, len(cfg.chunkServers))
	endnames := make([]string, len(cfg.chunkServers))
	for j := 0; j < len(cfg.chunkServers); j++ {
		endnames[j] = randstring(16)
		ends[j] = cfg.net.MakeEnd(endnames[j])
		cfg.net.Connect(endnames[j], cfg.ips[j])
	}

	ck := makeClerk(ends)
	cfg.clerks[ck] = endnames
	for j := 0; j < len(cfg.chunkServers); j++ {
		cfg.net.Enable(endnames[j], true)
	}
	return ck
}

func (cfg *config) deleteClient(ck *clerk) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	delete(cfg.clerks, ck)
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
