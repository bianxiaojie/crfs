package zk

import (
	"crfs/rpc"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"sort"
	"strings"
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

type server struct {
	mu       sync.Mutex
	zkClient ZKClient
	zkConn   ZKConn
	address  string
}

func makeServer(zkClient ZKClient, address string) *server {
	server := &server{}
	server.zkClient = zkClient
	server.address = address
	return server
}

func (s *server) getConn() ZKConn {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.zkConn
}

func (s *server) tryConnect() {
	s.mu.Lock()
	if s.zkConn != nil && !s.zkConn.Closed() {
		s.mu.Unlock()
		return
	}
	s.zkConn = nil
	s.mu.Unlock()

	zkConn, err := s.zkClient.Connect()
	if err != nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.zkConn != nil && !s.zkConn.Closed() {
		go func() { zkConn.Close() }()
		return
	}

	s.zkConn = zkConn
}

func (s *server) create(path string, data []byte, flags int32) (string, error) {
	conn := s.getConn()
	if conn == nil {
		return "", zk.ErrConnectionClosed
	}

	return conn.Create(path, data, flags)
}

func (s *server) delete(path string, version int32) error {
	conn := s.getConn()
	if conn == nil {
		return zk.ErrConnectionClosed
	}

	return conn.Delete(path, version)
}

func (s *server) exists(path string, watch bool) (bool, int32, <-chan zk.Event, error) {
	conn := s.getConn()
	if conn == nil {
		return false, 0, nil, zk.ErrConnectionClosed
	}

	return conn.Exists(path, watch)
}

func (s *server) get(path string, watch bool) (string, int32, <-chan zk.Event, error) {
	conn := s.getConn()
	if conn == nil {
		return "", 0, nil, zk.ErrConnectionClosed
	}

	data, version, eventCh, err := conn.Get(path, watch)
	return string(data), version, eventCh, err
}

func (s *server) set(path string, data []byte, version int32) (int32, error) {
	conn := s.getConn()
	if conn == nil {
		return 0, zk.ErrConnectionClosed
	}

	return conn.Set(path, data, version)
}

func (s *server) children(path string, watch bool) ([]string, int32, <-chan zk.Event, error) {
	conn := s.getConn()
	if conn == nil {
		return nil, 0, nil, zk.ErrConnectionClosed
	}

	return conn.Children(path, watch)
}

func (s *server) close() {
	s.mu.Lock()
	if s.zkConn == nil {
		s.mu.Unlock()
		return
	}
	zkConn := s.zkConn
	s.mu.Unlock()

	zkConn.Close()
}

type config struct {
	net      *rpc.Network
	servers  []*server
	zkServer *FakeZKServer

	t0     time.Time // 测试代码调用cfg.begin()的时间点
	rpcs0  int       // 测试开始时rpcTotal()的值
	bytes0 int64
}

func makeConfig(n int) *config {
	runtime.GOMAXPROCS(4)

	cfg := &config{}

	net := rpc.MakeNetwork()
	cfg.net = net

	zkServer := MakeFakeZKServer()
	cfg.zkServer = zkServer

	svc := rpc.MakeService(zkServer)
	srv := rpc.MakeServer()
	srv.AddService(svc)
	net.AddServer(zoo, srv)

	cfg.servers = make([]*server, n)
	ip := [4]byte{192, 168, 0, 1}
	for i := 0; i < n; i++ {
		address := IPToString(ip)

		endname := address + "-" + zoo
		net.MakeEnd(endname)
		net.Connect(endname, zoo)
		net.Enable(endname, true)
		clients := rpc.MakeFakeClients(net, address)
		client, err := clients.MakeClient(zoo)
		if err != nil {
			log.Fatalln(err)
		}
		fzkc := MakeFakeZKClient(client, []string{zoo}, sessionTimeout)
		server := makeServer(fzkc, address)
		cfg.servers[i] = server

		ip = IPAddOne(ip)
	}

	return cfg
}

func (cfg *config) enable(address string, enabled bool) {
	endname := address + "-" + zoo
	cfg.net.Enable(endname, enabled)
}

func (cfg *config) begin(description string) {
	fmt.Printf("%s ...\n", description)
	cfg.t0 = time.Now()
	cfg.rpcs0 = cfg.net.GetTotalCount()
	cfg.bytes0 = cfg.net.GetTotalBytes()
}

func (cfg *config) end() {
	t := time.Since(cfg.t0).Seconds()              // 运行时间
	nrpc := cfg.net.GetTotalCount() - cfg.rpcs0    // rpc总数
	nbytes := cfg.net.GetTotalBytes() - cfg.bytes0 // rpc发送的字节总数

	fmt.Printf("  ... Passed --")
	fmt.Printf("  %4.1f  %4d %7d\n", t, nrpc, nbytes)
}

func (cfg *config) cleanup() {
	for _, s := range cfg.servers {
		s.close()
	}
	cfg.zkServer.Kill()
	cfg.net.Cleanup()
}

func StringsEqual(strings1 []string, strings2 []string) bool {
	if len(strings1) != len(strings2) {
		return false
	}

	sort.Strings(strings1)
	sort.Strings(strings2)

	return reflect.DeepEqual(strings1, strings2)
}

func TestCreateSingle(t *testing.T) {
	cfg := makeConfig(1)
	defer cfg.cleanup()

	cfg.begin("Test (Create): single client creates files")

	s := cfg.servers[0]
	s.tryConnect()

	directory := "/test"
	if _, err := s.create(directory, nil, 0); err != nil {
		t.Fatal(err)
	}

	nfiles := 10
	expected := make([]string, nfiles)
	for i := 1; i <= nfiles; i++ {
		file := fmt.Sprintf("file%d", i)
		if _, err := s.create(directory+"/"+file, nil, 0); err != nil {
			t.Fatal(err)
		}
		expected[i-1] = file
	}

	actual, _, _, err := s.children(directory, false)
	if err != nil {
		t.Fatal(err)
	}
	if !StringsEqual(expected, actual) {
		t.Fatalf("expected: %v, actual: %v", expected, actual)
	}

	cfg.end()
}

func TestCreateMany(t *testing.T) {
	nservers := 3
	cfg := makeConfig(nservers)
	defer cfg.cleanup()

	cfg.begin("Test (Create): many clients create files")

	directory := "/test"
	nfiles := 10
	file := "file"
	done := make(chan interface{})
	for _, s := range cfg.servers {
		go func(s *server) {
			var result interface{}
			defer func() { done <- result }()

			s.tryConnect()

			if _, err := s.create(directory, nil, 0); err != nil && err != zk.ErrNodeExists {
				result = err
				return
			}

			for i := 0; i < nfiles; i++ {
				if _, err := s.create(fmt.Sprintf("%s/%s%d", directory, file, i+1), nil, 0); err != nil && err != zk.ErrNodeExists {
					result = err
					return
				}
			}
		}(s)
	}

	for i := 0; i < len(cfg.servers); i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}

	expected := make([]string, nfiles)
	for i := 0; i < len(expected); i++ {
		expected[i] = fmt.Sprintf("%s%d", file, i+1)
	}
	actual, _, _ := cfg.zkServer.root.Children(strings.Split(directory[1:], "/"))
	if !StringsEqual(expected, actual) {
		t.Fatalf("expected: %v, actual: %v", expected, actual)
	}

	cfg.end()
}

func TestCreateSequencialSingle(t *testing.T) {
	cfg := makeConfig(1)
	defer cfg.cleanup()

	cfg.begin("Test (Create): single client creates sequential files")

	s := cfg.servers[0]
	s.tryConnect()

	directory := "/test"
	if _, err := s.create(directory, nil, 0); err != nil {
		t.Fatal(err)
	}

	nfiles := 10
	expected := make([]string, nfiles)
	file := "file"
	for i := 1; i <= nfiles; i++ {
		if _, err := s.create(directory+"/"+file, nil, zk.FlagSequence); err != nil {
			t.Fatal(err)
		}
		expected[i-1] = fmt.Sprintf("%s%d", file, i)
	}

	actual, _, _, err := s.children(directory, false)
	if err != nil {
		t.Fatal(err)
	}
	if !StringsEqual(expected, actual) {
		t.Fatalf("expected: %v, actual: %v", expected, actual)
	}

	cfg.end()
}

func TestCreateSequencialMany(t *testing.T) {
	nservers := 3
	cfg := makeConfig(nservers)
	defer cfg.cleanup()

	cfg.begin("Test (Create): many clients create sequential files")

	directory := "/test"
	nfiles := 10
	file := "file"
	done := make(chan interface{})
	for _, s := range cfg.servers {
		go func(s *server) {
			var result interface{}
			defer func() { done <- result }()

			s.tryConnect()

			if _, err := s.create(directory, nil, 0); err != nil && err != zk.ErrNodeExists {
				result = err
				return
			}

			for i := 0; i < nfiles; i++ {
				if _, err := s.create(directory+"/"+file, nil, zk.FlagSequence); err != nil {
					result = err
					return
				}
			}
		}(s)
	}

	for i := 0; i < len(cfg.servers); i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}

	expected := make([]string, nservers*nfiles)
	for i := 0; i < len(expected); i++ {
		expected[i] = fmt.Sprintf("%s%d", file, i+1)
	}
	actual, _, _ := cfg.zkServer.root.Children(strings.Split(directory[1:], "/"))
	if !StringsEqual(expected, actual) {
		t.Fatalf("expected: %v, actual: %v", expected, actual)
	}

	cfg.end()
}

func TestDeleteWithVersion(t *testing.T) {
	cfg := makeConfig(1)
	defer cfg.cleanup()

	cfg.begin("Test (Delete): delete with version")

	directory := "/test"
	file := "file"

	var mu sync.Mutex
	var err error
	cond := sync.NewCond(&mu)
	go func() {
		mu.Lock()
		defer mu.Unlock()

		s := cfg.servers[0]
		s.tryConnect()

		if _, e := s.create(directory, nil, 0); e != nil {
			err = e
			return
		}

		if _, e := s.create(directory+"/"+file, nil, 0); e != nil {
			err = e
			return
		}

		cond.Wait()
		if e := s.delete(directory+"/"+file, 0); e != zk.ErrBadVersion {
			err = fmt.Errorf("expected: ErrBadVersion, actual: %v", e)
			return
		}

		if e := s.delete(directory+"/"+file, 1); e != nil {
			err = e
			return
		}
	}()

	time.Sleep(100 * time.Millisecond)
	if _, e := cfg.zkServer.root.Set([]string{}, strings.Split(directory[1:]+"/"+file, "/"), nil, 0); e != success {
		t.Fatal(errMap[e])
	}
	cond.Signal()

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if err != nil {
		t.Fatal(err)
	}

	cfg.end()
}

func TestExists(t *testing.T) {
	cfg := makeConfig(1)
	defer cfg.cleanup()

	cfg.begin("Test (Exists): test exists")

	s := cfg.servers[0]
	s.tryConnect()

	directory := "/test"
	file := "file"
	if _, err := s.create(directory, nil, 0); err != nil {
		t.Fatal(err)
	}

	if _, err := s.create(directory+"/"+file, nil, 0); err != nil {
		t.Fatal(err)
	}

	ok, _, _, err := s.exists(directory+"/"+file, false)
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatalf("create %s/%s, but exists return false", directory, file)
	}

	cfg.end()
}

func TestSetWithVersion(t *testing.T) {
	cfg := makeConfig(1)
	defer cfg.cleanup()

	cfg.begin("Test (Set): set with version")

	directory := "/test"
	file := "file"
	datav1 := "datav1"
	datav2 := "datav2"

	var mu sync.Mutex
	var err error
	cond := sync.NewCond(&mu)
	go func() {
		mu.Lock()
		defer mu.Unlock()

		s := cfg.servers[0]
		s.tryConnect()

		if _, e := s.create(directory, nil, 0); e != nil {
			err = e
			return
		}

		if _, e := s.create(directory+"/"+file, nil, 0); e != nil {
			err = e
			return
		}

		cond.Wait()
		if _, e := s.set(directory+"/"+file, []byte(datav2), 0); e != zk.ErrBadVersion {
			err = fmt.Errorf("expected: ErrBadVersion, actual: %v", e)
			return
		}

		data, _, _, e := s.get(directory+"/"+file, false)
		if e != nil {
			err = e
			return
		}
		if datav1 != data {
			err = fmt.Errorf("expected: %s, actual: %s", datav1, data)
			return
		}

		if _, e := s.set(directory+"/"+file, []byte(datav2), 1); e != nil {
			err = e
			return
		}

		data, _, _, e = s.get(directory+"/"+file, false)
		if e != nil {
			err = e
			return
		}
		if datav2 != data {
			err = fmt.Errorf("expected: %s, actual: %s", datav2, data)
			return
		}
	}()

	time.Sleep(100 * time.Millisecond)
	if _, e := cfg.zkServer.root.Set([]string{}, strings.Split(directory[1:]+"/"+file, "/"), []byte(datav1), 0); e != success {
		t.Fatal(e)
	}
	cond.Signal()

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if err != nil {
		t.Fatal(err)
	}

	cfg.end()
}

func TestWatchDelete(t *testing.T) {
	nservers := 3
	cfg := makeConfig(nservers)
	defer cfg.cleanup()

	cfg.begin("Test (Watch): watch delete files")

	directory := "/test"
	file := "file"
	done := make(chan interface{})
	for _, s := range cfg.servers {
		go func(s *server) {
			var result interface{}
			defer func() { done <- result }()

			s.tryConnect()

			if _, err := s.create(directory, nil, 0); err != nil && err != zk.ErrNodeExists {
				result = err
				return
			}

			if _, err := s.create(directory+"/"+file, nil, 0); err != nil && err != zk.ErrNodeExists {
				result = err
				return
			}

			time.Sleep(100 * time.Millisecond)

			ok, _, eventCh, err := s.exists(directory+"/"+file, true)
			if err != nil {
				result = err
				return
			}
			if !ok {
				result = fmt.Errorf("%s/%s not exists", directory, file)
				return
			}

			<-eventCh

			if _, _, _, err = s.exists(directory+"/"+file, false); err != nil {
				result = err
				return
			}
		}(s)
	}

	time.Sleep(300 * time.Millisecond)

	if err := cfg.zkServer.root.Delete([]string{}, strings.Split(directory[1:]+"/"+file, "/"), 0); err != success {
		t.Fatal(errMap[err])
	}

	for i := 0; i < len(cfg.servers); i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}

	cfg.end()
}

func TestWatchSet(t *testing.T) {
	nservers := 3
	cfg := makeConfig(nservers)
	defer cfg.cleanup()

	cfg.begin("Test (Watch): watch set files")

	directory := "/test"
	file := "file"
	datav1 := "datav1"
	done := make(chan interface{})
	for _, s := range cfg.servers {
		go func(s *server) {
			var result interface{}
			defer func() { done <- result }()

			s.tryConnect()

			if _, err := s.create(directory, nil, 0); err != nil && err != zk.ErrNodeExists {
				result = err
				return
			}

			if _, err := s.create(directory+"/"+file, nil, 0); err != nil && err != zk.ErrNodeExists {
				result = err
				return
			}

			time.Sleep(100 * time.Millisecond)

			_, _, eventCh, err := s.get(directory+"/"+file, true)
			if err != nil {
				result = err
				return
			}

			<-eventCh

			data, _, _, err := s.get(directory+"/"+file, false)
			if err != nil {
				result = err
				return
			}
			if datav1 != data {
				result = fmt.Errorf("expected: %s, actual: %s", datav1, data)
				return
			}
		}(s)
	}

	time.Sleep(300 * time.Millisecond)

	if _, err := cfg.zkServer.root.Set([]string{}, strings.Split(directory[1:]+"/"+file, "/"), []byte(datav1), 0); err != success {
		t.Fatal(errMap[err])
	}

	for i := 0; i < len(cfg.servers); i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}

	cfg.end()
}

func TestWatchEphemeral(t *testing.T) {
	nservers := 3
	cfg := makeConfig(nservers)
	defer cfg.cleanup()

	cfg.begin("Test (Watch): watch ephemeral files")

	expected := make([]string, 2)

	directory := "/test"
	file := "file"
	done := make(chan interface{})
	for i, s := range cfg.servers {
		if i > 0 {
			expected[i-1] = fmt.Sprintf("%s%d", file, i)
		}
		go func(i int, s *server) {
			var result interface{}
			defer func() { done <- result }()

			s.tryConnect()

			if _, err := s.create(directory, nil, 0); err != nil && err != zk.ErrNodeExists {
				result = err
				return
			}

			if _, err := s.create(fmt.Sprintf("%s/%s%d", directory, file, i), nil, zk.FlagEphemeral); err != nil {
				result = err
				return
			}

			time.Sleep(100 * time.Millisecond)

			actual, _, eventCh, err := s.children(directory, true)
			if err != nil {
				result = err
				return
			}
			if !StringsEqual(append(expected, "file0"), actual) {
				result = fmt.Errorf("expected: %v, actual: %v", append(expected, "file0"), actual)
				return
			}

			<-eventCh

			if i == 0 {
				return
			}

			actual, _, _, err = s.children(directory, true)
			if err != nil {
				result = err
				return
			}
			if !StringsEqual(expected, actual) {
				result = fmt.Errorf("expected: %v, actual: %v", expected, actual)
				return
			}
		}(i, s)
	}

	time.Sleep(300 * time.Millisecond)

	cfg.enable(cfg.servers[0].address, false)

	for i := 0; i < len(cfg.servers); i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}

	cfg.end()
}
