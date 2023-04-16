package rpc

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

type JunkArgs struct {
	X int
}
type JunkReply struct {
	X string
}

type JunkServer struct {
	mu   sync.Mutex
	log1 []string
	log2 []int
}

func (js *JunkServer) Handler1(args string, reply *int) {
	js.mu.Lock()
	defer js.mu.Unlock()
	js.log1 = append(js.log1, args)
	*reply, _ = strconv.Atoi(args)
}

func (js *JunkServer) Handler2(args int, reply *string) {
	js.mu.Lock()
	defer js.mu.Unlock()
	js.log2 = append(js.log2, args)
	*reply = "handler2-" + strconv.Itoa(args)
}

func (js *JunkServer) Handler3(args int, reply *int) {
	js.mu.Lock()
	defer js.mu.Unlock()
	time.Sleep(20 * time.Second)
	*reply = -args
}

// args是指针
func (js *JunkServer) Handler4(args *JunkArgs, reply *JunkReply) {
	reply.X = "pointer"
}

// args不是指针
func (js *JunkServer) Handler5(args JunkArgs, reply *JunkReply) {
	reply.X = "no pointer"
}

func (js *JunkServer) Handler6(args string, reply *int) {
	js.mu.Lock()
	defer js.mu.Unlock()
	*reply = len(args)
}

func (js *JunkServer) Handler7(args int, reply *string) {
	js.mu.Lock()
	defer js.mu.Unlock()
	*reply = ""
	for i := 0; i < args; i++ {
		*reply = *reply + "y"
	}
}

// 简单测试
func TestBasic(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer("server99", rs)

	rn.Connect("end1-99", "server99")
	rn.Enable("end1-99", true)

	{
		reply := ""
		e.Call("JunkServer.Handler2", 111, &reply)
		if reply != "handler2-111" {
			t.Fatalf("wrong reply from Handler2")
		}
	}

	{
		reply := 0
		e.Call("JunkServer.Handler1", "9099", &reply)
		if reply != 9099 {
			t.Fatalf("wrong reply from Handler1")
		}
	}
}

// 分别测试args是指针和不是指针的情况
func TestTypes(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer("server99", rs)

	rn.Connect("end1-99", "server99")
	rn.Enable("end1-99", true)

	{
		var args JunkArgs
		var reply JunkReply
		e.Call("JunkServer.Handler4", &args, &reply)
		if reply.X != "pointer" {
			t.Fatalf("wrong reply from Handler4")
		}
	}

	{
		var args JunkArgs
		var reply JunkReply
		e.Call("JunkServer.Handler5", args, &reply)
		if reply.X != "no pointer" {
			t.Fatalf("wrong reply from Handler5")
		}
	}
}

// 测试net.Enable(endname, false)是否能禁止通信
func TestDisconnect(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer("server99", rs)

	rn.Connect("end1-99", "server99")

	// 端点没有启用
	{
		reply := ""
		e.Call("JunkServer.Handler2", 111, &reply)
		if reply != "" {
			t.Fatalf("unexpected reply from Handler2")
		}
	}

	rn.Enable("end1-99", true)

	{
		reply := 0
		e.Call("JunkServer.Handler1", "9099", &reply)
		if reply != 9099 {
			t.Fatalf("wrong reply from Handler1")
		}
	}
}

// 测试rpc数量的统计,net.GetCount()
func TestCounts(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer(99, rs)

	rn.Connect("end1-99", 99)
	rn.Enable("end1-99", true)

	for i := 0; i < 17; i++ {
		reply := ""
		e.Call("JunkServer.Handler2", i, &reply)
		wanted := "handler2-" + strconv.Itoa(i)
		if reply != wanted {
			t.Fatalf("wrong reply %v from Handler1, expecting %v", reply, wanted)
		}
	}

	n := rn.GetCount(99)
	if n != 17 {
		t.Fatalf("wrong GetCount() %v, expected 17\n", n)
	}
}

// 测试统计的总字节数,net.GetTotalBytes()
func TestBytes(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer(99, rs)

	rn.Connect("end1-99", 99)
	rn.Enable("end1-99", true)

	for i := 0; i < 17; i++ {
		args := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
		args = args + args
		args = args + args
		reply := 0
		e.Call("JunkServer.Handler6", args, &reply)
		wanted := len(args)
		if reply != wanted {
			t.Fatalf("wrong reply %v from Handler6, expecting %v", reply, wanted)
		}
	}

	n := rn.GetTotalBytes()
	if n < 4828 || n > 6000 {
		t.Fatalf("wrong GetTotalBytes() %v, expected about 5000\n", n)
	}

	for i := 0; i < 17; i++ {
		args := 107
		reply := ""
		e.Call("JunkServer.Handler7", args, &reply)
		wanted := args
		if len(reply) != wanted {
			t.Fatalf("wrong reply len=%v from Handler6, expecting %v", len(reply), wanted)
		}
	}

	nn := rn.GetTotalBytes() - n
	if nn < 1800 || nn > 2500 {
		t.Fatalf("wrong GetTotalBytes() %v, expected about 2000\n", nn)
	}
}

// 并发客户端测试
func TestConcurrentMany(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer(1000, rs)

	ch := make(chan interface{})

	nclients := 20
	nrpcs := 10
	for ii := 0; ii < nclients; ii++ {
		go func(i int) {
			var x interface{}
			n := 0
			defer func() { ch <- x }()

			e := rn.MakeEnd(i)
			rn.Connect(i, 1000)
			rn.Enable(i, true)

			for j := 0; j < nrpcs; j++ {
				arg := i*100 + j
				reply := ""
				e.Call("JunkServer.Handler2", arg, &reply)
				wanted := "handler2-" + strconv.Itoa(arg)
				if reply != wanted {
					x = fmt.Sprintf("wrong reply %v from Handler2, expecting %v", reply, wanted)
					return
				}
				n += 1
			}
			x = n
		}(ii)
	}

	// 统计实际执行的rpc数量
	total := 0
	for ii := 0; ii < nclients; ii++ {
		x := <-ch
		if n, ok := x.(int); ok {
			total += n
		} else {
			t.Fatal(x)
		}
	}

	// 检查是否所有的rpc都已执行
	if total != nclients*nrpcs {
		t.Fatalf("wrong number of RPCs completed, got %v, expected %v", total, nclients*nrpcs)
	}

	// 检查Network统计的rpc数量是否正确
	n := rn.GetCount(1000)
	if n != total {
		t.Fatalf("wrong GetCount() %v, expected %v\n", n, total)
	}
}

// 测试网络不稳定
func TestUnreliable(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()
	rn.Reliable(false)

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer(1000, rs)

	ch := make(chan interface{})

	nclients := 300
	for ii := 0; ii < nclients; ii++ {
		go func(i int) {
			var x interface{}
			n := 0
			defer func() { ch <- x }()

			e := rn.MakeEnd(i)
			rn.Connect(i, 1000)
			rn.Enable(i, true)

			arg := i * 100
			reply := ""
			ok := e.Call("JunkServer.Handler2", arg, &reply)
			// 网络不稳定时ok可能为false
			if ok {
				wanted := "handler2-" + strconv.Itoa(arg)
				if reply != wanted {
					x = fmt.Sprintf("wrong reply %v from Handler1, expecting %v", reply, wanted)
					return
				}
				n += 1
			}
			x = n
		}(ii)
	}

	total := 0
	for ii := 0; ii < nclients; ii++ {
		x := <-ch
		if n, ok := x.(int); ok {
			total += n
		} else {
			t.Fatal(x)
		}
	}

	if total == nclients || total == 0 {
		t.Fatalf("all RPCs succeeded despite unreliable")
	}
}

// 测试同一个客户端并发发送rpc
func TestConcurrentOne(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer(1000, rs)

	e := rn.MakeEnd("c")
	rn.Connect("c", 1000)
	rn.Enable("c", true)

	ch := make(chan interface{})

	nrpcs := 20
	for ii := 0; ii < nrpcs; ii++ {
		go func(i int) {
			var x interface{}
			n := 0
			defer func() { ch <- x }()

			arg := 100 + i
			reply := ""
			e.Call("JunkServer.Handler2", arg, &reply)
			wanted := "handler2-" + strconv.Itoa(arg)
			if reply != wanted {
				x = fmt.Sprintf("wrong reply %v from Handler2, expecting %v", reply, wanted)
				return
			}
			n += 1
			x = n
		}(ii)
	}

	total := 0
	for ii := 0; ii < nrpcs; ii++ {
		x := <-ch
		if n, ok := x.(int); ok {
			total += n
		} else {
			t.Fatal(x)
		}
	}

	// 检查客户端发送的rpc的数量
	if total != nrpcs {
		t.Fatalf("wrong number of RPCs completed, got %v, expected %v", total, nrpcs)
	}

	js.mu.Lock()
	defer js.mu.Unlock()
	// 检查服务器接收的rpc的数量
	if len(js.log2) != nrpcs {
		t.Fatalf("wrong number of RPCs delivered")
	}

	// 检查Network统计的rpc的数量
	n := rn.GetCount(1000)
	if n != total {
		t.Fatalf("wrong GetCount() %v, expected %v\n", n, total)
	}
}

// 测试客户端启用前发送的rpc不会延迟启用后发送的rpc
func TestRegression1(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer(1000, rs)

	e := rn.MakeEnd("c")
	rn.Connect("c", 1000)

	// 在没有启用客户端的情况下发送rpc,全都延迟返回false
	rn.Enable("c", false)
	ch := make(chan bool)
	nrpcs := 20
	for ii := 0; ii < nrpcs; ii++ {
		go func(i int) {
			ok := false
			defer func() { ch <- ok }()

			arg := 100 + i
			reply := ""
			e.Call("JunkServer.Handler2", arg, &reply)
			ok = true
		}(ii)
	}

	time.Sleep(100 * time.Millisecond)

	// 启用客户端,检查rpc是否能快速返回,即不受到失败rpc的影响
	t0 := time.Now()
	rn.Enable("c", true)
	{
		arg := 99
		reply := ""
		e.Call("JunkServer.Handler2", arg, &reply)
		wanted := "handler2-" + strconv.Itoa(arg)
		if reply != wanted {
			t.Fatalf("wrong reply %v from Handler2, expecting %v", reply, wanted)
		}
	}
	dur := time.Since(t0).Seconds()

	if dur > 0.03 {
		t.Fatalf("RPC took too long (%v) after Enable", dur)
	}

	for ii := 0; ii < nrpcs; ii++ {
		<-ch
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	// 检查Sever句柄只调用了一次
	if len(js.log2) != 1 {
		t.Fatalf("wrong number (%v) of RPCs delivered, expected 1", len(js.log2))
	}

	// 检查Sever只统计了一次rpc调用
	n := rn.GetCount(1000)
	if n != 1 {
		t.Fatalf("wrong GetCount() %v, expected %v\n", n, 1)
	}
}

// 如果Server在rpc执行的过程中关闭,rpc应立刻返回false而不是等待返回结果
func TestKilled(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer("server99", rs)

	rn.Connect("end1-99", "server99")
	rn.Enable("end1-99", true)

	doneCh := make(chan bool)
	go func() {
		reply := 0
		ok := e.Call("JunkServer.Handler3", 99, &reply)
		doneCh <- ok
	}()

	time.Sleep(1000 * time.Millisecond)

	select {
	case <-doneCh:
		t.Fatalf("Handler3 should not have returned yet")
	case <-time.After(100 * time.Millisecond):
	}

	rn.DeleteServer("server99")

	// 调度goroutine会在等待Server返回结果的过程中检查Server是否关闭,并在检查到关闭时立刻返回
	select {
	case x := <-doneCh:
		if x != false {
			t.Fatalf("Handler3 returned successfully despite DeleteServer()")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("Handler3 should return after DeleteServer()")
	}
}

// 基准测试
func TestBenchmark(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer("server99", rs)

	rn.Connect("end1-99", "server99")
	rn.Enable("end1-99", true)

	t0 := time.Now()
	n := 100000
	for iters := 0; iters < n; iters++ {
		reply := ""
		e.Call("JunkServer.Handler2", 111, &reply)
		if reply != "handler2-111" {
			t.Fatalf("wrong reply from Handler2")
		}
	}
	fmt.Printf("%v for %v\n", time.Since(t0), n)
}
