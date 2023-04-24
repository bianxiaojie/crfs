package rpc

import (
	"fmt"
	"log"
	"runtime"
	"strconv"
	"testing"
)

func TestConcurrentOneClient(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer("server99", rs)

	_ = rn.MakeEnd("server0-server99")
	rn.Enable("server0-server99", true)
	rn.Connect("server0-server99", "server99")

	cls := MakeFakeClients(rn, "server0")
	cl, _ := cls.MakeClient("server99")

	ch := make(chan interface{})

	nrpcs := 30
	for ii := 0; ii < nrpcs; ii++ {
		go func(i int) {
			var x interface{}
			n := 0
			defer func() { ch <- x }()

			arg := 100 + i
			reply := ""
			cl.Call("JunkServer.Handler2", arg, &reply)
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
	for i := 0; i < nrpcs; i++ {
		x := <-ch
		if n, ok := x.(int); ok {
			total += n
		} else {
			log.Fatal(x)
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
	n := rn.GetCount("server99")
	if n != total {
		t.Fatalf("wrong GetCount() %v, expected %v\n", n, total)
	}
}

func TestSameClientsMany(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer("server99", rs)

	_ = rn.MakeEnd("server0-server99")
	rn.Enable("server0-server99", true)
	rn.Connect("server0-server99", "server99")

	cls := MakeFakeClients(rn, "server0")

	ch := make(chan interface{})

	nclients := 10
	nrpcs := 10
	for ii := 0; ii < nclients; ii++ {
		go func(i int) {
			var x interface{}
			n := 0
			defer func() { ch <- x }()

			cl, _ := cls.MakeClient("server99")

			for j := 0; j < nrpcs; j++ {
				arg := i*100 + j
				reply := ""
				cl.Call("JunkServer.Handler2", arg, &reply)
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

	// 检查客户端发送的rpc的数量
	if total != nclients*nrpcs {
		t.Fatalf("wrong number of RPCs completed, got %v, expected %v", total, nrpcs)
	}

	js.mu.Lock()
	defer js.mu.Unlock()
	// 检查服务器接收的rpc的数量
	if len(js.log2) != nclients*nrpcs {
		t.Fatalf("wrong number of RPCs delivered")
	}

	// 检查Network统计的rpc的数量
	n := rn.GetCount("server99")
	if n != total {
		t.Fatalf("wrong GetCount() %v, expected %v\n", n, total)
	}
}
