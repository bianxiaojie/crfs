package rpc

import (
	"fmt"
	"log"
	"sync/atomic"
)

type FakeClient struct {
	end    *ClientEnd
	closed int32
}

func (fc *FakeClient) Call(svcMeth string, args interface{}, reply interface{}) bool {
	if atomic.LoadInt32(&fc.closed) == 1 {
		log.Fatalln("client has been closed")
	}
	return fc.end.Call(svcMeth, args, reply)
}

func (fc *FakeClient) Close() {
	atomic.StoreInt32(&fc.closed, 1)
}

type FakeClients struct {
	net *Network
	me  string // 客户端服务器地址
}

func MakeFakeClients(rn *Network, servername string) *FakeClients {
	fcs := &FakeClients{
		net: rn,
		me:  servername,
	}
	return fcs
}

func (fcs *FakeClients) MakeClient(serverAddr string) (*FakeClient, error) {
	endname := fmt.Sprintf("%s-%s", fcs.me, serverAddr)
	end, ok := fcs.net.GetEnd(endname)
	if !ok {
		return nil, fmt.Errorf("服务器%s不存在", serverAddr)
	}
	fc := FakeClient{
		end: end,
	}
	return &fc, nil
}
