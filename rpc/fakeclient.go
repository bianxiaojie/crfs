package rpc

import "fmt"

type fakeClient struct {
	end *ClientEnd
}

func (fc *fakeClient) Call(svcMeth string, args interface{}, reply interface{}) bool {
	return fc.end.Call(svcMeth, args, reply)
}

type fakeClients struct {
	net *Network
	me  string // 客户端服务器地址
}

func (fcs *fakeClients) MakeClient(serverAddr string) (Client, error) {
	endname := fmt.Sprintf("%s-%s", fcs.me, serverAddr)
	end, ok := fcs.net.GetEnd(endname)
	if !ok {
		return nil, fmt.Errorf("服务器%s不存在", serverAddr)
	}
	fc := fakeClient{
		end: end,
	}
	return &fc, nil
}

func MakeClients(rn *Network, servername string) Clients {
	fcs := &fakeClients{
		net: rn,
		me:  servername,
	}
	return fcs
}
