package rpc

type Client interface {
	Call(svcMeth string, args interface{}, reply interface{}) bool // 调用rpc服务
	Close()
}

type Clients interface {
	MakeClient(address string) (Client, error)
}
