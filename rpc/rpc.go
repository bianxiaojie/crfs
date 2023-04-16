package rpc

type Client interface {
	Call(svcMeth string, args interface{}, reply interface{}) bool // 调用rpc服务
}

type Clients interface {
	MakeClient(me string) (Client, error)
}
