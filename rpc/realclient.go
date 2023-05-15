package rpc

import "net/rpc"

type RealClient struct {
	c *rpc.Client
}

func (rc *RealClient) Call(svcMeth string, args interface{}, reply interface{}) bool {
	return rc.c.Call(svcMeth, args, reply) == nil
}

func (rc *RealClient) Close() {
	rc.c.Close()
}

type RealClients struct{}

func MakeRealClients() *RealClients {
	return &RealClients{}
}

func (rcs *RealClients) MakeClient(address string) (Client, error) {
	rc := &RealClient{}
	c, err := rpc.Dial("tcp", address)
	rc.c = c
	return rc, err
}
