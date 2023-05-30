package rpc

import (
	"net/rpc"
)

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
	c, err := rpc.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return &RealClient{c: c}, err
}
