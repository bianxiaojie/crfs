package zk

import (
	"time"

	"github.com/go-zookeeper/zk"
)

type RealZKConn struct {
	conn *zk.Conn
}

func (rzkc *RealZKConn) Close() {
	rzkc.conn.Close()
}

func (rzkc *RealZKConn) Closed() bool {
	return rzkc.conn.State() == zk.StateDisconnected
}

func (rzkc *RealZKConn) Create(path string, data []byte, flags int32) (string, error) {
	acls := zk.WorldACL(zk.PermAll)
	return rzkc.conn.Create(path, data, flags, acls)
}

func (rzkc *RealZKConn) Delete(path string, version int32) error {
	return rzkc.conn.Delete(path, version)
}

func (rzkc *RealZKConn) Exists(path string, watch bool) (bool, int32, <-chan zk.Event, error) {
	if watch {
		ok, stat, eventCh, err := rzkc.conn.ExistsW(path)
		return ok, stat.Version, eventCh, err
	} else {
		ok, stat, err := rzkc.conn.Exists(path)
		return ok, stat.Version, nil, err
	}
}

func (rzkc *RealZKConn) Get(path string, watch bool) ([]byte, int32, <-chan zk.Event, error) {
	if watch {
		data, stat, eventCh, err := rzkc.conn.GetW(path)
		return data, stat.Version, eventCh, err
	} else {
		data, stat, err := rzkc.conn.Get(path)
		return data, stat.Version, nil, err
	}
}

func (rzkc *RealZKConn) Set(path string, data []byte, version int32) (int32, error) {
	stat, err := rzkc.conn.Set(path, data, version)
	return stat.Version, err
}

func (rzkc *RealZKConn) Children(path string, watch bool) ([]string, int32, <-chan zk.Event, error) {
	if watch {
		children, stat, eventCh, err := rzkc.conn.ChildrenW(path)
		return children, stat.Version, eventCh, err
	} else {
		children, stat, err := rzkc.conn.Children(path)
		return children, stat.Version, nil, err
	}
}

func (rzkc *RealZKConn) Sync(path string) (string, error) {
	return rzkc.conn.Sync(path)
}

type RealZKClient struct {
	servers        []string
	sessionTimeout time.Duration
}

func MakeRealZKClient(servers []string, sessionTimeout time.Duration) *RealZKClient {
	rzkc := &RealZKClient{}
	rzkc.servers = servers
	rzkc.sessionTimeout = sessionTimeout
	return rzkc
}

func (rzkc *RealZKClient) Connect() (ZKConn, error) {
	zkConn := &RealZKConn{}
	conn, _, err := zk.Connect(rzkc.servers, rzkc.sessionTimeout)
	zkConn.conn = conn
	return zkConn, err
}
