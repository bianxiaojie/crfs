package common

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strings"
	"time"
)

const (
	// zookeeper
	SessionTimeout = 3 * time.Second

	// master
	MasterPort      = 9999
	CleanupInterval = 24 * time.Hour
	ExpiredDuration = 3 * 24 * time.Hour

	// chunk server
	ChunkPort = 7999

	// chain node
	NodePort     = 8999
	ZKChainNode  = "/chain"
	ZKCommitNode = "/commit"
)

// flag
type StringSliceFlag []string

func (ssf *StringSliceFlag) String() string {
	return strings.Join(*ssf, ", ")
}

func (ssf *StringSliceFlag) Set(value string) error {
	*ssf = append(*ssf, value)
	return nil
}

// net
func GetLocalAddress() net.IP {
	con, error := net.Dial("udp", "8.8.8.8:80")
	if error != nil {
		log.Fatal(error)
	}
	defer con.Close()

	localAddress := con.LocalAddr().(*net.UDPAddr)

	return localAddress.IP
}

func Listen(rcvr any, port int, desciption string) {
	rpc.Register(rcvr)
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	fmt.Printf("%s, listen on: %s:%d\n", desciption, GetLocalAddress(), port)
	rpc.Accept(l)
}
