package main

import (
	"crfs/chunkserver"
	"crfs/common"
	"crfs/master"
	"crfs/persister"
	"crfs/rpc"
	"crfs/zk"
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

func main() {
	// 1.解析命令行参数
	masterAddress := flag.String("master", "", "master address")
	var zookeeperAddresses common.StringSliceFlag
	flag.Var(&zookeeperAddresses, "zookeeper", "zookeeper addresses")
	flag.Parse()

	if *masterAddress == "" || len(zookeeperAddresses) == 0 {
		fmt.Fprintln(os.Stderr, "缺少参数")
		flag.Usage()
		return
	}

	// 2.创建master client
	clients := rpc.MakeRealClients()
	masterClient, err := clients.MakeClient(*masterAddress)
	if err != nil {
		log.Fatal(err)
	}

	// 3.创建zookeeper client
	zkClient := zk.MakeRealZKClient([]string(zookeeperAddresses), common.SessionTimeout)

	// 4.获取hostname
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	// 5.申请chain
	var chainName string
	for {
		args := master.AllocateServerArgs{
			ChunkServer: hostname,
		}
		var reply master.AllocateServerReply

		if masterClient.Call("Master.AllocateServer", &args, &reply) {
			chainName = reply.ChainName
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	// 6.创建persister
	p := persister.MakeFilePersister("/data", chainName)

	chainPath := common.ZKChainNode + "/" + chainName
	commitPath := common.ZKCommitNode + "/" + chainName

	// 7.创建chunk server
	done := make(chan bool)
	cs := chunkserver.MakeChunkServer(masterClient, zkClient, clients, hostname, chainPath, commitPath, common.CleanupInterval, p, done)

	go common.Listen(cs.Node, 8999, "node")
	go common.Listen(cs, 7999, "chunk")

	<-done
}
