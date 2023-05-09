package node

import (
	"testing"
	"time"
)

func TestChainBasic(t *testing.T) {
	nnodes := 3
	cfg := makeConfig(t, nnodes, false)
	defer cfg.cleanup()

	cfg.begin("Test (Chain): basic chain test")
	time.Sleep(sessionTimeout)

	cfg.checkChain(0, 1, 2)

	cfg.end()
}

func TestChainVaried(t *testing.T) {
	nnodes := 3
	cfg := makeConfig(t, nnodes, false)
	defer cfg.cleanup()

	cfg.begin("Test (Chain): varied chain test")

	time.Sleep(sessionTimeout)
	cfg.checkChain(0, 1, 2)

	cfg.crashOne(0)
	time.Sleep(2 * sessionTimeout)
	cfg.checkChain(1, 2)

	cfg.crashOne(1)
	time.Sleep(2 * sessionTimeout)
	cfg.checkChain(2)

	cfg.startOne(0)
	cfg.startOne(1)
	time.Sleep(sessionTimeout)
	cfg.checkChain(0, 1, 2)

	cfg.end()
}

func TestChainVariedMore(t *testing.T) {
	nnodes := 5
	cfg := makeConfig(t, nnodes, false)
	defer cfg.cleanup()

	cfg.begin("Test (Chain): test chain varied more")

	iters := 3

	for i := 0; i < iters; i++ {
		time.Sleep(sessionTimeout)
		cfg.checkChain(0, 1, 2, 3, 4)

		cfg.crashOne(0)
		time.Sleep(2 * sessionTimeout)
		cfg.checkChain(1, 2, 3, 4)

		cfg.crashOne(1)
		cfg.crashOne(2)
		cfg.crashOne(3)
		cfg.startOne(0)
		time.Sleep(2 * sessionTimeout)
		cfg.checkChain(0, 4)

		cfg.startOne(1)
		cfg.startOne(2)
		cfg.startOne(3)
	}

	cfg.end()
}

func TestAgreementBasic(t *testing.T) {
	nnodes := 3
	cfg := makeConfig(t, nnodes, false)
	defer cfg.cleanup()

	cfg.begin("Test (Agreement): basic agreement test")
	time.Sleep(sessionTimeout)

	chain := cfg.checkChain(0, 1, 2)
	header, node1, node2 := chain[0], chain[1], chain[2]

	cmd := 0
	iters := 3
	for i := 0; i < iters; i++ {
		nd, _ := cfg.nCommitted(i, header, node1, node2)
		if nd > 0 {
			t.Fatal("有服务器在Start()就提交日志了")
		}

		xi := cfg.one(cmd, header, false, header, node1, node2)
		if xi != i {
			t.Fatalf("预期提交的索引是%d,但实际上是%d\n", i, xi)
		}
		cmd++
	}

	cfg.end()
}

func TestAgreementConcurrent(t *testing.T) {
	nnodes := 3
	cfg := makeConfig(t, nnodes, false)
	defer cfg.cleanup()

	cfg.begin("Test (Agreement): concurrent agreement test")
	time.Sleep(sessionTimeout)

	chain := cfg.checkChain(0, 1, 2)
	header, node1, node2 := chain[0], chain[1], chain[2]

	cmd := 0
	iters := 30
	done := make(chan bool)
	for i := 0; i < iters; i++ {
		go func(cmd int) {
			result := false
			defer func() { done <- result }()

			cfg.one(cmd, header, true, header, node1, node2)
			result = true
		}(cmd)
		cmd++
	}

	for i := 0; i < iters; i++ {
		if !<-done {
			cfg.t.Fatal("没能完成协议")
		}
	}

	cfg.end()
}

func TestAgreementCrashRestart(t *testing.T) {
	nnodes := 3
	cfg := makeConfig(t, nnodes, false)
	defer cfg.cleanup()

	cfg.begin("Test (Agreement): agreement test with crash and restart")
	time.Sleep(sessionTimeout)

	chain := cfg.checkChain(0, 1, 2)
	header1, node11, node12 := chain[0], chain[1], chain[2]

	cmd := 0
	iters := 10
	for i := 0; i < iters; i++ {
		cfg.one(cmd, header1, false, header1, node11, node12)
		cmd++
	}

	cfg.crashOne(node11)
	time.Sleep(2 * sessionTimeout)

	chain = cfg.checkChain(header1, node12)
	if header1 != chain[0] {
		cfg.t.Fatalf("中间节点crash不应该改变头节点,头节点应当为: %s, 实际的头节点为: %s\n", cfg.addresses[header1], cfg.addresses[chain[0]])
	}

	for i := 0; i < iters; i++ {
		cfg.one(cmd, header1, false, header1, node12)
		cmd++
	}

	cfg.crashOne(header1)
	cfg.startOne(node11)
	time.Sleep(2 * sessionTimeout)

	chain = cfg.checkChain(node11, node12)
	header2, node21 := chain[0], chain[1]

	for i := 0; i < iters; i++ {
		cfg.one(cmd, header2, false, header2, node21)
		cmd++
	}

	cfg.startOne(header1)
	time.Sleep(sessionTimeout)

	chain = cfg.checkChain(header2, node21, header1)
	if header2 != chain[0] {
		cfg.t.Fatalf("中间节点crash不应该改变头节点,头节点应当为: %s, 实际的头节点为: %s\n", cfg.addresses[header2], cfg.addresses[chain[0]])
	}

	for i := 0; i < iters; i++ {
		cfg.one(cmd, header2, false, header2, node21, header1)
		cmd++
	}

	cfg.end()
}

func TestAgreementDisconnect(t *testing.T) {
	nnodes := 3
	cfg := makeConfig(t, nnodes, false)
	defer cfg.cleanup()

	cfg.begin("Test (Agreement): agreement test with disconnect")
	time.Sleep(sessionTimeout)

	chain := cfg.checkChain(0, 1, 2)
	header, node1, node2 := chain[0], chain[1], chain[2]
	cfg.enable(node1, node2, false)

	cmd := 0
	iters := 10
	for i := 0; i < iters; i++ {
		cfg.nodes[header].Start(cmd)
		cmd++
	}

	if nCommitted, _ := cfg.nCommitted(0, header, node1, node2); nCommitted != 0 {
		cfg.t.Fatalf("节点%s和尾节点%s网络断开,不应有日志提交,但有%d个节点提交了第一个日志\n", cfg.addresses[node1], cfg.addresses[node2], nCommitted)
	}

	cfg.enable(node1, node2, true)
	time.Sleep(sessionTimeout)

	if nCommitted, _ := cfg.nCommitted(cmd-1, header, node1, node2); nCommitted != 3 {
		cfg.t.Fatalf("节点%s和尾节点%s网络恢复,所有节点应当提交所有日志,但只有%d个节点提交了所有日志\n", cfg.addresses[node1], cfg.addresses[node2], nCommitted)
	}

	for i := 0; i < iters; i++ {
		cfg.one(cmd, header, false, header, node1)
		cmd++
	}

	cfg.end()
}

func TestAgreementCommittedNodeCrashed(t *testing.T) {
	nnodes := 3
	cfg := makeConfig(t, nnodes, false)
	defer cfg.cleanup()

	cfg.begin("Test (Agreement): agreement test where committed node crashed")
	cfg.crashOne(1)
	cfg.crashOne(2)

	time.Sleep(2 * sessionTimeout)

	_ = cfg.checkChain(0)

	cmd := 0
	iters := 30
	for i := 0; i < iters; i++ {
		cfg.one(cmd, 0, false, 0)
		cmd++
	}

	cfg.crashOne(0)
	cfg.startOne(1)
	cfg.startOne(2)

	time.Sleep(2 * sessionTimeout)

	cfg.checkCrash(1, 2)

	cfg.end()
}

func TestAgreementHeaderParitition(t *testing.T) {
	nnodes := 3
	cfg := makeConfig(t, nnodes, false)
	defer cfg.cleanup()

	cfg.begin("Test (Agreement): agreement test with header partition")

	time.Sleep(sessionTimeout)

	chain := cfg.checkChain(0, 1, 2)
	header1, node11, node12 := chain[0], chain[1], chain[2]

	cfg.enableZookeeper(header1, false)
	zkClient, err := makeZKClient(cfg.net, "config")
	if err != nil {
		cfg.t.Fatal(err)
	}
	zkConn, err := makeZKConn(zkClient)
	if err != nil {
		cfg.t.Fatal(err)
	}
	defer zkConn.Close()
	if err = zkConn.Delete(cfg.nodes[header1].chainPath+"/"+cfg.nodes[header1].name, 0); err != nil {
		cfg.t.Fatal(err)
	}

	time.Sleep(sessionTimeout)

	chain = cfg.checkChain(node11, node12)
	header2, node21 := chain[0], chain[1]

	cmd := 0
	iters := 3
	for i := 0; i < iters; i++ {
		cfg.nodes[header1].Start(cmd)
		cmd++
	}
	if nCommitted, _ := cfg.nCommitted(0, header1, header2, node21); nCommitted != 0 {
		cfg.t.Fatalf("新头节%s点应当丢弃原来的头节点%s发送的日志,但有%d个节点提交了第一个日志\n", cfg.addresses[header2], cfg.addresses[header1], nCommitted)
	}

	for i := 0; i < iters; i++ {
		cfg.one(cmd, header2, false, header2, node21)
		cmd++
	}

	cfg.end()
}

func TestAgreementTailParitition(t *testing.T) {
	nnodes := 3
	cfg := makeConfig(t, nnodes, false)
	defer cfg.cleanup()

	cfg.begin("Test (Agreement): agreement test with tail partition")
	cfg.crashOne(1)
	cfg.crashOne(2)

	time.Sleep(2 * sessionTimeout)

	_ = cfg.checkChain(0)

	cfg.enableZookeeper(0, false)
	zkClient, err := makeZKClient(cfg.net, "config")
	if err != nil {
		cfg.t.Fatal(err)
	}
	if conn, err := makeZKConn(zkClient); err != nil {
		cfg.t.Fatal(err)
	} else if err = conn.Delete(cfg.nodes[0].chainPath+"/"+cfg.nodes[0].name, 0); err != nil {
		cfg.t.Fatal(err)
		conn.Close()
	}
	cfg.startOne(1)
	cfg.startOne(2)

	time.Sleep(sessionTimeout)

	_ = cfg.checkChain(0)
	chain := cfg.checkChain(1, 2)

	cmd := 0
	iters := 3
	for i := 0; i < iters; i++ {
		cfg.nodes[chain[0]].Start(cmd)
		cmd++
	}
	if nCommitted, _ := cfg.nCommitted(cmd-1, 1, 2); nCommitted != 0 {
		cfg.t.Fatalf("原来的尾节点%s占用了锁,新尾节点不应提交日志,但有%d个节点提交了第一个日志\n", cfg.addresses[0], nCommitted)
	}

	time.Sleep(sessionTimeout)

	if nCommitted, _ := cfg.nCommitted(cmd-1, 1, 2); nCommitted != 2 {
		cfg.t.Fatalf("原来的尾节点%s释放了锁,新链表节点应当提交所有日志,但只有%d个节点提交所有日志\n", cfg.addresses[0], nCommitted)
	}

	cfg.end()
}

func TestAgreementConcurrentUnrealiable(t *testing.T) {
	nnodes := 3
	cfg := makeConfig(t, nnodes, true)
	defer cfg.cleanup()

	cfg.begin("Test (Agreement): concurrent agreement test (unrealiable)")
	time.Sleep(sessionTimeout)

	chain := cfg.checkChain(0, 1, 2)
	header, node1, node2 := chain[0], chain[1], chain[2]

	cmd := 0
	iters := 160
	done := make(chan bool)
	for i := 0; i < iters; i++ {
		go func(cmd int) {
			result := false
			defer func() { done <- result }()

			cfg.one(cmd, header, true, header, node1, node2)
			result = true
		}(cmd)
		cmd++
	}

	for i := 0; i < iters; i++ {
		if !<-done {
			cfg.t.Fatal("没能完成协议")
		}
	}

	cfg.end()
}

func TestPersistenceBasic(t *testing.T) {
	nnodes := 3
	cfg := makeConfig(t, nnodes, false)
	defer cfg.cleanup()

	cfg.begin("Test (Persistence): test basic persistence")
	time.Sleep(sessionTimeout)

	chain := cfg.checkChain(0, 1, 2)
	header1, node11, node12 := chain[0], chain[1], chain[2]

	cmd := 0
	iters := 10
	for i := 0; i < iters; i++ {
		cfg.one(cmd, header1, false, header1, node11, node12)
		cmd++
	}

	cfg.crashOne(0)
	cfg.crashOne(1)
	cfg.crashOne(2)

	time.Sleep(sessionTimeout)

	cfg.startOne(0)
	cfg.startOne(1)
	cfg.startOne(2)

	time.Sleep(sessionTimeout)

	if nCommitted, _ := cfg.nCommitted(cmd-1, 0, 1, 2); nCommitted != 3 {
		cfg.t.Fatalf("节点重启后应当保留提交的日志,但%d个节点中只有%d个节点保留所有提交的日志\n", 3, nCommitted)
	}

	cfg.end()
}

func TestPersistenceMore(t *testing.T) {
	nnodes := 5
	cfg := makeConfig(t, nnodes, false)
	defer cfg.cleanup()

	cfg.begin("Test (Persistence): test more persistence")

	cmd := 0
	iters := 3
	for i := 0; i < iters; i++ {
		time.Sleep(sessionTimeout)

		chain := cfg.checkChain(0, 1, 2, 3, 4)
		header1, node11, node12, node13, node14 := chain[0], chain[1], chain[2], chain[3], chain[4]
		cfg.one(cmd, header1, false, header1, node11, node12, node13, node14)
		cmd++

		cfg.crashOne(node11)
		time.Sleep(2 * sessionTimeout)

		chain = cfg.checkChain(header1, node12, node13, node14)
		header2, node21, node22, node23 := chain[0], chain[1], chain[2], chain[3]
		cfg.one(cmd, header2, false, header2, node21, node22, node23)
		cmd++

		cfg.crashOne(header2)
		cfg.startOne(node11)
		time.Sleep(2 * sessionTimeout)

		chain = cfg.checkChain(node11, node21, node22, node23)
		header3, node31, node32, node33 := chain[0], chain[1], chain[2], chain[3]
		cfg.one(cmd, header3, false, header3, node31, node32, node33)
		cmd++

		cfg.startOne(header2)
	}

	cfg.end()
}
func TestPersistenceUnreliable(t *testing.T) {
	nnodes := 3
	cfg := makeConfig(t, nnodes, true)
	defer cfg.cleanup()

	cfg.begin("Test (Persistence): test persistence (unreliable)")
	time.Sleep(sessionTimeout)

	chain := cfg.checkChain(0, 1, 2)
	header1, node11, node12 := chain[0], chain[1], chain[2]

	cmd := 0
	iters := 10
	for i := 0; i < iters; i++ {
		cfg.one(cmd, header1, false, header1, node11, node12)
		cmd++
	}

	cfg.crashOne(0)
	cfg.crashOne(1)
	cfg.crashOne(2)

	time.Sleep(2 * sessionTimeout)

	cfg.startOne(0)
	cfg.startOne(1)
	cfg.startOne(2)

	time.Sleep(sessionTimeout)

	chain = cfg.checkChain(0, 1, 2)
	header2, node21, node22 := chain[0], chain[1], chain[2]

	for i := 0; i < iters; i++ {
		cfg.one(cmd, header2, false, header2, node21, node22)
		cmd++
	}

	if nCommitted, _ := cfg.nCommitted(cmd-1, 0, 1, 2); nCommitted != 3 {
		cfg.t.Fatalf("节点重启后应当保留提交的日志,但%d个节点中只有%d个节点保留所有提交的日志\n", 3, nCommitted)
	}

	cfg.end()
}
