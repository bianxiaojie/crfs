package chunkserver

import (
	"container/list"
	"crfs/common"
	"crfs/master"
	"crfs/node"
	"crfs/persister"
	"crfs/rpc"
	izk "crfs/zk"
	"encoding/gob"
	"fmt"
	"log"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Type      string
	ChunkName string
	Offset    int
	Data      []byte
	Size      int
	ClerkId   int64
	RequestId int64
}

type Result struct {
	RequestId int64
	Offset    int
	Data      []byte
	Err       persister.Err
}

type ChunkServer struct {
	mu           sync.Mutex
	masterClient rpc.Client
	address      string
	applyCh      <-chan node.ApplyMsg
	Node         *node.Node
	persister    persister.Persister
	done         chan bool
	dead         bool

	lastApplied int
	processList map[string]*list.List
	resultMap   map[int64]Result
}

func MakeChunkServer(masterClient rpc.Client, zkClient izk.ZKClient, clients rpc.Clients, ip string, chainPath string, commitPath string, cleanupInterval time.Duration, persister persister.Persister, done chan bool) *ChunkServer {
	gob.Register(Op{})

	cs := &ChunkServer{}

	cs.masterClient = masterClient
	cs.address = fmt.Sprintf("%s:%d", ip, common.ChunkPort)
	applyCh := make(chan node.ApplyMsg)
	cs.applyCh = applyCh
	cs.persister = persister
	cs.done = done

	cs.lastApplied = -1
	cs.processList = make(map[string]*list.List)
	cs.resultMap = make(map[int64]Result)

	cs.persister.RestoreSnapshot()

	node := node.MakeNode(zkClient, clients, fmt.Sprintf("%s:%d", ip, common.NodePort), chainPath, commitPath, persister, applyCh)
	cs.Node = node

	go cs.kickOffApply()
	if masterClient != nil {
		go cs.kickOffCleanup(cleanupInterval)
	}

	return cs
}

type WriteArgs struct {
	ChunkName string // chunk文件名
	Offset    int    // 写入的起始地址
	Data      []byte // 写入的数据
	ClerkId   int64  // 客户端Id
	RequestId int64  // 客户端维护的请求Id
}

type WriteReply struct {
	Err persister.Err
}

func (cs *ChunkServer) Write(args *WriteArgs, reply *WriteReply) error {
	DPrintf("[server %s] receives write from %d, chunkName: %s, offset: %d, size: %d, requestId: %d\n",
		cs.address, args.ClerkId, args.ChunkName, args.Offset, len(args.Data), args.RequestId)

	// 如果超出chunk的范围,则返回
	if args.Offset < 0 || args.Offset+len(args.Data) > persister.MaxChunkSize {
		reply.Err = persister.OutOfChunk
		return nil
	}
	if len(args.Data) == 0 {
		reply.Err = persister.Success
		return nil
	}

	// 1.检查resultMap[ClerkId]返回结果中的RequestId是否等于Write操作的RequestId
	cs.mu.Lock()
	if result, ok := cs.resultMap[args.ClerkId]; ok && result.RequestId == args.RequestId {
		reply.Err = result.Err
		cs.mu.Unlock()
		return nil
	}
	cs.mu.Unlock()

	// 2.开始一致性协议
	_, ok := cs.Node.Start(Op{
		Type:      "Write",
		ChunkName: args.ChunkName,
		Offset:    args.Offset,
		Data:      args.Data,
		ClerkId:   args.ClerkId,
		RequestId: args.RequestId,
	})
	// 3.如果Start返回false,则直接返回persister.WrongHead错误,表明该服务器不是头节点,不能开始一致性协议
	if !ok {
		reply.Err = persister.WrongHead
		return nil
	}

	for {
		if cs.Killed() {
			cs.Kill()
			reply.Err = persister.Crash
			return nil
		}

		if !cs.Node.IsHead() {
			reply.Err = persister.WrongHead
			return nil
		}

		// 4.检查resultMap[ClerkId]返回结果中的RequestId是否等于Write操作的RequestId
		cs.mu.Lock()
		if result, ok := cs.resultMap[args.ClerkId]; ok && result.RequestId == args.RequestId {
			reply.Err = result.Err
			cs.mu.Unlock()
			return nil
		}
		cs.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

type AppendArgs struct {
	ChunkName string // chunk文件名
	Data      []byte // 追加的数据
	ClerkId   int64  // 客户端Id
	RequestId int64  // 客户端维护的请求Id
}

type AppendReply struct {
	Offset int
	Err    persister.Err
}

func (cs *ChunkServer) Append(args *AppendArgs, reply *AppendReply) error {
	DPrintf("[server %s] receives append from %d, chunkName: %s, size: %d, requestId: %d\n",
		cs.address, args.ClerkId, args.ChunkName, len(args.Data), args.RequestId)

	if len(args.Data) == 0 {
		reply.Err = persister.Success
		return nil
	}

	// 1.检查resultMap[ClerkId]返回结果中的RequestId是否等于Append操作的RequestId
	cs.mu.Lock()
	if result, ok := cs.resultMap[args.ClerkId]; ok && result.RequestId == args.RequestId {
		reply.Offset = result.Offset
		reply.Err = result.Err
		cs.mu.Unlock()
		return nil
	}
	cs.mu.Unlock()

	// 2.开始一致性协议
	_, ok := cs.Node.Start(Op{
		Type:      "Append",
		ChunkName: args.ChunkName,
		Data:      args.Data,
		ClerkId:   args.ClerkId,
		RequestId: args.RequestId,
	})
	// 3.如果Start返回false,则直接返回persister.WrongHead错误,表明该服务器不是头节点,不能开始一致性协议
	if !ok {
		reply.Err = persister.WrongHead
		return nil
	}

	for {
		if cs.Killed() {
			cs.Kill()
			reply.Err = persister.Crash
			return nil
		}

		if !cs.Node.IsHead() {
			reply.Err = persister.WrongHead
			return nil
		}

		// 4.检查resultMap[ClerkId]返回结果中的RequestId是否等于Write操作的RequestId
		cs.mu.Lock()
		if result, ok := cs.resultMap[args.ClerkId]; ok && result.RequestId == args.RequestId {
			reply.Offset = result.Offset
			reply.Err = result.Err
			cs.mu.Unlock()
			return nil
		}
		cs.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

type ReadArgs struct {
	ChunkName string // chunk文件名
	Offset    int    // 读取的起始地址
	Size      int    // 读取的长度
	ClerkId   int64  // 客户端Id
	RequestId int64  // 客户端维护的请求Id
}

type ReadReply struct {
	Data []byte
	Err  persister.Err
}

func (cs *ChunkServer) Read(args *ReadArgs, reply *ReadReply) error {
	DPrintf("[server %s] receives read from %d, chunkName: %s, offset: %d, size: %d, requestId: %d\n",
		cs.address, args.ClerkId, args.ChunkName, args.Offset, args.Size, args.RequestId)

	if args.Offset < 0 || args.Size < 0 || args.Offset+args.Size > persister.MaxChunkSize {
		reply.Err = persister.OutOfChunk
		return nil
	}
	if args.Size == 0 {
		reply.Data = make([]byte, 0)
		reply.Err = persister.Success
		return nil
	}

	// 1.检查resultMap[ClerkId]返回结果中的RequestId是否等于Get操作的RequestId
	cs.mu.Lock()
	if result, ok := cs.resultMap[args.ClerkId]; ok && result.RequestId == args.RequestId {
		reply.Data = result.Data
		reply.Err = result.Err
		cs.mu.Unlock()
		return nil
	}
	cs.mu.Unlock()

	var lastCommittedIndex int
	for {
		if cs.Killed() {
			cs.Kill()
			reply.Err = persister.Crash
			return nil
		}

		// 2.调用node.LastCommittedIndex获取当前zookeeper提交的日志索引,获取成功后赋值给lastCommittedIndex
		if committedIndex, ok := cs.Node.LastCommittedIndex(); ok {
			lastCommittedIndex = committedIndex
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	for {
		if cs.Killed() {
			cs.Kill()
			reply.Err = persister.Crash
			return nil
		}

		// 3.如果cs.lastApplied >= lastCommittedIndex,将Read操作添加到相应chunk的任务队列的末尾
		cs.mu.Lock()
		if cs.lastApplied >= lastCommittedIndex {
			op := Op{
				Type:      "Read",
				ChunkName: args.ChunkName,
				Offset:    args.Offset,
				Size:      args.Size,
				ClerkId:   args.ClerkId,
				RequestId: args.RequestId,
			}
			l, ok := cs.processList[op.ChunkName]
			if !ok {
				go cs.apply(op.ChunkName)
				cs.processList[op.ChunkName] = list.New()
				l = cs.processList[op.ChunkName]
			}
			l.PushBack(op)
			cs.mu.Unlock()
			break
		}
		cs.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}

	for {
		if cs.Killed() {
			cs.Kill()
			reply.Err = persister.Crash
			return nil
		}

		// 4.不断检查resultMap[ClerkId]返回结果中的RequestId是否等于Get操作的RequestId,如果等于,表明执行器执行完该请求,将该结果返回给客户端
		cs.mu.Lock()
		if result, ok := cs.resultMap[args.ClerkId]; ok && result.RequestId == args.RequestId {
			reply.Data = result.Data
			reply.Err = result.Err
			cs.mu.Unlock()
			return nil
		}
		cs.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

func (cs *ChunkServer) Delete(chunkName string) persister.Err {
	// 1.将delete操作封装成Op对象，调用node.Start(op)开始一致性协议。
	index, ok := cs.Node.Start(Op{
		Type:      "Delete",
		ChunkName: chunkName,
	})

	// 2.如果Start返回false,则直接返回persister.WrongHead错误,表明该服务器不是头节点,不能开始一致性协议
	if !ok {
		return persister.WrongHead
	}

	for {
		if cs.Killed() {
			cs.Kill()
			return persister.Crash
		}

		// 3.如果cs.lastApplied >= index,则返回persister.Success
		if cs.lastApplied >= index {
			return persister.Success
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (cs *ChunkServer) apply(chunkName string) {
	for {
		if cs.Killed() {
			cs.Kill()
			return
		}

		cs.mu.Lock()
		// 待处理的任务列表为空,跳出循环
		list := cs.processList[chunkName]
		if list == nil || list.Len() == 0 {
			delete(cs.processList, chunkName)
			cs.mu.Unlock()
			return
		}
		// 移除第一个节点
		front := list.Front()
		list.Remove(front)
		cs.mu.Unlock()

		// 1.调用persister相应的文件读写函数,执行完毕后更新resultMap[ClerkId]
		op := front.Value.(Op)
		switch op.Type {
		case "Write":
			DPrintf("[server %s] applies write: %v\n", cs.address, op)
			err := cs.persister.WriteChunk(op.ChunkName, op.Offset, op.Data)

			cs.mu.Lock()
			if cs.resultMap[op.ClerkId].RequestId >= op.RequestId {
				log.Fatalf("重复执行操作: %s, clerkId: %d, requestId: %d\n", op.Type, op.ClerkId, op.RequestId)
			}
			cs.resultMap[op.ClerkId] = Result{
				RequestId: op.RequestId,
				Err:       err,
			}
			cs.mu.Unlock()
		case "Append":
			DPrintf("[server %s] applies append: %v\n", cs.address, op)
			offset, err := cs.persister.AppendChunk(op.ChunkName, op.Data)

			cs.mu.Lock()
			if cs.resultMap[op.ClerkId].RequestId >= op.RequestId {
				log.Fatalf("重复执行操作: %s, clerkId: %d, requestId: %d\n", op.Type, op.ClerkId, op.RequestId)
			}
			cs.resultMap[op.ClerkId] = Result{
				RequestId: op.RequestId,
				Offset:    offset,
				Err:       err,
			}
			cs.mu.Unlock()
		case "Read":
			DPrintf("[server %s] applies read: %v\n", cs.address, op)
			data, err := cs.persister.ReadChunk(op.ChunkName, op.Offset, op.Size)

			cs.mu.Lock()
			if cs.resultMap[op.ClerkId].RequestId >= op.RequestId {
				log.Fatalf("重复执行操作: %s, clerkId: %d, requestId: %d\n", op.Type, op.ClerkId, op.RequestId)
			}
			cs.resultMap[op.ClerkId] = Result{
				RequestId: op.RequestId,
				Data:      data,
				Err:       err,
			}
			cs.mu.Unlock()
		case "Delete":
			DPrintf("[server %s] applies delete: %v\n", cs.address, op)
			cs.persister.DeleteChunk(op.ChunkName)
		default:
			log.Fatalf("未知的操作类型: %s\n", op.Type)
		}
	}
}

func (cs *ChunkServer) kickOffApply() {
	for {
		if cs.Killed() {
			cs.Kill()
			return
		}

		applyMsg := <-cs.applyCh
		op := applyMsg.Command.(Op)

		DPrintf("[server %s] receives applyMsg: %v from node\n", cs.address, applyMsg)

		cs.mu.Lock()
		if result, ok := cs.resultMap[op.ClerkId]; !ok || result.RequestId < op.RequestId {
			// 添加到任务队列
			l, ok := cs.processList[op.ChunkName]
			if !ok {
				go cs.apply(op.ChunkName)
				cs.processList[op.ChunkName] = list.New()
				l = cs.processList[op.ChunkName]
			}
			l.PushBack(op)

			DPrintf("[server %s] sends applyMsg: %v to process list\n", cs.address, applyMsg)
		}
		if applyMsg.CommandIndex > cs.lastApplied {
			cs.lastApplied = applyMsg.CommandIndex
		}
		cs.mu.Unlock()
	}
}

func (cs *ChunkServer) kickOffCleanup(cleanupInterval time.Duration) {
	for {
		time.Sleep(cleanupInterval)

		if cs.Killed() {
			cs.Kill()
			return
		}

		chunkNames := cs.persister.ChunkNames()
		var rubbishChunksReply master.RubbishChunksReply
		for {
			args := master.RubbishChunksArgs{
				ChunkNames: chunkNames,
			}
			var reply master.RubbishChunksReply

			if cs.masterClient.Call("Master.RubbishChunks", &args, &reply) {
				rubbishChunksReply = reply
				break
			}

			time.Sleep(100 * time.Millisecond)
		}

		for _, rubbishChunkName := range rubbishChunksReply.RubbishChunkNames {
			cs.Delete(rubbishChunkName)
		}
	}
}

func (cs *ChunkServer) Kill() {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.dead {
		return
	}

	cs.Node.Kill()

	cs.dead = true
	go func() {
		cs.done <- true
	}()
}

func (cs *ChunkServer) Killed() bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	return cs.dead || cs.Node.Killed()
}
