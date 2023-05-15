package client

import (
	"crfs/chunkserver"
	"crfs/master"
	"crfs/persister"
	"crfs/rpc"
	crand "crypto/rand"
	"log"
	"math/big"
	"time"
)

type Client struct {
	masterClient rpc.Client
	rpcClients   rpc.Clients
	id           int64
	requestid    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 60)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClient(masterClient rpc.Client, rpcClients rpc.Clients) *Client {
	c := &Client{}
	c.masterClient = masterClient
	c.rpcClients = rpcClients
	c.id = nrand()
	return c
}

func (c *Client) Create(path string, isFile bool) master.FileOperationErr {
	args := master.CreateArgs{
		Path:   path,
		IsFile: isFile,
	}
	var reply master.CreateReply

	if !c.masterClient.Call("Master.Create", &args, &reply) {
		return master.ErrConnection
	}

	return reply.Err
}

func (c *Client) Delete(path string, isFile bool) master.FileOperationErr {
	args := master.DeleteArgs{
		Path:   path,
		IsFile: isFile,
	}
	var reply master.DeleteReply

	if !c.masterClient.Call("Master.Delete", &args, &reply) {
		return master.ErrConnection
	}

	return reply.Err
}

func (c *Client) Restore(path string, isFile bool) master.FileOperationErr {
	args := master.RestoreArgs{
		Path:   path,
		IsFile: isFile,
	}
	var reply master.RestoreReply

	if !c.masterClient.Call("Master.Restore", &args, &reply) {
		return master.ErrConnection
	}

	return reply.Err
}

func (c *Client) Move(srcPath string, targetPath string) master.FileOperationErr {
	args := master.MoveArgs{
		SrcPath:    srcPath,
		TargetPath: targetPath,
	}
	var reply master.MoveReply

	if !c.masterClient.Call("Master.Move", &args, &reply) {
		return master.ErrConnection
	}

	return reply.Err
}

func (c *Client) List(path string) ([]master.FileInfo, master.FileOperationErr) {
	args := master.ListArgs{
		Path: path,
	}
	var reply master.ListReply

	if !c.masterClient.Call("Master.List", &args, &reply) {
		return nil, master.ErrConnection
	}

	if reply.Err != master.Success {
		return nil, reply.Err
	}
	return reply.FileInfos, master.Success
}

func (c *Client) Write(path string, offset int, data []byte) master.FileOperationErr {
	if offset < 0 {
		return master.InvalidOffset
	}

	// 1.根据offset和data的大小计算需要写入的chunk索引范围
	firstChunkIndex := offset / persister.MaxChunkSize
	lastChunkIndex := (offset + len(data) - 1) / persister.MaxChunkSize

	// 2.获取待写入的chunk names和chunk servers地址
	var writeReply master.WriteReply
	for {
		args := master.WriteArgs{
			Path:            path,
			FirstChunkIndex: firstChunkIndex,
			LastChunkIndex:  lastChunkIndex,
		}
		var reply master.WriteReply

		if c.masterClient.Call("Master.Write", &args, &reply) {
			writeReply = reply
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if writeReply.Err != master.Success {
		return writeReply.Err
	}

	// 3.按照chunk索引顺序将数据写入相应的chunk servers中
	for i, chunkName := range writeReply.ChunkNames {
		// 连接chunk servers
		chunkServers := writeReply.ChunkServers[i]
		chunkServerClients := make([]rpc.Client, len(chunkServers))
		for j, chunkServer := range chunkServers {
			chunkServerClient, err := c.rpcClients.MakeClient(chunkServer + ":7999")
			if err != nil {
				return master.ErrConnection
			}
			chunkServerClients[j] = chunkServerClient
		}

		c.requestid++

		// 计算write chunk的起始地址和数据
		var chunkOffset int
		if i == 0 {
			chunkOffset = offset - firstChunkIndex*persister.MaxChunkSize
		}

		var dataStart, dataEnd int
		if i > 0 {
			dataStart = (i-1)*persister.MaxChunkSize + (firstChunkIndex+1)*persister.MaxChunkSize - offset
		}
		if i == len(writeReply.ChunkNames)-1 {
			dataEnd = len(data)
		} else {
			dataEnd = i*persister.MaxChunkSize + (firstChunkIndex+1)*persister.MaxChunkSize - offset
		}

		chunkData := data[dataStart:dataEnd]

		j := 0
		for {
			args := chunkserver.WriteArgs{
				ChunkName: chunkName,
				Offset:    chunkOffset,
				Data:      chunkData,
				ClerkId:   c.id,
				RequestId: c.requestid,
			}
			var reply chunkserver.WriteReply

			if chunkServerClients[j].Call("ChunkServer.Write", &args, &reply) && reply.Err == persister.Success {
				break
			}

			if reply.Err == persister.OutOfChunk {
				log.Fatal(reply.Err)
			}

			j = (j + 1) % len(chunkServerClients)
			if j == 0 {
				time.Sleep(100 * time.Millisecond)
			}
		}

		for _, chunkServerClient := range chunkServerClients {
			chunkServerClient.Close()
		}
	}

	// 4.向master ack
	var ackReply master.AckReply
	for {
		args := master.AckArgs{
			Path:    path,
			Pointer: offset + len(data),
		}
		var reply master.AckReply

		if c.masterClient.Call("Master.Ack", &args, &reply) {
			ackReply = reply
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if ackReply.Err != master.Success {
		return ackReply.Err
	}

	// 5.返回执行成功
	return master.Success
}

func (c *Client) Append(path string, data []byte) (int, master.FileOperationErr) {
	if len(data) > persister.MaxChunkSize/3 {
		return -1, master.AppendTooLarge
	}

	offset := -1
	fullChunkIndex := -1
	ok := false
	for !ok {
		// 1.获取待追加的chunk name和chunk servers地址
		var appendReply master.AppendReply
		for {
			args := master.AppendArgs{
				Path:           path,
				FullChunkIndex: fullChunkIndex,
			}
			var reply master.AppendReply

			if c.masterClient.Call("Master.Append", &args, &reply) {
				appendReply = reply
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		if appendReply.Err != master.Success {
			return -1, appendReply.Err
		}

		chunkServerClients := make([]rpc.Client, len(appendReply.ChunkServers))
		for j, chunkServer := range appendReply.ChunkServers {
			chunkServerClient, err := c.rpcClients.MakeClient(chunkServer + ":7999")
			if err != nil {
				return -1, master.ErrConnection
			}
			chunkServerClients[j] = chunkServerClient
		}

		c.requestid++

		// 2.将数据追加到chunk servers中
		j := 0
		for {
			args := chunkserver.AppendArgs{
				ChunkName: appendReply.ChunkName,
				Data:      data,
				ClerkId:   c.id,
				RequestId: c.requestid,
			}
			var reply chunkserver.AppendReply

			// 3.如果chunk servers返回persister.OutOfChunk错误,更新fullChunkIndex = appendReply.ChunkIndex,并返回步骤1
			if chunkServerClients[j].Call("ChunkServer.Append", &args, &reply) {
				if reply.Err == persister.Success {
					offset = reply.Offset
					ok = true
					break
				} else if reply.Err == persister.OutOfChunk {
					fullChunkIndex = appendReply.ChunkIndex
					break
				}
			}

			j = (j + 1) % len(chunkServerClients)
			if j == 0 {
				time.Sleep(100 * time.Millisecond)
			}
		}

		for _, chunkServerClient := range chunkServerClients {
			chunkServerClient.Close()
		}
	}

	// 3.向master ack
	var ackReply master.AckReply
	for {
		args := master.AckArgs{
			Path:    path,
			Pointer: offset + len(data),
		}
		var reply master.AckReply

		if c.masterClient.Call("Master.Ack", &args, &reply) {
			ackReply = reply
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if ackReply.Err != master.Success {
		return -1, ackReply.Err
	}

	// 4.返回执行成功
	return offset, master.Success
}

func (c *Client) Read(path string, offset int, size int) ([]byte, master.FileOperationErr) {
	if offset < 0 {
		return nil, master.InvalidOffset
	}

	if size == 0 {
		return []byte{}, master.Success
	}

	// 1.根据offset和data的大小计算需要读取的chunk索引范围
	firstChunkIndex := offset / persister.MaxChunkSize
	lastChunkIndex := (offset + size - 1) / persister.MaxChunkSize

	// 2.获取待写入的chunk names和chunk servers地址
	var readReply master.ReadReply
	for {
		args := master.ReadArgs{
			Path:            path,
			FirstChunkIndex: firstChunkIndex,
			LastChunkIndex:  lastChunkIndex,
		}
		var reply master.ReadReply

		if c.masterClient.Call("Master.Read", &args, &reply) {
			readReply = reply
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if readReply.Err != master.Success {
		return nil, readReply.Err
	}

	// 3.按照chunk索引顺序依次读取数据
	result := make([]byte, size)
	for i, chunkName := range readReply.ChunkNames {
		// 连接chunk servers
		chunkServers := readReply.ChunkServers[i]
		chunkServerClients := make([]rpc.Client, len(chunkServers))
		for j, chunkServer := range chunkServers {
			chunkServerClient, err := c.rpcClients.MakeClient(chunkServer + ":7999")
			if err != nil {
				return nil, master.ErrConnection
			}
			chunkServerClients[j] = chunkServerClient
		}

		c.requestid++

		// 计算read chunk的起始地址和数据
		var chunkOffset int
		if i == 0 {
			chunkOffset = offset - firstChunkIndex*persister.MaxChunkSize
		}

		var dataStart, dataEnd int
		if i > 0 {
			dataStart = (i-1)*persister.MaxChunkSize + (firstChunkIndex+1)*persister.MaxChunkSize - offset
		}
		if i == len(readReply.ChunkNames)-1 {
			dataEnd = size
		} else {
			dataEnd = i*persister.MaxChunkSize + (firstChunkIndex+1)*persister.MaxChunkSize - offset
		}

		chunkSize := dataEnd - dataStart

		j := 0
		for {
			args := chunkserver.ReadArgs{
				ChunkName: chunkName,
				Offset:    chunkOffset,
				Size:      chunkSize,
				ClerkId:   c.id,
				RequestId: c.requestid,
			}
			var reply chunkserver.ReadReply

			if chunkServerClients[j].Call("ChunkServer.Read", &args, &reply) && reply.Err == persister.Success {
				for dataIndex := dataStart; dataIndex < dataEnd; dataIndex++ {
					result[dataIndex] = reply.Data[dataIndex-dataStart]
				}
				break
			}

			if reply.Err == persister.OutOfChunk {
				for _, chunkServerClient := range chunkServerClients {
					chunkServerClient.Close()
				}
				return nil, master.OutOfChunk
			}

			j = (j + 1) % len(chunkServerClients)
			if j == 0 {
				time.Sleep(100 * time.Millisecond)
			}
		}

		for _, chunkServerClient := range chunkServerClients {
			chunkServerClient.Close()
		}
	}

	// 4.返回执行结果
	return result, master.Success
}

func (client *Client) Close() {
	client.masterClient.Close()
}
