package persister

import (
	"bytes"
	"encoding/gob"
	"log"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type chunk struct {
	data    []byte
	version int
}

type MemoryPersister struct {
	mu        sync.Mutex
	nodestate []byte
	snapshot  []byte
	chunkMap  map[string]chunk
}

func MakeMemoryPersister() *MemoryPersister {
	ps := &MemoryPersister{}
	ps.chunkMap = make(map[string]chunk, 0)
	return ps
}

func (ps *MemoryPersister) Copy() Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakeMemoryPersister()
	np.nodestate = ps.nodestate
	np.snapshot = ps.snapshot
	np.chunkMap = ps.chunkMap
	return np
}

func (ps *MemoryPersister) SaveNodeState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.nodestate = state
}

func (ps *MemoryPersister) ReadNodeState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.nodestate
}

func (ps *MemoryPersister) NodeStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.nodestate)
}

func (ps *MemoryPersister) SaveStateAndSnapshot(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.nodestate = state

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(ps.chunkMap)
	ps.snapshot = w.Bytes()
}

func (ps *MemoryPersister) RestoreSnapshot() {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.snapshot == nil || len(ps.snapshot) == 0 {
		ps.chunkMap = make(map[string]chunk)
		return
	}

	var chunkMap map[string]chunk
	r := bytes.NewBuffer(ps.snapshot)
	d := gob.NewDecoder(r)
	if err := d.Decode(&chunkMap); err != nil {
		log.Fatal(err)
	}
	ps.chunkMap = chunkMap
}

func (ps *MemoryPersister) WriteChunk(chunkName string, offset int, data []byte) Err {
	ps.mu.Lock()
	chunk := ps.chunkMap[chunkName]
	chunkData, version := chunk.data, chunk.version
	ps.mu.Unlock()

	if offset < 0 || offset+len(data) > MaxChunkSize {
		DPrintf("write out of chunk, chunkName: %s, offset: %d, data size: %d, max size: %d\n", chunkName, offset, len(data), MaxChunkSize)
		return OutOfChunk
	}

	if offset > len(chunkData) {
		zeros := make([]byte, offset-len(chunkData))
		chunkData = append(chunkData, zeros...)
	}

	front := chunkData[:offset]
	var end []byte
	if offset+len(data) < len(chunkData) {
		end = chunkData[offset+len(data):]
	}
	chunkData = append(append(front, data...), end...)

	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.chunkMap[chunkName].version != version {
		log.Fatalf("不要并发修改同一个chunk: %s\n", chunkName)
	}
	chunk.data = chunkData
	chunk.version = version + 1
	ps.chunkMap[chunkName] = chunk

	return Success
}

func (ps *MemoryPersister) AppendChunk(chunkName string, data []byte) (int, Err) {
	ps.mu.Lock()
	chunk := ps.chunkMap[chunkName]
	chunkData, version := chunk.data, chunk.version
	offset := len(chunkData)
	ps.mu.Unlock()

	if offset < 0 || offset+len(data) > MaxChunkSize {
		DPrintf("append out of chunk, chunkName: %s, data size: %d, max size: %d\n", chunkName, len(data), MaxChunkSize)
		return -1, OutOfChunk
	}

	chunkData = append(chunkData, data...)

	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.chunkMap[chunkName].version != version {
		log.Fatalf("不要并发修改同一个chunk: %s\n", chunkName)
	}
	chunk.data = chunkData
	chunk.version = version + 1
	ps.chunkMap[chunkName] = chunk

	return offset, Success
}

func (ps *MemoryPersister) ReadChunk(chunkName string, offset int, size int) ([]byte, Err) {
	ps.mu.Lock()
	input := ps.chunkMap[chunkName].data
	ps.mu.Unlock()

	if offset < 0 || size < 0 || offset+size > len(input) {
		DPrintf("read out of chunk, chunkName: %s, offset: %d, size: %d, actual data: %d\n", chunkName, offset, size, len(input))
		return nil, OutOfChunk
	}

	return input[offset : offset+size], Success
}

func (ps *MemoryPersister) DeleteChunk(chunkName string) Err {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	delete(ps.chunkMap, chunkName)

	return Success
}
