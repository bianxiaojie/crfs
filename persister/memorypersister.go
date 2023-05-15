package persister

import (
	"bytes"
	"encoding/gob"
	"log"
	"sync"
)

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

	if offset+len(data) > MaxChunkSize {
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
		return nil, OutOfChunk
	}

	return input[offset : offset+size], Success
}

func (ps *MemoryPersister) DeleteChunk(chunkName string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	delete(ps.chunkMap, chunkName)
}

func (ps *MemoryPersister) ChunkNames() []string {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	chunkNames := make([]string, 0, len(ps.chunkMap))
	for k := range ps.chunkMap {
		chunkNames = append(chunkNames, k)
	}

	return chunkNames
}
