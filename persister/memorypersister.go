package persister

import "sync"

type MemoryPersister struct {
	mu        sync.Mutex
	nodestate []byte
	snapshot  []byte
}

func MakeMemoryPersister() *MemoryPersister {
	return &MemoryPersister{}
}

func (ps *MemoryPersister) Copy() Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakeMemoryPersister()
	np.nodestate = ps.nodestate
	np.snapshot = ps.snapshot
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

func (ps *MemoryPersister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.nodestate = state
	ps.snapshot = snapshot
}

func (ps *MemoryPersister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.snapshot
}

func (ps *MemoryPersister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
