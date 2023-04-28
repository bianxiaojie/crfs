package persister

type Persister interface {
	Copy() Persister
	SaveNodeState(state []byte)
	ReadNodeState() []byte
	NodeStateSize() int
	SaveStateAndSnapshot(state []byte, snapshot []byte)
	ReadSnapshot() []byte
	SnapshotSize() int
}
