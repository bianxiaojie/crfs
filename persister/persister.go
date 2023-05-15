package persister

const (
	MaxChunkSize = 64 * 1024 * 1024
	Success      = "Success"
	Crash        = "Crash"
	WrongHead    = "WrongHead"
	OutOfChunk   = "OutOfChunk"
)

type Err string

type Persister interface {
	Copy() Persister
	SaveNodeState(state []byte)
	ReadNodeState() []byte
	NodeStateSize() int
	SaveStateAndSnapshot(state []byte)
	RestoreSnapshot()
	WriteChunk(chunkName string, offset int, data []byte) Err
	AppendChunk(chunkName string, data []byte) (int, Err)
	ReadChunk(chunkName string, offset int, size int) ([]byte, Err)
	DeleteChunk(chunkName string)
	ChunkNames() []string
}
