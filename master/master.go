package master

import (
	"bytes"
	crand "crypto/rand"
	"encoding/base64"
	"encoding/gob"
	"log"
	"math"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MaxChainLength = 3

	Success = "success"

	ErrConnection = "connection err"

	RootImmutable = "/ immutable"

	InvalidPath = "invalid path"

	DirectoryExists    = "directory exists"
	DirectoryNotExists = "directory not exists"
	NotADirectory      = "not a directory"
	DirectoryNotEmpty  = "directory not empty"

	FileExists    = "file exists"
	FileNotExists = "file not exists"
	NotAFile      = "not a file"

	EmptyIndices = "empty indices"

	InvalidOffset  = "invalid offset"
	AppendTooLarge = "append too large"

	ChunkNotExists = "chunk not exists"
	OutOfChunk     = "out of chunk"
)

type FileOperationErr string

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

type ChainInfo struct {
	Name       string
	Servers    []string
	ChunkNames map[string]bool
}

type ChunkStore struct {
	mu          sync.Mutex
	ChunkInfos  map[string]string
	ServerInfos map[string]string
	ChainInfos  map[string]*ChainInfo
}

func makeChunkStore() *ChunkStore {
	cs := &ChunkStore{}
	cs.ChunkInfos = make(map[string]string)
	cs.ServerInfos = make(map[string]string)
	cs.ChainInfos = make(map[string]*ChainInfo)
	return cs
}

func (cs *ChunkStore) allocateChunk(chunkNames []string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	for _, chunkName := range chunkNames {
		// chunk只能分配一次
		if _, ok := cs.ChunkInfos[chunkName]; ok {
			log.Fatalf("chunk %s already allocated\n", chunkName)
		}

		// 找到chunk数最少的链表
		min := math.MaxInt
		var chainInfo *ChainInfo
		for cn, ci := range cs.ChainInfos {
			if len(ci.ChunkNames) < min {
				min = len(ci.ChunkNames)
				chainInfo = cs.ChainInfos[cn]
			}
		}

		// 更新chunkInfos和chainInfos
		cs.ChunkInfos[chunkName] = chainInfo.Name
		chainInfo.ChunkNames[chunkName] = true
	}
}

func (cs *ChunkStore) chunkServers(chunkNames []string) [][]string {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	chunkServers := make([][]string, len(chunkNames))
	for i, chunkName := range chunkNames {
		chainName := cs.ChunkInfos[chunkName]
		chunkServers[i] = cs.ChainInfos[chainName].Servers
	}

	return chunkServers
}

func (cs *ChunkStore) removeChunks(chunkNames []string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	for _, chunkName := range chunkNames {
		chainName := cs.ChunkInfos[chunkName]
		delete(cs.ChunkInfos, chunkName)
		delete(cs.ChainInfos[chainName].ChunkNames, chunkName)
	}
}

func (cs *ChunkStore) allocateServer(server string) string {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// 已分配server,将server调整到链表信息的最后并返回
	if chainName, ok := cs.ServerInfos[server]; ok {
		chainInfo := cs.ChainInfos[chainName]
		var i int
		for i = 0; i < len(chainInfo.Servers); i++ {
			if chainInfo.Servers[i] == server {
				break
			}
		}
		chainInfo.Servers = append(append(chainInfo.Servers[:i], chainInfo.Servers[i+1:]...), server)
		return chainName
	}

	min := math.MaxInt
	var mci *ChainInfo
	for _, ci := range cs.ChainInfos {
		size := len(ci.Servers)
		if size < MaxChainLength && size < min {
			min = size
			mci = ci
		}
	}

	if mci == nil {
		mci = &ChainInfo{
			Name:       "chain" + randstring(32),
			Servers:    []string{server},
			ChunkNames: make(map[string]bool),
		}
		cs.ServerInfos[server] = mci.Name
		cs.ChainInfos[mci.Name] = mci
	} else {
		mci.Servers = append(mci.Servers, server)
		cs.ServerInfos[server] = mci.Name
	}

	return mci.Name
}

func (cs *ChunkStore) rubbishChunks(chunkNames []string) []string {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	rubbishChunks := make([]string, 0)
	for _, chunkName := range chunkNames {
		if _, ok := cs.ChunkInfos[chunkName]; !ok {
			rubbishChunks = append(rubbishChunks, chunkName)
		}
	}

	return rubbishChunks
}

type MNode struct {
	mu          sync.RWMutex
	Name        string
	IsFile      bool
	ChunkNames  []string
	Children    map[string]*MNode
	Size        int
	LastUpdated time.Time
	LastDeleted time.Time
	CS          *ChunkStore
}

func makeMNode(name string, isFile bool, cs *ChunkStore) *MNode {
	mn := &MNode{}
	mn.Name = name
	mn.IsFile = isFile
	if isFile {
		mn.ChunkNames = make([]string, 0)
	} else {
		mn.Children = make(map[string]*MNode)
	}
	mn.LastUpdated = time.Now()
	mn.CS = cs
	return mn
}

func (mn *MNode) create(parents []string, paths []string, isFile bool) FileOperationErr {
	if len(paths) == 0 {
		return RootImmutable
	}

	if len(paths) > 1 {
		mn.mu.RLock()
		defer mn.mu.RUnlock()

		child, ok := mn.Children[paths[0]]
		if !ok {
			return DirectoryNotExists
		}
		return child.create(append(parents, paths[0]), paths[1:], isFile)
	}

	mn.mu.Lock()
	defer mn.mu.Unlock()

	if mn.IsFile {
		return NotADirectory
	}

	name := paths[0]
	if _, ok := mn.Children[name]; ok {
		if isFile {
			return FileExists
		} else {
			return DirectoryExists
		}
	}

	mn.Children[name] = makeMNode(name, isFile, mn.CS)
	if child, ok := mn.Children["_"+name]; ok {
		mn.deleteNode(child)
	}
	mn.LastUpdated = time.Now()

	return Success
}

func (mn *MNode) delete(parents []string, paths []string, isFile bool) FileOperationErr {
	if len(paths) == 0 {
		return RootImmutable
	}

	if len(paths) > 1 {
		mn.mu.RLock()
		defer mn.mu.RUnlock()

		child, ok := mn.Children[paths[0]]
		if !ok {
			return DirectoryNotExists
		}
		return child.delete(append(parents, paths[0]), paths[1:], isFile)
	}

	mn.mu.Lock()
	defer mn.mu.Unlock()

	name := paths[0]
	child, ok := mn.Children[name]

	if isFile {
		if !ok {
			return FileNotExists
		}

		if !child.IsFile {
			return NotAFile
		}
	} else {
		if !ok {
			return DirectoryNotExists
		}

		if child.IsFile {
			return NotADirectory
		}

		for name := range child.Children {
			if name[0] != '_' {
				return DirectoryNotEmpty
			}
		}
	}

	delete(mn.Children, name)
	child.Name = "_" + name
	child.LastDeleted = time.Now()
	mn.Children["_"+name] = child
	mn.LastUpdated = time.Now()

	return Success
}

func (mn *MNode) restore(parents []string, paths []string, isFile bool) FileOperationErr {
	if len(paths) == 0 {
		return RootImmutable
	}

	if len(paths) > 1 {
		mn.mu.RLock()
		defer mn.mu.RUnlock()

		child, ok := mn.Children[paths[0]]
		if !ok {
			return DirectoryNotExists
		}
		return child.restore(append(parents, paths[0]), paths[1:], isFile)
	}

	mn.mu.Lock()
	defer mn.mu.Unlock()

	name := paths[0]
	child, ok := mn.Children["_"+name]

	if isFile {
		if !ok {
			return FileNotExists
		}
	} else {
		if !ok {
			return DirectoryNotExists
		}
	}

	delete(mn.Children, "_"+name)
	child.Name = name
	mn.Children[name] = child
	mn.LastUpdated = time.Now()

	return Success
}

func runmove(src *MNode, srcParents []string, srcPaths []string, target *MNode, targetParents []string, targetPaths []string, lockInAdvance bool) FileOperationErr {
	if len(srcPaths) == 0 || len(targetPaths) == 0 {
		return RootImmutable
	}

	if len(srcPaths) > 1 && len(targetPaths) > 1 {
		src.mu.RLock()
		defer src.mu.RUnlock()

		target.mu.RLock()
		defer target.mu.RUnlock()

		srcChild, srcOk := src.Children[srcPaths[0]]
		targetChild, targetOk := target.Children[targetPaths[0]]
		if !srcOk || !targetOk {
			return DirectoryNotExists
		}
		return runmove(srcChild, append(srcParents, srcPaths[0]), srcPaths[1:], targetChild, append(targetParents, targetPaths[0]), targetPaths[1:], lockInAdvance)
	} else if len(srcPaths) > 1 {
		if !lockInAdvance {
			// 如果src和target路径重合,则提前获取写锁
			if len(srcParents) == len(targetParents) && reflect.DeepEqual(srcParents, targetParents) {
				target.mu.Lock()
				defer target.mu.Unlock()

				lockInAdvance = true
			} else {
				src.mu.RLock()
				defer src.mu.RUnlock()
			}
		}

		srcChild, srcOk := src.Children[srcPaths[0]]
		if !srcOk {
			return DirectoryNotExists
		}
		return runmove(srcChild, append(srcParents, srcPaths[0]), srcPaths[1:], target, targetParents, targetPaths, lockInAdvance)
	} else if len(targetPaths) > 1 {
		if !lockInAdvance {
			if len(srcParents) == len(targetParents) && reflect.DeepEqual(srcParents, targetParents) {
				src.mu.Lock()
				defer src.mu.Unlock()

				lockInAdvance = true
			} else {
				target.mu.RLock()
				defer target.mu.RUnlock()
			}
		}

		targetChild, targetOk := target.Children[targetPaths[0]]
		if !targetOk {
			return DirectoryNotExists
		}
		return runmove(src, srcParents, srcPaths, targetChild, append(targetParents, targetPaths[0]), targetPaths[1:], lockInAdvance)
	}

	if !lockInAdvance {
		src.mu.Lock()
		defer src.mu.Unlock()

		// 路径相同,只获取一次
		if src != target {
			target.mu.Lock()
			defer target.mu.Unlock()
		}
	}

	if src.IsFile || target.IsFile {
		return NotADirectory
	}

	srcChild, srcOk := src.Children[srcPaths[0]]
	if !srcOk {
		return FileNotExists
	}
	if _, targetOk := target.Children[targetPaths[0]]; targetOk {
		return FileExists
	}

	delete(src.Children, srcPaths[0])
	srcChild.Name = targetPaths[0]
	target.Children[targetPaths[0]] = srcChild
	if child, ok := target.Children["_"+targetPaths[0]]; ok {
		target.deleteNode(child)
	}
	src.LastUpdated = time.Now()
	target.LastUpdated = time.Now()

	return Success
}

func move(src *MNode, srcParents []string, srcPaths []string, target *MNode, targetParents []string, targetPaths []string) FileOperationErr {
	return runmove(src, srcParents, srcPaths, target, targetParents, targetPaths, false)
}

func (mn *MNode) list(parents []string, paths []string) ([]FileInfo, FileOperationErr) {

	if len(paths) > 0 {
		mn.mu.RLock()
		defer mn.mu.RUnlock()

		child, ok := mn.Children[paths[0]]
		if !ok {
			return nil, DirectoryNotExists
		}
		return child.list(append(parents, paths[0]), paths[1:])
	}

	mn.mu.Lock()
	defer mn.mu.Unlock()

	if mn.IsFile {
		return nil, NotADirectory
	}

	fileInfos := make([]FileInfo, 0)
	for _, child := range mn.Children {
		if child.Name[0] != '_' {
			fileInfo := FileInfo{
				Name:        child.Name,
				IsFile:      child.IsFile,
				Size:        child.Size,
				LastUpdated: child.LastUpdated,
			}
			fileInfos = append(fileInfos, fileInfo)
		}
	}

	return fileInfos, Success
}

func (mn *MNode) write(parents []string, paths []string, firstChunkIndex int, lastChunkIndex int) ([]string, [][]string, FileOperationErr) {
	if firstChunkIndex > lastChunkIndex {
		return nil, nil, ChunkNotExists
	}

	if len(paths) > 0 {
		mn.mu.RLock()
		defer mn.mu.RUnlock()

		child, ok := mn.Children[paths[0]]
		if !ok {
			return nil, nil, DirectoryNotExists
		}
		return child.write(append(parents, paths[0]), paths[1:], firstChunkIndex, lastChunkIndex)
	}

	mn.mu.RLock()
	defer mn.mu.RUnlock()

	if !mn.IsFile {
		return nil, nil, NotAFile
	}

	// 延长mn.chunks
	if lastChunkIndex >= len(mn.ChunkNames) {
		newEmptyChunks := make([]string, lastChunkIndex+1-len(mn.ChunkNames))
		mn.ChunkNames = append(mn.ChunkNames, newEmptyChunks...)
	}

	// 获取chunkNames
	newChunkNames := make([]string, 0)
	chunkNames := make([]string, lastChunkIndex-firstChunkIndex+1)
	for index := lastChunkIndex; index >= firstChunkIndex; index-- {
		// 创建chunk
		chunkName := mn.ChunkNames[index]
		if chunkName == "" {
			chunkName = randstring(64)
			mn.ChunkNames[index] = chunkName
			newChunkNames = append(newChunkNames, chunkName)
		}
		chunkNames[index-firstChunkIndex] = chunkName
	}

	mn.CS.allocateChunk(newChunkNames)
	chunkServers := mn.CS.chunkServers(chunkNames)

	return chunkNames, chunkServers, Success
}

func (mn *MNode) append(parents []string, paths []string, fullChunkIndex int) (int, string, []string, FileOperationErr) {
	if len(paths) > 0 {
		mn.mu.RLock()
		defer mn.mu.RUnlock()

		child, ok := mn.Children[paths[0]]
		if !ok {
			return -1, "", nil, DirectoryNotExists
		}
		return child.append(append(parents, paths[0]), paths[1:], fullChunkIndex)
	}

	mn.mu.RLock()
	defer mn.mu.RUnlock()

	if !mn.IsFile {
		return -1, "", nil, NotAFile
	}

	if fullChunkIndex >= 0 {
		if fullChunkIndex+1 >= len(mn.ChunkNames) {
			newEmptyChunks := make([]string, fullChunkIndex+2-len(mn.ChunkNames))
			mn.ChunkNames = append(mn.ChunkNames, newEmptyChunks...)
		}
		nextChunkName := mn.ChunkNames[fullChunkIndex+1]
		if nextChunkName == "" {
			nextChunkName = randstring(64)
			mn.ChunkNames[fullChunkIndex+1] = nextChunkName
			mn.CS.allocateChunk([]string{nextChunkName})
		}
		return fullChunkIndex + 1, nextChunkName, mn.CS.chunkServers([]string{nextChunkName})[0], Success
	} else {
		if len(mn.ChunkNames) == 0 {
			mn.ChunkNames = append(mn.ChunkNames, "")
		}
		lastChunkName := mn.ChunkNames[len(mn.ChunkNames)-1]
		if lastChunkName == "" {
			lastChunkName = randstring(64)
			mn.ChunkNames[len(mn.ChunkNames)-1] = lastChunkName
			mn.CS.allocateChunk([]string{lastChunkName})
		}
		return len(mn.ChunkNames) - 1, lastChunkName, mn.CS.chunkServers([]string{lastChunkName})[0], Success
	}
}

func (mn *MNode) ack(parents []string, paths []string, pointer int) FileOperationErr {
	if len(paths) > 0 {
		mn.mu.RLock()
		defer mn.mu.RUnlock()

		child, ok := mn.Children[paths[0]]
		if !ok {
			return DirectoryNotExists
		}
		return child.ack(append(parents, paths[0]), paths[1:], pointer)
	}

	mn.mu.Lock()
	defer mn.mu.Unlock()

	if !mn.IsFile {
		return NotAFile
	}

	if pointer > mn.Size {
		mn.Size = pointer
	}
	mn.LastUpdated = time.Now()

	return Success
}

func (mn *MNode) read(parents []string, paths []string, firstChunkIndex int, lastChunkIndex int) ([]string, [][]string, FileOperationErr) {
	if firstChunkIndex > lastChunkIndex {
		return nil, nil, ChunkNotExists
	}

	if len(paths) > 0 {
		mn.mu.RLock()
		defer mn.mu.RUnlock()

		child, ok := mn.Children[paths[0]]
		if !ok {
			return nil, nil, DirectoryNotExists
		}
		return child.read(append(parents, paths[0]), paths[1:], firstChunkIndex, lastChunkIndex)
	}

	mn.mu.RLock()
	defer mn.mu.RUnlock()

	if !mn.IsFile {
		return nil, nil, NotAFile
	}

	if lastChunkIndex >= len(mn.ChunkNames) {
		return nil, nil, ChunkNotExists
	}

	chunkNames := make([]string, lastChunkIndex-firstChunkIndex+1)
	for index := lastChunkIndex; index >= firstChunkIndex; index-- {
		if mn.ChunkNames[index] == "" {
			return nil, nil, ChunkNotExists
		}
		chunkNames[index-firstChunkIndex] = mn.ChunkNames[index]
	}

	return chunkNames, mn.CS.chunkServers(chunkNames), Success
}

func (mn *MNode) deleteNode(child *MNode) {
	children := make([]*MNode, 0)
	for _, c := range child.Children {
		children = append(children, c)
	}
	for _, c := range children {
		child.deleteNode(c)
	}

	delete(mn.Children, child.Name)
	mn.LastUpdated = time.Now()

	child.CS.removeChunks(child.ChunkNames)
}

func (mn *MNode) deleteExpiredNodes(expiredDuration time.Duration) {
	mn.mu.RLock()
	defer mn.mu.RUnlock()

	for _, child := range mn.Children {
		if child.Name[0] == '_' && time.Since(child.LastDeleted) >= expiredDuration {
			mn.mu.RUnlock()

			mn.mu.Lock()
			// 节点过期,则递归删除
			mn.deleteNode(child)
			mn.mu.Unlock()

			mn.mu.RLock()
		} else {
			// 查找子树中过期的节点并删除
			child.deleteExpiredNodes(expiredDuration)
		}
	}
}

type Master struct {
	persistLock sync.RWMutex
	cs          *ChunkStore
	root        *MNode // 命名空间根节点,表示/
	directory   string
	done        chan bool
	dead        int32
}

func MakeMaster(directory string, cleanupInterval time.Duration, expiredDuration time.Duration, doen chan bool) *Master {
	m := &Master{}
	cs := makeChunkStore()
	m.cs = cs
	m.root = makeMNode("", false, cs)
	m.directory = directory

	if directory != "" {
		os.MkdirAll(directory+"/master", 0775)
	}

	m.readPersist()

	go m.cleanup(cleanupInterval, expiredDuration)

	return m
}

func (m *Master) persist() {
	if m.directory == "" {
		return
	}

	m.persistLock.Lock()
	defer m.persistLock.Unlock()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	// persistLock可以确保没有其他线程持有任何m.root或m.cs中的锁
	e.Encode(*m.root)
	e.Encode(*m.cs)
	state := w.Bytes()

	if err := os.WriteFile(m.directory+"/master/state", state, 0664); err != nil {
		log.Fatal(err)
	}
}

func (m *Master) readPersist() {
	if m.directory == "" {
		return
	}

	m.persistLock.Lock()
	defer m.persistLock.Unlock()

	data, err := os.ReadFile(m.directory + "/master/state")
	if os.IsNotExist(err) {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var root MNode
	var cs ChunkStore
	if d.Decode(&root) != nil || d.Decode(&cs) != nil {
		log.Fatalln("decode error")
	}

	m.root = &root
	m.cs = &cs
}

type AllocateServerArgs struct {
	ChunkServer string
}

type AllocateServerReply struct {
	ChainName string
}

func (m *Master) AllocateServer(args *AllocateServerArgs, reply *AllocateServerReply) error {
	DPrintf("[master] recieves allocate server from %s\n", args.ChunkServer)

	m.persistLock.RLock()
	chainName := m.cs.allocateServer(args.ChunkServer)
	m.persistLock.RUnlock()
	reply.ChainName = chainName

	m.persist()

	return nil
}

func validatePath(path string) FileOperationErr {
	if path == "" {
		return InvalidPath
	}

	if path[0] != '/' {
		return InvalidPath
	}

	if path[len(path)-1] == '/' {
		return InvalidPath
	}

	offset := strings.LastIndexByte(path, '/') + 1
	if path[offset] == '_' {
		return InvalidPath
	}

	return Success
}

type CreateArgs struct {
	Path   string
	IsFile bool
}

type CreateReply struct {
	Err FileOperationErr
}

func (m *Master) Create(args *CreateArgs, reply *CreateReply) error {
	DPrintf("[master] recieves create from client, path: %s, isFile: %v\n", args.Path, args.IsFile)

	if err := validatePath(args.Path); err != Success {
		reply.Err = err
		return nil
	}

	paths := strings.Split(args.Path[1:], "/")
	m.persistLock.RLock()
	reply.Err = m.root.create([]string{}, paths, args.IsFile)
	m.persistLock.RUnlock()

	if reply.Err == Success {
		m.persist()
	}

	return nil
}

type DeleteArgs struct {
	Path   string
	IsFile bool
}

type DeleteReply struct {
	Err FileOperationErr
}

func (m *Master) Delete(args *DeleteArgs, reply *DeleteReply) error {
	DPrintf("[master] recieves delete from client, path: %s, isFile: %v\n", args.Path, args.IsFile)

	if err := validatePath(args.Path); err != Success {
		reply.Err = err
		return nil
	}

	paths := strings.Split(args.Path[1:], "/")
	m.persistLock.RLock()
	reply.Err = m.root.delete([]string{}, paths, args.IsFile)
	m.persistLock.RUnlock()

	if reply.Err == Success {
		m.persist()
	}

	return nil
}

type RestoreArgs struct {
	Path   string
	IsFile bool
}

type RestoreReply struct {
	Err FileOperationErr
}

func (m *Master) Restore(args *RestoreArgs, reply *RestoreReply) error {
	DPrintf("[master] recieves restore from client, path: %s, isFile: %v\n", args.Path, args.IsFile)

	if err := validatePath(args.Path); err != Success {
		reply.Err = err
		return nil
	}

	paths := strings.Split(args.Path[1:], "/")
	m.persistLock.RLock()
	reply.Err = m.root.restore([]string{}, paths, args.IsFile)
	m.persistLock.RUnlock()

	if reply.Err == Success {
		m.persist()
	}

	return nil
}

type MoveArgs struct {
	SrcPath    string
	TargetPath string
}

type MoveReply struct {
	Err FileOperationErr
}

func (m *Master) Move(args *MoveArgs, reply *MoveReply) error {
	DPrintf("[master] recieves move from client, src: %s, target: %v\n", args.SrcPath, args.TargetPath)

	if err := validatePath(args.SrcPath); err != Success {
		reply.Err = err
		return nil
	}
	if err := validatePath(args.TargetPath); err != Success {
		reply.Err = err
		return nil
	}

	srcPaths := strings.Split(args.SrcPath[1:], "/")
	targetPaths := strings.Split(args.TargetPath[1:], "/")
	m.persistLock.RLock()
	reply.Err = move(m.root, []string{}, srcPaths, m.root, []string{}, targetPaths)
	m.persistLock.RUnlock()

	if reply.Err == Success {
		m.persist()
	}

	return nil
}

type FileInfo struct {
	Name        string
	IsFile      bool
	Size        int
	LastUpdated time.Time
}

type ListArgs struct {
	Path string
}

type ListReply struct {
	FileInfos []FileInfo
	Err       FileOperationErr
}

func (m *Master) List(args *ListArgs, reply *ListReply) error {
	DPrintf("[master] recieves list from client, path: %s\n", args.Path)

	m.persistLock.RLock()
	defer m.persistLock.RUnlock()

	if args.Path == "/" {
		reply.FileInfos, reply.Err = m.root.list([]string{}, []string{})
	} else {
		if err := validatePath(args.Path); err != Success {
			reply.Err = err
			return nil
		}

		paths := strings.Split(args.Path[1:], "/")
		reply.FileInfos, reply.Err = m.root.list([]string{}, paths)
	}

	return nil
}

type WriteArgs struct {
	Path            string
	FirstChunkIndex int
	LastChunkIndex  int
}

type WriteReply struct {
	ChunkNames   []string
	ChunkServers [][]string
	Err          FileOperationErr
}

func (m *Master) Write(args *WriteArgs, reply *WriteReply) error {
	DPrintf("[master] recieves write from client, path: %s, first chunk index: %d, last chunk index: %d\n", args.Path, args.FirstChunkIndex, args.LastChunkIndex)

	if err := validatePath(args.Path); err != Success {
		reply.Err = err
		return nil
	}

	paths := strings.Split(args.Path[1:], "/")
	m.persistLock.RLock()
	reply.ChunkNames, reply.ChunkServers, reply.Err = m.root.write([]string{}, paths, args.FirstChunkIndex, args.LastChunkIndex)
	m.persistLock.RUnlock()

	if reply.Err == Success {
		m.persist()
	}

	return nil
}

type AppendArgs struct {
	Path           string
	FullChunkIndex int
}

type AppendReply struct {
	ChunkIndex   int
	ChunkName    string
	ChunkServers []string
	Err          FileOperationErr
}

func (m *Master) Append(args *AppendArgs, reply *AppendReply) error {
	DPrintf("[master] recieves append from client, path: %s, full chunk index: %d\n", args.Path, args.FullChunkIndex)

	if err := validatePath(args.Path); err != Success {
		reply.Err = err
		return nil
	}

	paths := strings.Split(args.Path[1:], "/")
	m.persistLock.RLock()
	reply.ChunkIndex, reply.ChunkName, reply.ChunkServers, reply.Err = m.root.append([]string{}, paths, args.FullChunkIndex)
	m.persistLock.RUnlock()

	if reply.Err == Success {
		m.persist()
	}

	return nil
}

type AckArgs struct {
	Path    string
	Pointer int
}

type AckReply struct {
	Err FileOperationErr
}

func (m *Master) Ack(args *AckArgs, reply *AckReply) error {
	DPrintf("[master] recieves ack from client, path: %s, pointer: %d\n", args.Path, args.Pointer)

	if err := validatePath(args.Path); err != Success {
		reply.Err = err
		return nil
	}

	paths := strings.Split(args.Path[1:], "/")
	m.persistLock.RLock()
	reply.Err = m.root.ack([]string{}, paths, args.Pointer)
	m.persistLock.RUnlock()

	if reply.Err == Success {
		m.persist()
	}

	return nil
}

type ReadArgs struct {
	Path            string
	FirstChunkIndex int
	LastChunkIndex  int
}

type ReadReply struct {
	ChunkNames   []string
	ChunkServers [][]string
	Err          FileOperationErr
}

func (m *Master) Read(args *ReadArgs, reply *ReadReply) error {
	DPrintf("[master] recieves read from client, path: %s, first chunk index: %d, last chunk index: %d\n", args.Path, args.FirstChunkIndex, args.LastChunkIndex)

	if err := validatePath(args.Path); err != Success {
		reply.Err = err
		return nil
	}

	paths := strings.Split(args.Path[1:], "/")
	m.persistLock.RLock()
	reply.ChunkNames, reply.ChunkServers, reply.Err = m.root.read([]string{}, paths, args.FirstChunkIndex, args.LastChunkIndex)
	m.persistLock.RUnlock()

	if reply.Err == Success {
		m.persist()
	}

	return nil
}

type RubbishChunksArgs struct {
	ChunkNames []string
}

type RubbishChunksReply struct {
	RubbishChunkNames []string
}

func (m *Master) RubbishChunks(args *RubbishChunksArgs, reply *RubbishChunksReply) error {
	DPrintf("[master] recieves query rubbish chunks from server, chunk names: %v\n", args.ChunkNames)

	m.persistLock.RLock()
	defer m.persistLock.RUnlock()

	reply.RubbishChunkNames = m.cs.rubbishChunks(args.ChunkNames)

	return nil
}

func (m *Master) cleanup(clearnupDuration time.Duration, expiredDuration time.Duration) {
	for {
		time.Sleep(clearnupDuration)

		if m.Killed() {
			break
		}

		m.persistLock.RLock()
		m.root.deleteExpiredNodes(expiredDuration)
		m.persistLock.RUnlock()

		m.persist()
	}
}

func (m *Master) Kill() {
	if atomic.CompareAndSwapInt32(&m.dead, 0, 1) {
		m.done <- true
	}
}

// 检查当前节点是否已被Kill,所有无限循环的函数每隔一段时间都应当检查该条件,如Node.watchChain
func (m *Master) Killed() bool {
	return atomic.LoadInt32(&m.dead) == 1
}
