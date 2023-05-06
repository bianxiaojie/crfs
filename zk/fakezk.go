package zk

import (
	"crfs/rpc"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-zookeeper/zk"
)

const (
	HeartBeatInterval = 100 * time.Millisecond

	success                 = "success"
	connectionClosed        = "connectionClosed"
	noNode                  = "noNode"
	nodeExists              = "nodeExists"
	notEmpty                = "notEmpty"
	badVersion              = "badVersion"
	noChildrenForEphemerals = "noChildrenForEphemerals"
	invalidPath             = "invalidPath"
)

var errMap = map[err]error{
	noNode:                  zk.ErrNoNode,
	nodeExists:              zk.ErrNodeExists,
	notEmpty:                zk.ErrNotEmpty,
	badVersion:              zk.ErrBadVersion,
	noChildrenForEphemerals: zk.ErrNoChildrenForEphemerals,
	invalidPath:             zk.ErrInvalidPath,
}

type err string

type FakeZKConn struct {
	mu             sync.Mutex
	client         rpc.Client
	name           string
	t              time.Time
	sessionTimeout time.Duration
	closed         bool
	watchers       map[string]chan zk.Event
}

func (fzkc *FakeZKConn) close() {
	if fzkc.closed {
		return
	}

	for _, watcher := range fzkc.watchers {
		go func(watcher chan zk.Event) {
			watcher <- zk.Event{}
		}(watcher)
	}
	fzkc.watchers = nil
	fzkc.closed = true
}

func (fzkc *FakeZKConn) Close() {
	fzkc.mu.Lock()
	if fzkc.closed {
		fzkc.mu.Unlock()
		return
	}
	args := CloseArgs{ConnName: fzkc.name}
	var reply CloseReply
	fzkc.mu.Unlock()

	fzkc.client.Call("FakeZKServer.Close", &args, &reply)

	fzkc.mu.Lock()
	defer fzkc.mu.Unlock()

	fzkc.close()
}

func (fzkc *FakeZKConn) Closed() bool {
	fzkc.mu.Lock()
	defer fzkc.mu.Unlock()

	return fzkc.closed
}

func (fzkc *FakeZKConn) Create(path string, data []byte, flags int32) (string, error) {
	fzkc.mu.Lock()
	if fzkc.closed {
		fzkc.mu.Unlock()
		return "", zk.ErrConnectionClosed
	}
	args := CreateArgs{
		ConnName: fzkc.name,
		Path:     path,
		Data:     data,
		Flags:    flags,
	}
	var reply CreateReply
	fzkc.mu.Unlock()

	if !fzkc.client.Call("FakeZKServer.Create", &args, &reply) {
		return "", zk.ErrNoServer
	}

	fzkc.mu.Lock()
	defer fzkc.mu.Unlock()

	if reply.Err == connectionClosed {
		fzkc.close()
	}
	if fzkc.closed {
		return "", zk.ErrConnectionClosed
	}

	if reply.Err != success {
		return "", errMap[reply.Err]
	}

	fzkc.t = time.Now()

	return reply.Name, nil
}

func (fzkc *FakeZKConn) Delete(path string, version int32) error {
	fzkc.mu.Lock()
	if fzkc.closed {
		fzkc.mu.Unlock()
		return zk.ErrConnectionClosed
	}
	args := DeleteArgs{
		ConnName: fzkc.name,
		Path:     path,
		Version:  version,
	}
	var reply DeleteReply
	fzkc.mu.Unlock()

	if !fzkc.client.Call("FakeZKServer.Delete", &args, &reply) {
		return zk.ErrNoServer
	}

	fzkc.mu.Lock()
	defer fzkc.mu.Unlock()

	if reply.Err == connectionClosed {
		fzkc.close()
	}
	if fzkc.closed {
		return zk.ErrConnectionClosed
	}

	fzkc.t = time.Now()

	return errMap[reply.Err]
}

func (fzkc *FakeZKConn) Exists(path string, watch bool) (bool, int32, <-chan zk.Event, error) {
	fzkc.mu.Lock()
	if fzkc.closed {
		fzkc.mu.Unlock()
		return false, 0, nil, zk.ErrConnectionClosed
	}
	args := ExistsArgs{
		ConnName: fzkc.name,
		Path:     path,
		Watch:    watch,
	}
	var reply ExistsReply
	fzkc.mu.Unlock()

	if !fzkc.client.Call("FakeZKServer.Exists", &args, &reply) {
		return false, 0, nil, zk.ErrNoServer
	}

	fzkc.mu.Lock()
	defer fzkc.mu.Unlock()

	if reply.Err == connectionClosed {
		fzkc.close()
	}
	if fzkc.closed {
		return false, 0, nil, zk.ErrConnectionClosed
	}

	if reply.Err != success {
		return false, 0, nil, errMap[reply.Err]
	}

	fzkc.t = time.Now()

	eventCh := make(chan zk.Event)
	fzkc.watchers[reply.WatcherName] = eventCh

	return reply.OK, reply.Version, eventCh, nil
}

func (fzkc *FakeZKConn) Get(path string, watch bool) ([]byte, int32, <-chan zk.Event, error) {
	fzkc.mu.Lock()
	if fzkc.closed {
		fzkc.mu.Unlock()
		return nil, 0, nil, zk.ErrConnectionClosed
	}
	args := GetArgs{
		ConnName: fzkc.name,
		Path:     path,
		Watch:    watch,
	}
	var reply GetReply
	fzkc.mu.Unlock()

	if !fzkc.client.Call("FakeZKServer.Get", &args, &reply) {
		return nil, 0, nil, zk.ErrNoServer
	}

	fzkc.mu.Lock()
	defer fzkc.mu.Unlock()

	if reply.Err == connectionClosed {
		fzkc.close()
	}
	if fzkc.closed {
		return nil, 0, nil, zk.ErrConnectionClosed
	}

	if reply.Err != success {
		return nil, 0, nil, errMap[reply.Err]
	}

	fzkc.t = time.Now()

	eventCh := make(chan zk.Event)
	fzkc.watchers[reply.WatcherName] = eventCh

	return reply.Data, reply.Version, eventCh, nil
}

func (fzkc *FakeZKConn) Set(path string, data []byte, version int32) (int32, error) {
	fzkc.mu.Lock()
	if fzkc.closed {
		fzkc.mu.Unlock()
		return 0, zk.ErrConnectionClosed
	}
	args := SetArgs{
		ConnName: fzkc.name,
		Path:     path,
		Data:     data,
		Version:  version,
	}
	var reply SetReply
	fzkc.mu.Unlock()

	if !fzkc.client.Call("FakeZKServer.Set", &args, &reply) {
		return 0, zk.ErrNoServer
	}

	fzkc.mu.Lock()
	defer fzkc.mu.Unlock()

	if reply.Err == connectionClosed {
		fzkc.close()
	}
	if fzkc.closed {
		return 0, zk.ErrConnectionClosed
	}

	if reply.Err != success {
		return 0, errMap[reply.Err]
	}

	fzkc.t = time.Now()

	return reply.Version, nil
}

func (fzkc *FakeZKConn) Children(path string, watch bool) ([]string, int32, <-chan zk.Event, error) {
	fzkc.mu.Lock()
	if fzkc.closed {
		fzkc.mu.Unlock()
		return nil, 0, nil, zk.ErrConnectionClosed
	}
	args := ChildrenArgs{
		ConnName: fzkc.name,
		Path:     path,
		Watch:    watch,
	}
	var reply ChildrenReply
	fzkc.mu.Unlock()

	if !fzkc.client.Call("FakeZKServer.Children", &args, &reply) {
		return nil, 0, nil, zk.ErrNoServer
	}

	fzkc.mu.Lock()
	defer fzkc.mu.Unlock()

	if reply.Err == connectionClosed {
		fzkc.close()
	}
	if fzkc.closed {
		return nil, 0, nil, zk.ErrConnectionClosed
	}

	if reply.Err != success {
		return nil, 0, nil, errMap[reply.Err]
	}

	fzkc.t = time.Now()

	eventCh := make(chan zk.Event)
	fzkc.watchers[reply.WatcherName] = eventCh

	return reply.Children, reply.Version, eventCh, nil
}

func (fzkc *FakeZKConn) Sync(path string) (string, error) {
	fzkc.mu.Lock()
	if fzkc.closed {
		fzkc.mu.Unlock()
		return "", zk.ErrConnectionClosed
	}
	args := SyncArgs{
		ConnName: fzkc.name,
		Path:     path,
	}
	var reply SyncReply
	fzkc.mu.Unlock()

	if !fzkc.client.Call("FakeZKServer.Sync", &args, &reply) {
		return "", zk.ErrNoServer
	}

	fzkc.mu.Lock()
	defer fzkc.mu.Unlock()

	if reply.Err == connectionClosed {
		fzkc.close()
	}
	if fzkc.closed {
		return "", zk.ErrConnectionClosed
	}

	fzkc.t = time.Now()

	return reply.Path, errMap[reply.Err]
}

func (fzkc *FakeZKConn) kickOffHeartBeat() {
	triggerAck := false

	for {
		fzkc.mu.Lock()
		if time.Since(fzkc.t) >= fzkc.sessionTimeout {
			args := CloseArgs{ConnName: fzkc.name}
			var reply CloseReply
			fzkc.mu.Unlock()

			fzkc.client.Call("FakeZKServer.Close", &args, &reply)

			fzkc.mu.Lock()
			fzkc.close()
		}
		if fzkc.closed {
			fzkc.mu.Unlock()
			break
		}
		args := SendHeartBeatArgs{
			ConnName:   fzkc.name,
			TriggerAck: triggerAck,
		}
		var reply SendHeartBeatReply
		fzkc.mu.Unlock()

		triggerAck = false

		if fzkc.client.Call("FakeZKServer.SendHeartBeat", &args, &reply) {
			fzkc.mu.Lock()

			if reply.Err == connectionClosed {
				fzkc.close()
			}
			if fzkc.closed {
				fzkc.mu.Unlock()
				break
			}

			if reply.Err == success {
				fzkc.t = time.Now()

				for _, watcherName := range reply.WatcherNames {
					if watcher, ok := fzkc.watchers[watcherName]; ok {
						go func(watcher chan zk.Event) {
							watcher <- zk.Event{}
						}(watcher)
						delete(fzkc.watchers, watcherName)
					}
				}

				triggerAck = true
			}

			fzkc.mu.Unlock()
		}

		time.Sleep(HeartBeatInterval)
	}
}

type FakeZKClient struct {
	client         rpc.Client
	servers        []string
	sessionTimeout time.Duration
}

func MakeFakeZKClient(client rpc.Client, servers []string, sessionTimeout time.Duration) *FakeZKClient {
	fzkc := &FakeZKClient{}
	fzkc.client = client
	fzkc.servers = servers
	fzkc.sessionTimeout = sessionTimeout
	return fzkc
}

func (fzkc *FakeZKClient) Connect() (ZKConn, error) {
	args := ConnectArgs{Servers: fzkc.servers, SessionTimeout: fzkc.sessionTimeout}
	var reply ConnectReply

	if !fzkc.client.Call("FakeZKServer.Connect", &args, &reply) {
		return nil, zk.ErrNoServer
	}

	conn := FakeZKConn{}
	conn.client = fzkc.client
	conn.name = reply.ConnName
	conn.t = time.Now()
	conn.sessionTimeout = fzkc.sessionTimeout
	conn.watchers = make(map[string]chan zk.Event)

	go conn.kickOffHeartBeat()

	return &conn, nil
}

type watcher struct {
	name     string
	path     string
	connName string
}

type watcherStore struct {
	mu                  sync.Mutex
	untriggeredWatchers map[string][]watcher // 未触发的watcher,key是路径
	triggeredWatchers   map[string][]string  // 触发的watcher,key是conn的name
}

func makeWatcherStore() *watcherStore {
	watcherStore := &watcherStore{}
	watcherStore.untriggeredWatchers = make(map[string][]watcher)
	watcherStore.triggeredWatchers = make(map[string][]string)
	return watcherStore
}

func (ws *watcherStore) addConn(connName string) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	ws.triggeredWatchers[connName] = make([]string, 0)
}

func (ws *watcherStore) removeConn(connName string) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	delete(ws.triggeredWatchers, connName)
}

func (ws *watcherStore) addUntriggered(watcher watcher) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	ws.untriggeredWatchers[watcher.path] = append(ws.untriggeredWatchers[watcher.path], watcher)
}

func (ws *watcherStore) trigger(paths ...string) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	watchers := make([]watcher, 0)
	for _, path := range paths {
		watchers = append(watchers, ws.untriggeredWatchers[path]...)
		delete(ws.untriggeredWatchers, path)
	}
	for _, watcher := range watchers {
		if triggeredWatchers, ok := ws.triggeredWatchers[watcher.connName]; ok {
			ws.triggeredWatchers[watcher.connName] = append(triggeredWatchers, watcher.name)
		}
	}
}

func (ws *watcherStore) getTriggered(connName string) []string {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	triggeredWatchers, ok := ws.triggeredWatchers[connName]
	if ok {
		ws.triggeredWatchers[connName] = make([]string, 0)
	}

	return triggeredWatchers
}

type znode struct {
	mu       sync.RWMutex
	connName string
	name     string
	children map[string]*znode
	data     []byte
	flags    int32
	version  int32
	counters map[string]uint64
	ws       *watcherStore
}

func makeZNode(connName string, name string, data []byte, flags int32, ws *watcherStore) *znode {
	zn := znode{}
	zn.connName = connName
	zn.name = name
	zn.children = make(map[string]*znode)
	zn.data = data
	zn.flags = flags
	zn.counters = make(map[string]uint64)
	zn.ws = ws
	return &zn
}

func (zn *znode) Create(connName string, parents []string, paths []string, data []byte, flags int32) (string, err) {
	if len(paths) == 0 {
		return "", invalidPath
	}

	if len(paths) > 1 {
		zn.mu.RLock()
		defer zn.mu.RUnlock()

		child, ok := zn.children[paths[0]]
		if !ok {
			return "", noNode
		}
		return child.Create(connName, append(parents, paths[0]), paths[1:], data, flags)
	}

	if zn.flags&zk.FlagEphemeral == zk.FlagEphemeral {
		return "", noChildrenForEphemerals
	}

	zn.mu.Lock()
	defer zn.mu.Unlock()

	name := paths[0]
	if flags&zk.FlagSequence == zk.FlagSequence {
		zn.counters[name]++
		name = fmt.Sprintf("%s%d", name, zn.counters[name])
	}
	if _, ok := zn.children[name]; ok {
		return "", nodeExists
	}

	child := makeZNode(connName, name, data, flags, zn.ws)
	zn.children[name] = child
	zn.version++

	zn.ws.trigger("/" + strings.Join(parents, "/"))

	return name, success
}

func (zn *znode) Delete(parents []string, paths []string, version int32) err {
	if len(paths) == 0 {
		return invalidPath
	}

	if len(paths) > 1 {
		zn.mu.RLock()
		defer zn.mu.RUnlock()

		child, ok := zn.children[paths[0]]
		if !ok {
			return noNode
		}
		return child.Delete(append(parents, paths[0]), paths[1:], version)
	}

	zn.mu.Lock()
	defer zn.mu.Unlock()

	name := paths[0]
	child, ok := zn.children[name]
	if !ok {
		return noNode
	}

	if version != child.version {
		return badVersion
	}

	if len(child.children) > 0 {
		return notEmpty
	}

	delete(zn.children, name)
	zn.version++

	directory := "/" + strings.Join(parents, "/")
	file := directory + "/" + name
	zn.ws.trigger(directory, file)

	return success
}

func (zn *znode) Exists(paths []string) (bool, int32) {
	if len(paths) > 0 {
		zn.mu.RLock()
		defer zn.mu.RUnlock()

		child, ok := zn.children[paths[0]]
		if !ok {
			return false, 0
		}
		return child.Exists(paths[1:])
	}

	zn.mu.RLock()
	defer zn.mu.RUnlock()

	return true, zn.version
}

func (zn *znode) Get(paths []string) ([]byte, int32, err) {
	if len(paths) > 0 {
		zn.mu.RLock()
		defer zn.mu.RUnlock()

		child, ok := zn.children[paths[0]]
		if !ok {
			return nil, 0, noNode
		}
		return child.Get(paths[1:])
	}

	zn.mu.RLock()
	defer zn.mu.RUnlock()

	// 这里直接返回data是安全的,因为write总是替换data
	return zn.data, zn.version, success
}

func (zn *znode) Set(parents []string, paths []string, data []byte, version int32) (int32, err) {
	if len(paths) > 0 {
		zn.mu.RLock()
		defer zn.mu.RUnlock()

		child, ok := zn.children[paths[0]]
		if !ok {
			return 0, noNode
		}
		return child.Set(append(parents, paths[0]), paths[1:], data, version)
	}

	zn.mu.Lock()
	defer zn.mu.Unlock()

	if version != zn.version {
		return 0, badVersion
	}

	zn.data = data
	zn.version++

	zn.ws.trigger("/" + strings.Join(parents, "/"))

	return zn.version, success
}

func (zn *znode) Children(paths []string) ([]string, int32, err) {
	if len(paths) > 0 {
		zn.mu.RLock()
		defer zn.mu.RUnlock()

		child, ok := zn.children[paths[0]]
		if !ok {
			return nil, 0, noNode
		}
		return child.Children(paths[1:])
	}

	zn.mu.RLock()
	defer zn.mu.RUnlock()

	names := make([]string, 0, len(zn.children))
	for name := range zn.children {
		names = append(names, name)
	}

	return names, zn.version, success
}

func (zn *znode) Sync(paths []string) (string, err) {
	if len(paths) > 0 {
		zn.mu.RLock()
		defer zn.mu.RUnlock()

		child, ok := zn.children[paths[0]]
		if !ok {
			return "", noNode
		}
		return child.Sync(paths[1:])
	}

	zn.mu.RLock()
	defer zn.mu.RUnlock()

	return zn.name, success
}

func (zn *znode) deleteRecursive(parents []string, name string) {
	child := zn.children[name]
	for c := range child.children {
		zn.deleteRecursive(append(parents, name), c)
	}
	delete(zn.children, name)

	directory := "/" + strings.Join(parents, "/")
	file := directory + "/" + name
	zn.ws.trigger(directory, file)
}

// 删除临时节点及所有子节点
func (zn *znode) deleteEphemeral(connName string, parents []string, paths []string) {
	if len(paths) == 0 {
		return
	}

	if len(paths) > 1 {
		zn.mu.RLock()
		defer zn.mu.RUnlock()

		child, ok := zn.children[paths[0]]
		if !ok {
			return
		}
		child.deleteEphemeral(connName, append(parents, paths[0]), paths[1:])
		return
	}

	// 获取临时节点的父节点的写锁,在删除临时节点及所有子节点的过程中无需加锁
	zn.mu.Lock()
	defer zn.mu.Unlock()

	name := paths[0]
	// 如果临时节点已被删除,或被重新创建,则不能删除该临时节点
	if child, ok := zn.children[name]; !ok || child.connName != connName || child.flags&zk.FlagEphemeral != zk.FlagEphemeral {
		return
	}
	zn.deleteRecursive(parents, name)
}

type conn struct {
	name           string
	t              time.Time
	sessionTimeout time.Duration
}

type FakeZKServer struct {
	mu                sync.Mutex
	ws                *watcherStore
	root              *znode
	conns             map[string]*conn
	connsZNodes       map[string][]string // 每个连接创建的临时节点的列表,连接断开时移除
	triggeredWatchers map[string][]string
	dead              int32
}

func MakeFakeZKServer() *FakeZKServer {
	fzks := &FakeZKServer{}
	ws := makeWatcherStore()
	fzks.ws = ws
	fzks.root = makeZNode("", "", nil, 0, ws)
	fzks.conns = make(map[string]*conn)
	fzks.connsZNodes = make(map[string][]string)
	fzks.triggeredWatchers = make(map[string][]string)

	go fzks.kickOffCleanup()

	return fzks
}

func randstring(n int) string {
	b := make([]byte, 2*n)
	rand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func validatePath(path string, isSequential bool) err {
	if path == "" {
		return invalidPath
	}

	if path[0] != '/' {
		return invalidPath
	}

	n := len(path)
	if n == 1 {
		return success
	}

	if !isSequential && path[n-1] == '/' {
		return invalidPath
	}

	return success
}

type CloseArgs struct {
	ConnName string
}

type CloseReply struct {
	Err err
}

func (fzks *FakeZKServer) close(connName string) ([]string, bool) {
	_, ok := fzks.conns[connName]
	if !ok {
		return nil, false
	}

	delete(fzks.conns, connName)
	znodes := fzks.connsZNodes[connName]
	delete(fzks.connsZNodes, connName)
	fzks.ws.removeConn(connName)
	delete(fzks.triggeredWatchers, connName)

	return znodes, true
}

func (fzks *FakeZKServer) Close(args *CloseArgs, reply *CloseReply) {
	fzks.mu.Lock()
	znodes, ok := fzks.close(args.ConnName)
	fzks.mu.Unlock()

	if !ok {
		reply.Err = connectionClosed
		return
	}

	for _, znode := range znodes {
		fzks.root.deleteEphemeral(args.ConnName, []string{}, strings.Split(znode[1:], "/"))
	}
	reply.Err = success
}

type CreateArgs struct {
	ConnName string
	Path     string
	Data     []byte
	Flags    int32
}

type CreateReply struct {
	Name string
	Err  err
}

func (fzks *FakeZKServer) RefreshConn(connName string) bool {
	fzks.mu.Lock()
	conn, ok := fzks.conns[connName]
	if !ok {
		fzks.mu.Unlock()
		return false
	}
	if time.Since(conn.t) >= conn.sessionTimeout {
		znodes, _ := fzks.close(connName)
		fzks.mu.Unlock()

		for _, znode := range znodes {
			fzks.root.deleteEphemeral(connName, []string{}, strings.Split(znode[1:], "/"))
		}
		return false
	}
	conn.t = time.Now()
	fzks.mu.Unlock()
	return true
}

func (fzks *FakeZKServer) Create(args *CreateArgs, reply *CreateReply) {
	if !fzks.RefreshConn(args.ConnName) {
		reply.Err = connectionClosed
		return
	}

	if err := validatePath(args.Path, args.Flags&zk.FlagSequence == zk.FlagSequence); err != success {
		reply.Err = err
		return
	}
	paths := strings.Split(args.Path[1:], "/")
	name, err := fzks.root.Create(args.ConnName, []string{}, paths, args.Data, args.Flags)
	if err != success {
		reply.Err = err
		return
	}

	if args.Flags&zk.FlagEphemeral == zk.FlagEphemeral {
		fzks.mu.Lock()
		if znodes, ok := fzks.connsZNodes[args.ConnName]; ok {
			fzks.connsZNodes[args.ConnName] = append(znodes, args.Path[:strings.LastIndexByte(args.Path, '/')+1]+name)
		}
		fzks.mu.Unlock()
	}

	reply.Name = name
	reply.Err = success
}

type DeleteArgs struct {
	ConnName string
	Path     string
	Version  int32
}

type DeleteReply struct {
	Err err
}

func (fzks *FakeZKServer) Delete(args *DeleteArgs, reply *DeleteReply) {
	if !fzks.RefreshConn(args.ConnName) {
		reply.Err = connectionClosed
		return
	}

	if err := validatePath(args.Path, false); err != success {
		reply.Err = err
		return
	}
	paths := strings.Split(args.Path[1:], "/")
	err := fzks.root.Delete([]string{}, paths, args.Version)

	reply.Err = err
}

type ExistsArgs struct {
	ConnName string
	Path     string
	Watch    bool
}

type ExistsReply struct {
	OK          bool
	Version     int32
	WatcherName string
	Err         err
}

func (fzks *FakeZKServer) Exists(args *ExistsArgs, reply *ExistsReply) {
	if !fzks.RefreshConn(args.ConnName) {
		reply.Err = connectionClosed
		return
	}

	if err := validatePath(args.Path, false); err != success {
		reply.Err = err
		return
	}
	paths := strings.Split(args.Path[1:], "/")
	ok, version := fzks.root.Exists(paths)

	watcherName := ""
	if args.Watch {
		watcherName = randstring(16)
		watcher := watcher{
			name:     watcherName,
			path:     args.Path,
			connName: args.ConnName,
		}
		fzks.ws.addUntriggered(watcher)
	}

	reply.OK = ok
	reply.Version = version
	reply.WatcherName = watcherName
	reply.Err = success
}

type GetArgs struct {
	ConnName string
	Path     string
	Watch    bool
}

type GetReply struct {
	Data        []byte
	Version     int32
	WatcherName string
	Err         err
}

func (fzks *FakeZKServer) Get(args *GetArgs, reply *GetReply) {
	if !fzks.RefreshConn(args.ConnName) {
		reply.Err = connectionClosed
		return
	}

	if err := validatePath(args.Path, false); err != success {
		reply.Err = err
		return
	}
	paths := strings.Split(args.Path[1:], "/")
	data, version, err := fzks.root.Get(paths)
	if err != success {
		reply.Err = err
		return
	}

	watcherName := ""
	if args.Watch {
		watcherName = randstring(16)
		watcher := watcher{
			name:     watcherName,
			path:     args.Path,
			connName: args.ConnName,
		}
		fzks.ws.addUntriggered(watcher)
	}

	reply.Data = data
	reply.Version = version
	reply.WatcherName = watcherName
	reply.Err = success
}

type SetArgs struct {
	ConnName string
	Path     string
	Data     []byte
	Version  int32
}

type SetReply struct {
	Version int32
	Err     err
}

func (fzks *FakeZKServer) Set(args *SetArgs, reply *SetReply) {
	if !fzks.RefreshConn(args.ConnName) {
		reply.Err = connectionClosed
		return
	}

	if err := validatePath(args.Path, false); err != success {
		reply.Err = err
		return
	}
	paths := strings.Split(args.Path[1:], "/")
	version, err := fzks.root.Set([]string{}, paths, args.Data, args.Version)
	if err != success {
		reply.Err = err
		return
	}

	reply.Version = version
	reply.Err = err
}

type ChildrenArgs struct {
	ConnName string
	Path     string
	Watch    bool
}

type ChildrenReply struct {
	Children    []string
	Version     int32
	WatcherName string
	Err         err
}

func (fzks *FakeZKServer) Children(args *ChildrenArgs, reply *ChildrenReply) {
	if !fzks.RefreshConn(args.ConnName) {
		reply.Err = connectionClosed
		return
	}

	if err := validatePath(args.Path, false); err != success {
		reply.Err = err
		return
	}
	paths := strings.Split(args.Path[1:], "/")
	children, version, err := fzks.root.Children(paths)
	if err != success {
		reply.Err = err
		return
	}

	watcherName := ""
	if args.Watch {
		watcherName = randstring(16)
		watcher := watcher{
			name:     watcherName,
			path:     args.Path,
			connName: args.ConnName,
		}
		fzks.ws.addUntriggered(watcher)
	}

	reply.Children = children
	reply.Version = version
	reply.WatcherName = watcherName
	reply.Err = err
}

type SyncArgs struct {
	ConnName string
	Path     string
}

type SyncReply struct {
	Path string
	Err  err
}

func (fzks *FakeZKServer) Sync(args *SyncArgs, reply *SyncReply) {
	if !fzks.RefreshConn(args.ConnName) {
		reply.Err = connectionClosed
		return
	}

	if err := validatePath(args.Path, false); err != success {
		reply.Err = err
		return
	}
	paths := strings.Split(args.Path[1:], "/")
	path, err := fzks.root.Sync(paths)

	reply.Path = path
	reply.Err = err
}

type SendHeartBeatArgs struct {
	ConnName   string
	TriggerAck bool
}

type SendHeartBeatReply struct {
	WatcherNames []string
	Err          err
}

func (fzks *FakeZKServer) SendHeartBeat(args *SendHeartBeatArgs, reply *SendHeartBeatReply) {
	if !fzks.RefreshConn(args.ConnName) {
		reply.Err = connectionClosed
		return
	}

	fzks.mu.Lock()
	defer fzks.mu.Unlock()

	if args.TriggerAck {
		delete(fzks.triggeredWatchers, args.ConnName)
	}

	watcherNames, ok := fzks.triggeredWatchers[args.ConnName]
	if !ok {
		fzks.triggeredWatchers[args.ConnName] = fzks.ws.getTriggered(args.ConnName)
		watcherNames = fzks.triggeredWatchers[args.ConnName]
	}

	reply.WatcherNames = watcherNames
	reply.Err = success
}

type ConnectArgs struct {
	Servers        []string
	SessionTimeout time.Duration
}

type ConnectReply struct {
	ConnName string
	Err      err
}

func (fzks *FakeZKServer) Connect(args *ConnectArgs, reply *ConnectReply) {
	fzks.mu.Lock()
	defer fzks.mu.Unlock()

	connName := randstring(16)
	fzks.conns[connName] = &conn{name: connName, t: time.Now(), sessionTimeout: args.SessionTimeout}
	fzks.connsZNodes[connName] = make([]string, 0)
	fzks.ws.addConn(connName)

	reply.ConnName = connName
	reply.Err = success
}

func (fzks *FakeZKServer) Kill() {
	atomic.StoreInt32(&fzks.dead, 1)
}

func (fzks *FakeZKServer) Killed() bool {
	return atomic.LoadInt32(&fzks.dead) == 1
}

func (fzks *FakeZKServer) kickOffCleanup() {
	for {
		if fzks.Killed() {
			return
		}

		fzks.mu.Lock()
		for _, conn := range fzks.conns {
			if time.Since(conn.t) >= conn.sessionTimeout {
				znodes, _ := fzks.close(conn.name)
				fzks.mu.Unlock()

				for _, znode := range znodes {
					fzks.root.deleteEphemeral(conn.name, []string{}, strings.Split(znode[1:], "/"))
				}
				fzks.mu.Lock()
			}
		}
		fzks.mu.Unlock()

		time.Sleep(HeartBeatInterval)
	}
}
