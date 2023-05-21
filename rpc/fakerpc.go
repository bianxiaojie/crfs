package rpc

//
// 基于channel实现的RPC,主要用来测试,出处: mit 6.824实验的源代码
//
// 模拟一个网络,可以丢失请求和回复,可以延迟消息,可以与指定主机断开连接
//
//
// net := MakeNetwork() -- 创建网络,维护客户端,服务器,客户端与服务器之间的映射
// end := net.MakeEnd(endname) -- 创建一个客户端
// net.AddServer(servername, server) -- 添加一个服务器
// net.DeleteServer(servername) -- 移除一个服务器
// net.Connect(endname, servername) -- 将一个客户端连接到一个服务器上
// net.Enable(endname, enabled) -- 开启或禁用一个客户端
// net.Reliable(bool) -- 连接是否可靠,不可靠的连接可能会延迟或丢数据
//
// end.Call("Service.Method", &args, &reply) -- 发送rpc
// Call()返回true表明成功执行并回复
// Call()返回false表明服务器失效,网络丢包或者出现异常,比如函数句柄不存在
// reply必须是指针类型
//
// srv := MakeServer()
// srv.AddService(svc) -- 添加服务
//
// svc := MakeService(receiverObject) -- 创建服务,receiverObject是提供服务的对象
//

import (
	"bytes"
	"encoding/gob"
	"log"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var errorType = reflect.TypeOf((*error)(nil)).Elem()

type reqMsg struct {
	endname  interface{} // 发送消息的客户端名
	svcMeth  string      // RPC调用名称, "Raft.AppendEntries"
	argsType reflect.Type
	args     []byte
	replyCh  chan replyMsg
}

type replyMsg struct {
	ok    bool
	reply []byte
}

type ClientEnd struct {
	endname interface{}   // 客户端名
	ch      chan reqMsg   // 用于发送消息的chan
	done    chan struct{} // 用于指示客户端网络已终止
}

// 发送rpc并等待回复, 返回值为true表示成功
func (e *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	req := reqMsg{}
	req.endname = e.endname
	req.svcMeth = svcMeth
	req.argsType = reflect.TypeOf(args)
	// 无需显示关闭,垃圾收集器会自动回收
	req.replyCh = make(chan replyMsg)

	qb := new(bytes.Buffer)
	qe := gob.NewEncoder(qb)
	qe.Encode(args)
	req.args = qb.Bytes()

	// 发送rpc
	select {
	case e.ch <- req:
		// 发送成功
	case <-e.done:
		// 网络终止
		return false
	}

	// 等待回复
	rep := <-req.replyCh
	if rep.ok {
		rb := bytes.NewBuffer(rep.reply)
		rd := gob.NewDecoder(rb)
		if err := rd.Decode(reply); err != nil {
			log.Fatalf("ClientEnd.Call(): decode error: %v\n", err)
		}
		return true
	} else {
		return false
	}
}

type Network struct {
	mu             sync.Mutex
	reliable       bool                        // 网络不稳定,会有延迟,可能会在发送或者接收的过程中返回错误
	longDelays     bool                        // 连接不可用时,延迟很长时间
	longReordering bool                        // 有时延长回复的时间
	ends           map[interface{}]*ClientEnd  // 客户端
	enabled        map[interface{}]bool        // 是否开启/禁用
	servers        map[interface{}]*Server     // 服务器
	connections    map[interface{}]interface{} // 客户端与服务器之间的映射
	endCh          chan reqMsg
	done           chan struct{} // 网络终止时调用close(done)终止长运行的goroutine
	count          int32         // rpc的数量,只在发送时统计,无论服务器是否接收到
	bytes          int64         // rpc发送的字节总数
}

func MakeNetwork() *Network {
	rn := &Network{}
	rn.reliable = true
	rn.ends = map[interface{}]*ClientEnd{}
	rn.enabled = map[interface{}]bool{}
	rn.servers = map[interface{}]*Server{}
	rn.connections = map[interface{}](interface{}){}
	rn.endCh = make(chan reqMsg)
	rn.done = make(chan struct{})

	// 处理ClientEnd.Call()
	go func() {
		for {
			select {
			case xreq := <-rn.endCh:
				// 统计rpc的数量和发送的字节总数
				atomic.AddInt32(&rn.count, 1)
				atomic.AddInt64(&rn.bytes, int64(len(xreq.args)))
				// 启动goroutine处理请求
				go rn.processReq(xreq)
			case <-rn.done:
				return
			}
		}
	}()

	return rn
}

func (rn *Network) Cleanup() {
	close(rn.done)
}

func (rn *Network) Reliable(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.reliable = yes
}

func (rn *Network) LongReordering(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.longReordering = yes
}

func (rn *Network) LongDelays(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.longDelays = yes
}

func (rn *Network) readEndnameInfo(endname interface{}) (enabled bool,
	servername interface{}, server *Server, reliable bool, longreordering bool,
) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	enabled = rn.enabled[endname]
	servername = rn.connections[endname]
	if servername != nil {
		server = rn.servers[servername]
	}
	reliable = rn.reliable
	longreordering = rn.longReordering
	return
}

func (rn *Network) isServerDead(endname interface{}, servername interface{}, server *Server) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// 如果客户端被禁用,或者服务器已被替换,则说明服务器死亡
	if !rn.enabled[endname] || rn.servers[servername] != server {
		return true
	}
	return false
}

func (rn *Network) processReq(req reqMsg) {
	enabled, servername, server, reliable, longreordering := rn.readEndnameInfo(req.endname)

	if enabled && servername != nil && server != nil {
		if !reliable {
			// 网络不稳定,首先延迟一小段时间
			ms := (rand.Int() % 27)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if !reliable && (rand.Int()%1000) < 100 {
			// 网络不稳定,10%的概率返回错误
			req.replyCh <- replyMsg{false, nil}
			return
		}

		// 在单独的goroutine中执行rpc句柄,
		// 当前goroutine在等待结果的过程中检查server是否已经死亡
		ech := make(chan replyMsg)
		go func() {
			// 执行句柄方法
			r := server.dispatch(req)
			ech <- r
		}()

		// 等待执行结果,如果检查到server死亡,则提前返回错误
		var reply replyMsg
		replyOK := false
		serverDead := false
		for !replyOK && !serverDead {
			select {
			case reply = <-ech:
				replyOK = true
			case <-time.After(100 * time.Millisecond):
				serverDead = rn.isServerDead(req.endname, servername, server)
				if serverDead {
					go func() {
						<-ech // 避免执行句柄的goroutine无限等待
					}()
				}
			}
		}

		serverDead = rn.isServerDead(req.endname, servername, server)

		if !replyOK || serverDead {
			req.replyCh <- replyMsg{false, nil}
		} else if !reliable && (rand.Int()%1000) < 100 {
			// 网络不稳定,10%的概率返回错误
			req.replyCh <- replyMsg{false, nil}
		} else if longreordering && rand.Intn(900) < 600 {
			// 延迟回复
			ms := 200 + rand.Intn(1+rand.Intn(2000))
			time.AfterFunc(time.Duration(ms)*time.Millisecond, func() {
				// rpc回复的字节总数
				atomic.AddInt64(&rn.bytes, int64(len(reply.reply)))
				req.replyCh <- reply
			})
		} else {
			// 正常回复
			atomic.AddInt64(&rn.bytes, int64(len(reply.reply)))
			req.replyCh <- reply
		}
	} else {
		// 服务器没有正常运行
		ms := 0
		if rn.longDelays {
			ms = (rand.Int() % 7000)
		} else {
			ms = (rand.Int() % 100)
		}
		time.AfterFunc(time.Duration(ms)*time.Millisecond, func() {
			req.replyCh <- replyMsg{false, nil}
		})
	}

}

// 获取客户端
func (rn *Network) GetEnd(endname interface{}) (*ClientEnd, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	e, ok := rn.ends[endname]

	return e, ok
}

// 创建客户端
func (rn *Network) MakeEnd(endname interface{}) *ClientEnd {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if _, ok := rn.ends[endname]; ok {
		log.Fatalf("MakeEnd: %v already exists\n", endname)
	}

	e := &ClientEnd{}
	e.endname = endname
	e.ch = rn.endCh
	e.done = rn.done
	rn.ends[endname] = e
	rn.enabled[endname] = false
	rn.connections[endname] = nil

	return e
}

func (rn *Network) AddServer(servername interface{}, rs *Server) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.servers[servername] = rs
}

func (rn *Network) DeleteServer(servername interface{}) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.servers[servername] = nil
}

// 这里仅仅是建立了客户端与服务器之间的映射,只有客户端Enable才能正常通信
func (rn *Network) Connect(endname interface{}, servername interface{}) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.connections[endname] = servername
}

// 启用或禁用客户端
func (rn *Network) Enable(endname interface{}, enabled bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.enabled[endname] = enabled
}

// 获取一个服务器收到的rpc数量
func (rn *Network) GetCount(servername interface{}) int {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	svr := rn.servers[servername]
	return svr.GetCount()
}

func (rn *Network) GetTotalCount() int {
	x := atomic.LoadInt32(&rn.count)
	return int(x)
}

func (rn *Network) GetTotalBytes() int64 {
	x := atomic.LoadInt64(&rn.bytes)
	return x
}

// server是service的集合,所有service共享同一个rpc分发器,
// 使得不同的service共同监听同一个rpc端口
type Server struct {
	mu       sync.Mutex
	services map[string]*Service
	count    int // 只有当Server收到rpc才统计
}

func MakeServer() *Server {
	rs := &Server{}
	rs.services = map[string]*Service{}
	return rs
}

func (rs *Server) AddService(svc *Service) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.services[svc.name] = svc
}

func (rs *Server) dispatch(req reqMsg) replyMsg {
	rs.mu.Lock()

	rs.count += 1

	// 将rpc句柄分为service和method两部分
	dot := strings.LastIndex(req.svcMeth, ".")
	serviceName := req.svcMeth[:dot]
	methodName := req.svcMeth[dot+1:]

	service, ok := rs.services[serviceName]

	rs.mu.Unlock()

	if ok {
		// 分发给service中具体的方法
		return service.dispatch(methodName, req)
	} else {
		choices := []string{}
		for k := range rs.services {
			choices = append(choices, k)
		}
		log.Fatalf("fakerpc.Server.dispatch(): unknown service %v in %v.%v; expecting one of %v\n",
			serviceName, serviceName, methodName, choices)
		return replyMsg{false, nil}
	}
}

func (rs *Server) GetCount() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.count
}

// 方法的集合,一个server可以有多个service
type Service struct {
	name    string
	rcvr    reflect.Value
	typ     reflect.Type
	methods map[string]reflect.Method
}

func MakeService(rcvr interface{}) *Service {
	svc := &Service{}
	svc.typ = reflect.TypeOf(rcvr)
	svc.rcvr = reflect.ValueOf(rcvr)
	svc.name = reflect.Indirect(svc.rcvr).Type().Name()
	svc.methods = map[string]reflect.Method{}

	for m := 0; m < svc.typ.NumMethod(); m++ {
		method := svc.typ.Method(m)
		mtype := method.Type
		mname := method.Name

		if method.PkgPath != "" || // 如果方法名小写开头
			mtype.NumIn() != 3 || // 方法的参数个数不等于3
			mtype.In(2).Kind() != reflect.Ptr || // 方法的第3个参数不是指针类型
			mtype.NumOut() != 1 ||
			!mtype.Out(0).AssignableTo(errorType) { // 返回值的个数不等于1
		} else {
			// 可能的handler
			svc.methods[mname] = method
		}
	}

	return svc
}

func (svc *Service) dispatch(methname string, req reqMsg) replyMsg {
	if method, ok := svc.methods[methname]; ok {
		// 反射创建一个指向argsType类型的变量指针
		args := reflect.New(req.argsType)

		ab := bytes.NewBuffer(req.args)
		ad := gob.NewDecoder(ab)
		ad.Decode(args.Interface()) // 也可以用ad.DecodeValue(args)

		replyType := method.Type.In(2)
		// 已经检查过replyType是指针类型,获取实际的类型
		replyType = replyType.Elem()
		replyv := reflect.New(replyType)

		function := method.Func
		function.Call([]reflect.Value{svc.rcvr, args.Elem(), replyv})

		// 对回复进行编码
		rb := new(bytes.Buffer)
		re := gob.NewEncoder(rb)
		re.EncodeValue(replyv)

		return replyMsg{true, rb.Bytes()}
	} else {
		choices := []string{}
		for k := range svc.methods {
			choices = append(choices, k)
		}
		log.Fatalf("fakerpc.Service.dispatch(): unknown method %v in %v; expecting one of %v\n",
			methname, req.svcMeth, choices)
		return replyMsg{false, nil}
	}
}
