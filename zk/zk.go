package zk

import (
	"github.com/go-zookeeper/zk"
)

// 代表一个zookeeper连接,可以向zookeeper服务端发送文件读写命令,连接一旦关闭就无法使用,应当重新建立新的连接
type ZKConn interface {
	// 关闭连接,与连接绑定的所有的监听器都将提前返回
	Close()

	// 连接是否已关闭
	Closed() bool

	// 在zookeeper服务器创建一个文件
	// 例子(1): name, err := Create("/a/b", []byte("Hello"), zk.FlagEphemeral)
	// 例子(2): name, err := Create("/a/b", []byte("Hello"), zk.FlagSequence)
	//
	// 参数
	// path: 文件路径,必须以/开头,不能递归创建文件,在本例中,如果没有创建/a,则创建/a/b会返回zk.ErrNoNode错误
	// data: 文件内容,在本例中会将Hello写入文件
	// flags: 文件类型,可选值包括:
	//      zk.FlagEphemeral: 临时文件,在例子(1)中,当ZKConn关闭时,/a/b会被zookeeper服务器自动删除
	//      zk.FlagSequence: 序列文件,会在path后添加一个递增的正整数,在例子(2)中,即为/a/b1(第二次创建的文件是/a/b2,第三次是/a/b3)
	//      zk.FlagEphemeral|zk.FlagSequence: 两者属性的结合
	//
	// 返回值
	// string: 创建的文件名,如果没有指定zk.FlagSequence,则文件名就是path指定的文件名,比如/a/b的文件名是b
	// 如果指定zk.FlagSequence,文件名就是添加后缀后的文件名,在本例中,即为/a/b1
	// error: 函数执行是否出错,err == nil表示执行成功,err != nil表示执行失败
	Create(path string, data []byte, flags int32) (string, error)

	// 在zookeeper服务器删除一个文件
	// 例子: err := Delete("/a/b", 1)
	//
	// 参数
	// path: 文件路径,只能删除没有子文件的文件,在本例中,必须先删除/a/b,才能删除/a,否则会返回zk.ErrNotEmpty错误
	// version: 指定待删除文件的版本号,如果与文件实际的版本号相同,才能删除文件,否则会返回zk.ErrBadVersion错误
	// 举一个删除失败的例子:
	// 连接A   读取文件/a/b,获得版本号为1                               用版本号1删除文件/a/b,由于实际的版本号为2,所以删除失败
	// 连接B                               修改文件/a/b,版本号更新为2
	// 因此,如果因为版本号错误而删除文件失败,应该重新读取版本号,再重新尝试删除文件
	//
	// 返回值
	// error: 函数执行是否出错
	Delete(path string, version int32) error

	// 检查一个文件是否存在
	// 例子(1): ok, version, _, err := Exists("/a/b", false)
	// 例子(2): ok, version, watcher, err := Exists("/a/b", true)
	//
	// 参数
	// path: 文件路径
	// watch: 是否监听文件的变化,false表示不监听,true表示监听
	//
	// 返回值
	// bool: 文件是否存在
	// int32: 文件的版本号,用于删除文件或者更新文件时指定
	// <-chan zk.Event: 用于监听文件变化的信道,当watch为false时应当忽略该返回值,使用方法如下:
	// 1    ok, version, watcher, err := Exists("/a/b", true)
	// 2    if err != nil {
	// 3        处理错误
	// 4        return(或者break或者log.Fatal())
	// 5    }
	// 6    <-watcher
	// 7    doSomething()
	// 在本例中,如果/a/b没有发生变化(没被删除或被更新),当前线程将会一直阻塞在第6行
	// 注意: watcher只能读取一次,即对于同一个watcher,第6行只能执行一次
	// err: 函数执行是否出错
	Exists(path string, watch bool) (bool, int32, <-chan zk.Event, error)

	// 读取文件内容
	// 例子(1): data, version, _, err := Get("/a/b", false)
	// 例子(2): data, version, watcher, err := Get("/a/b", true)
	//
	// 参数和返回值与Exists几乎完全相同
	Get(path string, watch bool) ([]byte, int32, <-chan zk.Event, error)

	// 更新文件内容
	// 例子: version, err := Set("/a/b", []byte("world"), 1)
	//
	// 参数
	// path: 文件路径
	// data: 文件内容
	// version: 指定待更新文件的版本号,与Delete()的version参数相同
	//
	// 返回值
	// int32: 更新后文件的版本号
	// error: 函数执行是否出错
	Set(path string, data []byte, version int32) (int32, error)

	// 读取目录内容
	// 例子(1): children, version, _, err := Children("/a/b", false)
	// 例子(2): children, version, watcher, err := Children("/a/b", true)
	//
	// 参数和返回值与Exists几乎完全相同
	Children(path string, watch bool) ([]string, int32, <-chan zk.Event, error)

	// 同步操作,确保读到最新的视图,即满足线性化的要求
	// 例子: _, err := Sync("/a/b")
	// 在crfs中,读取链表信息前调用Sync(),确保同一个链表的所有节点都能读到相同的配置信息,即尽可能避免:
	// 节点A读到的链表为: A -> B
	// 节点B读到的链表为: B -> C
	//
	// 参数
	// path: 同步路径
	//
	// 返回值
	// string: 同步路径
	// error: 函数执行是否出错
	Sync(path string) (string, error)
}

type ZKClient interface {
	// 连接zookeeper集群
	// 例子: zkConn, err := Connect(servers, sessionTimeout)
	//
	// 参数
	// servers: zookeeper集群地址
	// sessionTimeout: 连接的超时时长,当客户端和zookeeper服务器超过sessionTimeout没有通信时,连接会自动断开
	//
	// 返回值
	// ZKConn: zookeeper连接
	// error: 函数执行是否出错
	Connect() (ZKConn, error)
}
