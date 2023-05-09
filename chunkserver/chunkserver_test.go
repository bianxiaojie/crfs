package chunkserver

import (
	"bytes"
	"crfs/persister"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func GenericTest(t *testing.T, nclients int, unreliable bool, crash bool) {
	title := "Test: "
	if unreliable {
		title = title + "unreliable net, "
	}
	if crash {
		title = title + "restarts, "
	}
	if nclients > 1 {
		title = title + "many clients"
	} else {
		title = title + "one client"
	}

	nservers := 3
	cfg := makeConfig(t, nservers, unreliable)
	defer cfg.cleanup()

	cfg.begin(title)

	clientsDone := int32(0)
	type update struct {
		count int
		size  int
	}
	clientUpdates := make([]chan interface{}, nclients)
	for i := 0; i < nclients; i++ {
		clientUpdates[i] = make(chan interface{})
	}
	for i := 0; i < 3; i++ {
		atomic.StoreInt32(&clientsDone, 0)
		spawnClientsAndWait(t, cfg, nclients, func(cli int, ck *clerk, t *testing.T) {
			var v interface{}
			defer func() {
				clientUpdates[cli] <- v
			}()

			chunkName := strconv.Itoa(cli)
			last := make([]byte, 0)
			for atomic.LoadInt32(&clientsDone) == 0 {
				if r := rand.Int() % 1000; r < 700 {
					offset := rand.Intn(len(last) + 1)
					data := []byte(randstring(16))
					err := Write(cfg, ck, chunkName, offset, data)
					if err != persister.Success {
						v = err
						return
					}
					front := last[:offset]
					var end []byte
					if offset+16 < len(last) {
						end = last[offset+16:]
					}
					last = append(append(front, data...), end...)
				} else if r >= 300 && r < 700 {
					data := []byte(randstring(16))
					offset, err := Append(cfg, ck, chunkName, data)
					if err != persister.Success {
						v = err
						return
					}
					if len(last) != offset {
						v = fmt.Errorf("Append不一致, chunk name: %s, 预期的offset: %d, 实际的offset: %d\n", chunkName, len(last), offset)
						return
					}
					last = append(last, data...)
				} else {
					data, err := Read(cfg, ck, chunkName, 0, len(last))
					if err != persister.Success {
						v = err
						return
					}
					if !bytes.Equal(last, data) {
						v = fmt.Errorf("读取不一致, chunk name: %s, 预期的数据:\n%v\n, 实际的数据:\n%v\n", chunkName, last, data)
						return
					}
				}
			}
		}, func() {
			time.Sleep(sessionTimeout)
			atomic.StoreInt32(&clientsDone, 1)

			for i := 0; i < nclients; i++ {
				if err := <-clientUpdates[i]; err != nil {
					t.Fatal(err)
				}
			}

			if crash {
				for i := 0; i < nservers; i++ {
					cfg.crashOne(i)
				}

				time.Sleep(2 * sessionTimeout)

				for i := 0; i < nservers; i++ {
					cfg.startOne(i)
				}

				time.Sleep(sessionTimeout)
			}
		})

		for i := 0; i < nclients; i++ {
			chunkName := strconv.Itoa(i)
			for j := 0; ; {
				if cfg.chunkServers[j].delete(chunkName) == persister.Success {
					break
				}
				j = (j + 1) % len(cfg.chunkServers)

				time.Sleep(100 * time.Millisecond)
			}
		}

		atomic.StoreInt32(&clientsDone, 0)
		spawnClientsAndWait(t, cfg, nclients, func(cli int, ck *clerk, t *testing.T) {
			var v interface{}
			defer func() {
				clientUpdates[cli] <- v
			}()

			chunkName := strconv.Itoa(cli)
			lastString := ""
			count := 0
			size := 0
			for atomic.LoadInt32(&clientsDone) == 0 {
				if rand.Int()%1000 < 640 {
					data := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(count) + " y"
					offset, err := Append(cfg, ck, chunkName, []byte(data))
					if err != persister.Success {
						v = err
						return
					}
					if size != offset {
						v = fmt.Errorf("Append不一致, chunk name: %s, 预期的offset: %d, 实际的offset: %d\n", chunkName, size, offset)
						return
					}
					lastString = lastString + data
					count++
					size += len(string(data))
				} else {
					data, err := Read(cfg, ck, chunkName, 0, len([]byte(lastString)))
					if err != persister.Success {
						v = err
						return
					}
					if lastString != string(data) {
						v = fmt.Errorf("读取不一致, chunk name: %s, 预期的数据:\n%s\n, 实际的数据:\n%s\n", chunkName, lastString, string(data))
						return
					}
				}
			}
			v = update{count: count, size: size}
		}, func() {
			time.Sleep(sessionTimeout)
			atomic.StoreInt32(&clientsDone, 1)

			ck := cfg.makeClient()
			for i := 0; i < nclients; i++ {
				v := <-clientUpdates[i]
				if u, ok := v.(update); ok {
					chunkName := strconv.Itoa(i)
					data, err := Read(cfg, ck, chunkName, 0, u.size)
					if err != persister.Success {
						t.Fatal(err)
					}
					checkClntAppends(t, i, string(data), u.count)
				} else {
					t.Fatal(v)
				}
			}
			cfg.deleteClient(ck)

			if crash {
				for i := 0; i < nservers; i++ {
					cfg.crashOne(i)
				}

				time.Sleep(2 * sessionTimeout)

				for i := 0; i < nservers; i++ {
					cfg.startOne(i)
				}

				time.Sleep(sessionTimeout)
			}
		})

		for i := 0; i < nclients; i++ {
			chunkName := strconv.Itoa(i)
			for j := 0; ; {
				if cfg.chunkServers[j].delete(chunkName) == persister.Success {
					break
				}
				j = (j + 1) % len(cfg.chunkServers)

				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	cfg.end()
}

func spawnClientsAndWait(t *testing.T, cfg *config, ncli int, fn func(me int, ck *clerk, t *testing.T), beforeWaitCallback func()) {
	ca := make([]chan bool, ncli)
	for cli := 0; cli < ncli; cli++ {
		ca[cli] = make(chan bool)
		go func(t *testing.T, cfg *config, me int, ca chan bool, fn func(me int, ck *clerk, t *testing.T)) {
			ok := false
			defer func() { ca <- ok }()
			ck := cfg.makeClient()
			fn(me, ck, t)
			ok = true
			cfg.deleteClient(ck)
		}(t, cfg, cli, ca[cli], fn)
	}
	beforeWaitCallback()
	for cli := 0; cli < ncli; cli++ {
		ok := <-ca[cli]
		if ok == false {
			t.Fatalf("failure")
		}
	}
}

func Write(cfg *config, ck *clerk, chunkName string, offset int, data []byte) persister.Err {
	err := ck.write(chunkName, offset, data)
	cfg.op()
	return err
}

func Append(cfg *config, ck *clerk, chunkName string, data []byte) (int, persister.Err) {
	offset, err := ck.append(chunkName, data)
	cfg.op()
	return offset, err
}

func Read(cfg *config, ck *clerk, chunkName string, offset int, size int) ([]byte, persister.Err) {
	data, err := ck.read(chunkName, offset, size)
	cfg.op()
	return data, err
}

func checkClntAppends(t *testing.T, chunkName int, data string, count int) {
	lastoff := -1
	for j := 0; j < count; j++ {
		wanted := "x " + strconv.Itoa(chunkName) + " " + strconv.Itoa(j) + " y"
		off := strings.Index(data, wanted)
		if off < 0 {
			t.Fatalf("%v未成功Append: %v, 添加的完整数据: %v\n", chunkName, wanted, data)
		}
		off1 := strings.LastIndex(data, wanted)
		if off1 != off {
			t.Fatalf("Append重复添加: %v\n", wanted)
		}
		if off <= lastoff {
			t.Fatalf("Append数据的顺序有误: %v\n", wanted)
		}
		lastoff = off
	}
}

func checkConcurrentAppends(t *testing.T, v string, counts []int) {
	nclients := len(counts)
	for i := 0; i < nclients; i++ {
		lastoff := -1
		for j := 0; j < counts[i]; j++ {
			wanted := "x " + strconv.Itoa(i) + " " + strconv.Itoa(j) + " y"
			off := strings.Index(v, wanted)
			if off < 0 {
				t.Fatalf("%v未成功Append: %v, 添加的完整数据: %v\n", i, wanted, v)
			}
			off1 := strings.LastIndex(v, wanted)
			if off1 != off {
				t.Fatalf("Append重复添加: %v\n", wanted)
			}
			if off <= lastoff {
				t.Fatalf("Append数据的顺序有误: %v\n", wanted)
			}
			lastoff = off
		}
	}
}

func TestBasic(t *testing.T) {
	GenericTest(t, 1, false, false)
}

func TestConcurrent(t *testing.T) {
	GenericTest(t, 5, false, false)
}

func TestUnreliable(t *testing.T) {
	GenericTest(t, 5, true, false)
}

func TestUnreliableOneChunk(t *testing.T) {
	const nservers = 3
	cfg := makeConfig(t, nservers, true)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.begin("Test: concurrent append to same chunk, unreliable")

	const nclient = 10
	const count = 10
	var size int64
	spawnClientsAndWait(t, cfg, nclient, func(me int, ck *clerk, t *testing.T) {
		n := 0
		for n < count {
			data := []byte("x " + strconv.Itoa(me) + " " + strconv.Itoa(n) + " y")
			_, err := Append(cfg, ck, "k", data)
			if err != persister.Success {
				t.Fatal(err)
			}
			n++
			atomic.AddInt64(&size, int64(len(data)))
		}
	}, func() {})

	var counts []int
	for i := 0; i < nclient; i++ {
		counts = append(counts, count)
	}

	vx, err := Read(cfg, ck, "k", 0, int(size))
	if err != persister.Success {
		t.Fatal(err)
	}
	checkConcurrentAppends(t, string(vx), counts)

	cfg.end()
}

func TestPersistOneClient(t *testing.T) {
	GenericTest(t, 1, false, true)
}

func TestPersistConcurrent(t *testing.T) {
	GenericTest(t, 5, false, true)
}

func TestPersistConcurrentUnreliable(t *testing.T) {
	GenericTest(t, 5, true, true)
}
