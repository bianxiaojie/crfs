package test

import (
	"bytes"
	"crfs/client"
	"crfs/master"
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
		cl := cfg.makeClient()

		atomic.StoreInt32(&clientsDone, 0)
		spawnClientsAndWait(t, cfg, nclients, func(cli int, cl *client.Client, t *testing.T) {
			var v interface{}
			defer func() {
				clientUpdates[cli] <- v
			}()

			file := fmt.Sprintf("/%d", cli)
			for {
				err := cl.Create(file, true)
				if err == master.Success || err == master.FileExists {
					break
				}
				if err != master.ErrConnection {
					v = err
					return
				}

				time.Sleep(100 * time.Millisecond)
			}

			last := make([]byte, 0)
			for atomic.LoadInt32(&clientsDone) == 0 {
				if r := rand.Int() % 1000; r < 700 {
					offset := rand.Intn(len(last) + 1)
					data := []byte(randstring(16))
					err := Write(cfg, cl, file, offset, data)
					if err != master.Success {
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
					offset, err := Append(cfg, cl, file, data)
					if err != master.Success {
						v = err
						return
					}
					if len(last) != offset {
						v = fmt.Errorf("Append不一致, file name: %s, 预期的offset: %d, 实际的offset: %d\n", file, len(last), offset)
						return
					}
					last = append(last, data...)
				} else {
					data, err := Read(cfg, cl, file, 0, len(last))
					if err != master.Success {
						v = err
						return
					}
					if !bytes.Equal(last, data) {
						v = fmt.Errorf("读取不一致, file name: %s, 预期的数据:\n%v\n, 实际的数据:\n%v\n", file, last, data)
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
			file := fmt.Sprintf("/%d", i)
			for {
				err := cl.Delete(file, true)
				if err == master.Success || err == master.FileNotExists {
					break
				}
				if err != master.ErrConnection {
					t.Fatal(err)
				}

				time.Sleep(100 * time.Millisecond)
			}
		}

		atomic.StoreInt32(&clientsDone, 0)
		spawnClientsAndWait(t, cfg, nclients, func(cli int, cl *client.Client, t *testing.T) {
			var v interface{}
			defer func() {
				clientUpdates[cli] <- v
			}()

			file := fmt.Sprintf("/%d", cli)
			for {
				err := cl.Create(file, true)
				if err == master.Success || err == master.FileExists {
					break
				}
				if err != master.ErrConnection {
					v = err
					return
				}

				time.Sleep(100 * time.Millisecond)
			}

			lastString := ""
			count := 0
			size := 0
			for atomic.LoadInt32(&clientsDone) == 0 {
				if rand.Int()%1000 < 640 {
					data := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(count) + " y"
					offset, err := Append(cfg, cl, file, []byte(data))
					if err != master.Success {
						v = err
						return
					}
					if size != offset {
						v = fmt.Errorf("Append不一致, file name: %s, 预期的offset: %d, 实际的offset: %d\n", file, size, offset)
						return
					}
					lastString = lastString + data
					count++
					size += len(string(data))
				} else {
					data, err := Read(cfg, cl, file, 0, len([]byte(lastString)))
					if err != master.Success {
						v = err
						return
					}
					if lastString != string(data) {
						v = fmt.Errorf("读取不一致, file name: %s, 预期的数据:\n%s\n, 实际的数据:\n%s\n", file, lastString, string(data))
						return
					}
				}
			}
			v = update{count: count, size: size}
		}, func() {
			time.Sleep(sessionTimeout)
			atomic.StoreInt32(&clientsDone, 1)

			cl := cfg.makeClient()
			for i := 0; i < nclients; i++ {
				v := <-clientUpdates[i]
				if u, ok := v.(update); ok {
					file := fmt.Sprintf("/%d", i)
					data, err := Read(cfg, cl, file, 0, u.size)
					if err != master.Success {
						t.Fatal(err)
					}
					checkClntAppends(t, i, string(data), u.count)
				} else {
					t.Fatal(v)
				}
			}
			cfg.deleteClient(cl)

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
			file := fmt.Sprintf("/%d", i)
			for {
				err := cl.Delete(file, true)
				if err == master.Success || err == master.FileNotExists {
					break
				}
				if err != master.ErrConnection {
					t.Fatal(err)
				}

				time.Sleep(100 * time.Millisecond)
			}
		}

		cl.Close()
	}

	cfg.end()
}

func spawnClientsAndWait(t *testing.T, cfg *config, ncli int, fn func(me int, cl *client.Client, t *testing.T), beforeWaitCallback func()) {
	ca := make([]chan bool, ncli)
	for cli := 0; cli < ncli; cli++ {
		ca[cli] = make(chan bool)
		go func(t *testing.T, cfg *config, me int, ca chan bool, fn func(me int, cl *client.Client, t *testing.T)) {
			ok := false
			defer func() { ca <- ok }()
			cl := cfg.makeClient()
			fn(me, cl, t)
			ok = true
			cfg.deleteClient(cl)
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

func Write(cfg *config, cl *client.Client, file string, offset int, data []byte) master.FileOperationErr {
	err := cl.Write(file, offset, data)
	cfg.op()
	return err
}

func Append(cfg *config, cl *client.Client, file string, data []byte) (int, master.FileOperationErr) {
	offset, err := cl.Append(file, data)
	cfg.op()
	return offset, err
}

func Read(cfg *config, cl *client.Client, file string, offset int, size int) ([]byte, master.FileOperationErr) {
	data, err := cl.Read(file, offset, size)
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

	cfg.begin("Test: concurrent append to same file, unreliable")

	cl := cfg.makeClient()
	file := "/k"
	for {
		err := cl.Create(file, true)
		if err == master.Success || err == master.FileExists {
			break
		}
		if err != master.ErrConnection {
			t.Fatal(err)
		}

		time.Sleep(100 * time.Millisecond)
	}

	const nclient = 10
	const count = 10
	var size int64
	spawnClientsAndWait(t, cfg, nclient, func(me int, cl *client.Client, t *testing.T) {
		n := 0
		for n < count {
			data := []byte("x " + strconv.Itoa(me) + " " + strconv.Itoa(n) + " y")
			_, err := Append(cfg, cl, file, data)
			if err != master.Success {
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

	vx, err := Read(cfg, cl, file, 0, int(size))
	if err != master.Success {
		t.Fatal(err)
	}
	checkConcurrentAppends(t, string(vx), counts)

	for {
		err := cl.Delete(file, true)
		if err == master.Success || err == master.FileNotExists {
			break
		}
		if err != master.ErrConnection {
			t.Fatal(err)
		}

		time.Sleep(100 * time.Millisecond)
	}

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
