package master

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"
)

func StringsEqual(strings1 []string, strings2 []string) bool {
	if len(strings1) != len(strings2) {
		return false
	}

	sort.Strings(strings1)
	sort.Strings(strings2)

	return reflect.DeepEqual(strings1, strings2)
}

func TestAllocateServer(t *testing.T) {
	done := make(chan bool)
	m := MakeMaster("", 24*time.Hour, 3*24*time.Hour, done)
	go func() {
		<-done
	}()

	ch := make(chan string)
	for i := 0; i <= MaxChainLength; i++ {
		go func(i int) {
			var reply AllocateServerReply
			defer func() {
				ch <- reply.ChainName
			}()

			args := AllocateServerArgs{ChunkServer: fmt.Sprintf("server%d", i)}
			m.AllocateServer(&args, &reply)
		}(i)
	}

	counters := make(map[string]int, 0)
	for i := 0; i <= MaxChainLength; i++ {
		chainName := <-ch
		if chainName == "" {
			t.Fatal("empty chain name")
		}
		counters[chainName]++
	}

	if len(counters) != 2 {
		t.Fatalf("expected count of different chain names: 2, actual count: %d\n", len(counters))
	}
	for _, count := range counters {
		if count != 1 && count != MaxChainLength {
			t.Fatalf("expected length of chain: 1 or %d, actual: %d\n", MaxChainLength, count)
		}
	}
}

func TestCreateInSameDirectory(t *testing.T) {
	done := make(chan bool)
	m := MakeMaster("", 24*time.Hour, 3*24*time.Hour, done)
	go func() {
		<-done
	}()

	ch := make(chan FileOperationErr)
	nfiles := 10
	for i := 0; i < nfiles; i++ {
		go func(i int) {
			var reply CreateReply
			defer func() {
				ch <- reply.Err
			}()

			args := CreateArgs{
				Path:   fmt.Sprintf("/file%d", i),
				IsFile: rand.Intn(2) == 0,
			}

			m.Create(&args, &reply)
		}(i)
	}

	expected := make([]string, nfiles)
	for i := 0; i < nfiles; i++ {
		err := <-ch
		if err != Success {
			t.Fatal(err)
		}
		expected[i] = fmt.Sprintf("file%d", i)
	}

	args := ListArgs{Path: "/"}
	var reply ListReply
	m.List(&args, &reply)
	if reply.Err != Success {
		t.Fatal(reply.Err)
	}
	actual := make([]string, nfiles)
	for i := 0; i < nfiles; i++ {
		actual[i] = reply.FileInfos[i].Name
	}

	if !StringsEqual(expected, actual) {
		t.Fatalf("list / expected: %v, actual: %v\n", expected, actual)
	}
}

func TestCreateSameFile(t *testing.T) {
	done := make(chan bool)
	m := MakeMaster("", 24*time.Hour, 3*24*time.Hour, done)
	go func() {
		<-done
	}()

	ch := make(chan int)
	file := "file"
	nthread := 1
	for i := 0; i < nthread; i++ {
		go func(i int) {
			ok := 0
			defer func() {
				ch <- ok
			}()

			args := CreateArgs{
				Path:   "/" + file,
				IsFile: true,
			}
			var reply CreateReply
			m.Create(&args, &reply)
			if reply.Err == Success {
				ok = 1
			}
		}(i)
	}

	ok := 0
	for i := 0; i < nthread; i++ {
		ok += <-ch
	}
	if ok != 1 {
		t.Fatalf("expected one creates file successfully, but %d dose\n", ok)
	}

	args := ListArgs{Path: "/"}
	var reply ListReply
	m.List(&args, &reply)
	if reply.Err != Success {
		t.Fatal(reply.Err)
	}

	if len(reply.FileInfos) != 1 {
		t.Fatalf("expected one file in /, but %d exists\n", len(reply.FileInfos))
	}

	actualFile := reply.FileInfos[0]
	if file != actualFile.Name {
		t.Fatalf("expected file: %s, actual: %s\n", file, actualFile.Name)
	}
	if !actualFile.IsFile {
		t.Fatal("expected file, actual directory")
	}
}

func TestDelete(t *testing.T) {
	done := make(chan bool)
	m := MakeMaster("", 24*time.Hour, 3*24*time.Hour, done)
	go func() {
		<-done
	}()

	m.Create(&CreateArgs{Path: "/directory", IsFile: false}, &CreateReply{})
	m.Create(&CreateArgs{Path: "/directory/file", IsFile: true}, &CreateReply{})
	var reply DeleteReply
	m.Delete(&DeleteArgs{Path: "/directory", IsFile: false}, &reply)
	if reply.Err != DirectoryNotEmpty {
		t.Fatalf("delete not empty directory: /directory, expected err: %s, actual: %s\n", DirectoryNotEmpty, reply.Err)
	}
	m.Delete(&DeleteArgs{Path: "/directory/file", IsFile: true}, &reply)
	if reply.Err != Success {
		t.Fatalf("fail to delete: %s\n", "/directory/file")
	}
	m.Delete(&DeleteArgs{Path: "/directory", IsFile: false}, &reply)
	if reply.Err != Success {
		t.Fatalf("fail to delete: %s\n", "/directory")
	}
}

func TestRestore(t *testing.T) {
	done := make(chan bool)
	m := MakeMaster("", 24*time.Hour, 3*24*time.Hour, done)
	go func() {
		<-done
	}()

	m.Create(&CreateArgs{Path: "/directory", IsFile: false}, &CreateReply{})
	m.Create(&CreateArgs{Path: "/directory/file", IsFile: true}, &CreateReply{})
	m.Delete(&DeleteArgs{Path: "/directory/file", IsFile: true}, &DeleteReply{})
	m.Delete(&DeleteArgs{Path: "/directory", IsFile: false}, &DeleteReply{})
	var reply RestoreReply
	m.Restore(&RestoreArgs{Path: "/directory/file", IsFile: true}, &reply)
	if reply.Err != DirectoryNotExists {
		t.Fatalf("restore file not exists: /directory/file, expected err: %s, actual: %s\n", DirectoryNotExists, reply.Err)
	}
	m.Restore(&RestoreArgs{Path: "/directory", IsFile: false}, &reply)
	if reply.Err != Success {
		t.Fatal("fail to restore: /directory")
	}
	m.Restore(&RestoreArgs{Path: "/directory/file", IsFile: true}, &reply)
	if reply.Err != Success {
		t.Fatal("fail to restore: /directory/file")
	}
}

func TestMove(t *testing.T) {
	done := make(chan bool)
	m := MakeMaster("", 24*time.Hour, 3*24*time.Hour, done)
	go func() {
		<-done
	}()

	m.Create(&CreateArgs{Path: "/directory", IsFile: false}, &CreateReply{})
	m.Create(&CreateArgs{Path: "/directory/file", IsFile: true}, &CreateReply{})
	var moveReply MoveReply
	m.Move(&MoveArgs{SrcPath: "/directory/file", TargetPath: "/file"}, &moveReply)
	if moveReply.Err != Success {
		t.Fatalf("fail to move /directory/file to /file: %v\n", moveReply.Err)
	}

	var listReply ListReply
	m.List(&ListArgs{Path: "/"}, &listReply)
	expected := []string{"directory", "file"}
	actual := make([]string, 0)
	for _, fileInfo := range listReply.FileInfos {
		actual = append(actual, fileInfo.Name)
	}

	if !StringsEqual(expected, actual) {
		t.Fatalf("list / expected: %v, actual: %v\n", expected, actual)
	}
}

func TestAllocateTrunks(t *testing.T) {
	done := make(chan bool)
	m := MakeMaster("", 24*time.Hour, 3*24*time.Hour, done)
	go func() {
		<-done
	}()

	for i := 0; i < 3*MaxChainLength; i++ {
		m.AllocateServer(&AllocateServerArgs{ChunkServer: fmt.Sprintf("server%d", i)}, &AllocateServerReply{})
	}

	m.Create(&CreateArgs{Path: "/file", IsFile: true}, &CreateReply{})
	var writeReply WriteReply
	m.Write(&WriteArgs{Path: "/file", FirstChunkIndex: 0, LastChunkIndex: 2}, &writeReply)
	if writeReply.Err != Success {
		t.Fatalf("fail to allocate chunks for /file: %v\n", writeReply.Err)
	}
	var readReply ReadReply
	m.Read(&ReadArgs{Path: "/file", FirstChunkIndex: 0, LastChunkIndex: 2}, &readReply)
	if readReply.Err != Success {
		t.Fatalf("fail to get allocated chunks /file: %v\n", readReply.Err)
	}

	if !reflect.DeepEqual(writeReply.ChunkNames, readReply.ChunkNames) {
		t.Fatalf("inconsistent chunk names: %v, %v\n", writeReply.ChunkNames, readReply.ChunkNames)
	}
	if !reflect.DeepEqual(writeReply.ChunkServers, readReply.ChunkServers) {
		t.Fatalf("inconsistent chunk servers: %v, %v\n", writeReply.ChunkServers, readReply.ChunkServers)
	}
}

func TestAllocateTrunk(t *testing.T) {
	done := make(chan bool)
	m := MakeMaster("", 24*time.Hour, 3*24*time.Hour, done)
	go func() {
		<-done
	}()

	for i := 0; i < 3*MaxChainLength; i++ {
		m.AllocateServer(&AllocateServerArgs{ChunkServer: fmt.Sprintf("server%d", i)}, &AllocateServerReply{})
	}

	m.Create(&CreateArgs{Path: "/file", IsFile: true}, &CreateReply{})
	var appendReply AppendReply
	m.Append(&AppendArgs{Path: "/file", FullChunkIndex: -1}, &appendReply)
	if appendReply.Err != Success {
		t.Fatalf("fail to allocate new chunk for /file: %v\n", appendReply.Err)
	}
	var readReply ReadReply
	m.Read(&ReadArgs{Path: "/file", FirstChunkIndex: 0, LastChunkIndex: 0}, &readReply)
	if readReply.Err != Success {
		t.Fatalf("fail to get allocated chunks /file: %v\n", readReply.Err)
	}

	if !reflect.DeepEqual(appendReply.ChunkName, readReply.ChunkNames[0]) {
		t.Fatalf("inconsistent chunk names: %v, %v\n", appendReply.ChunkName, readReply.ChunkNames[0])
	}
	if !reflect.DeepEqual(appendReply.ChunkServers, readReply.ChunkServers[0]) {
		t.Fatalf("inconsistent chunk servers: %v, %v\n", appendReply.ChunkServers, readReply.ChunkServers[0])
	}

	m.Append(&AppendArgs{Path: "/file", FullChunkIndex: -1}, &appendReply)
	if appendReply.Err != Success {
		t.Fatalf("fail to get allocated chunk for /file: %v\n", appendReply.Err)
	}
	m.Read(&ReadArgs{Path: "/file", FirstChunkIndex: 0, LastChunkIndex: 0}, &readReply)
	if readReply.Err != Success {
		t.Fatalf("fail to get allocated chunks /file: %v\n", readReply.Err)
	}

	if !reflect.DeepEqual(appendReply.ChunkName, readReply.ChunkNames[0]) {
		t.Fatalf("inconsistent chunk names: %v, %v\n", appendReply.ChunkName, readReply.ChunkNames[0])
	}
	if !reflect.DeepEqual(appendReply.ChunkServers, readReply.ChunkServers[0]) {
		t.Fatalf("inconsistent chunk servers: %v, %v\n", appendReply.ChunkServers, readReply.ChunkServers[0])
	}
}

func TestAck(t *testing.T) {
	done := make(chan bool)
	m := MakeMaster("", 24*time.Hour, 3*24*time.Hour, done)
	go func() {
		<-done
	}()

	for i := 0; i < 3*MaxChainLength; i++ {
		m.AllocateServer(&AllocateServerArgs{ChunkServer: fmt.Sprintf("server%d", i)}, &AllocateServerReply{})
	}

	m.Create(&CreateArgs{Path: "/file", IsFile: true}, &CreateReply{})

	ch := make(chan interface{})
	nclients := 10
	for i := 0; i < nclients; i++ {
		go func() {
			var result interface{}
			defer func() {
				ch <- result
			}()

			var appendReply AppendReply
			m.Append(&AppendArgs{Path: "/file", FullChunkIndex: -1}, &appendReply)
			if appendReply.Err != Success {
				result = appendReply.Err
				return
			}

			ms := rand.Intn(100)
			time.Sleep(time.Duration(ms) * time.Millisecond)

			pointer := rand.Intn(1000)
			var ackReply AckReply
			m.Ack(&AckArgs{Path: "/file", Pointer: pointer}, &ackReply)
			if ackReply.Err != Success {
				result = ackReply.Err
				return
			}

			result = pointer
		}()
	}

	maxPointer := 0
	for i := 0; i < nclients; i++ {
		result := <-ch
		if pointer, ok := result.(int); ok {
			maxPointer = int(math.Max(float64(maxPointer), float64(pointer)))
		} else {
			t.Fatal(result)
		}
	}

	var listReply ListReply
	m.List(&ListArgs{Path: "/"}, &listReply)
	if listReply.Err != Success {
		t.Fatalf("fail to list /: %v\n", listReply.Err)
	}

	var fileInfo *FileInfo
	for _, fi := range listReply.FileInfos {
		if fi.Name == "file" {
			fileInfo = &fi
			break
		}
	}
	if fileInfo == nil {
		t.Fatal("cannot find /file")
	}

	if fileInfo.Size != maxPointer+1 {
		t.Fatalf("expected file size: %d, actual file size: %d\n", maxPointer, fileInfo.Size)
	}
}

func TestCleanup(t *testing.T) {
	done := make(chan bool)
	m := MakeMaster("", time.Second, time.Second, done)
	go func() {
		<-done
	}()

	m.Create(&CreateArgs{Path: "/file", IsFile: true}, &CreateReply{})
	m.Delete(&DeleteArgs{Path: "/file", IsFile: true}, &DeleteReply{})
	var reply RestoreReply
	m.Restore(&RestoreArgs{Path: "/file", IsFile: true}, &reply)
	if reply.Err != Success {
		t.Fatal("fail to restore: /file")
	}
	m.Delete(&DeleteArgs{Path: "/file", IsFile: true}, &DeleteReply{})

	time.Sleep(time.Second + 10*time.Millisecond)

	m.Restore(&RestoreArgs{Path: "/file", IsFile: true}, &reply)
	if reply.Err == Success {
		t.Fatal("/file expected to be cleanup, but restore successfully")
	}
}
