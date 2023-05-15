package persister

import (
	"archive/zip"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type FilePersister struct {
	mu         sync.Mutex
	snapshotMu sync.RWMutex
	chunkMu    map[string]*sync.RWMutex
	directory  string
	chainName  string
}

func MakeFilePersister(directory string, chainName string) *FilePersister {
	fp := &FilePersister{}
	fp.directory = directory
	fp.chainName = chainName
	fp.chunkMu = make(map[string]*sync.RWMutex)

	if err := os.MkdirAll(directory+"/"+chainName+"/node", 0775); err != nil {
		log.Fatal(err)
	}
	if err := os.MkdirAll(directory+"/"+chainName+"/snapshot", 0775); err != nil {
		log.Fatal(err)
	}
	if err := os.MkdirAll(directory+"/"+chainName+"/chunk", 0775); err != nil {
		log.Fatal(err)
	}

	return fp
}

func (fp *FilePersister) Copy() Persister {
	return fp
}

func (fp *FilePersister) SaveNodeState(state []byte) {
	fp.snapshotMu.RLock()
	defer fp.snapshotMu.RUnlock()

	stateFile := fp.directory + "/" + fp.chainName + "/node/state"
	if err := os.WriteFile(stateFile, state, 0664); err != nil {
		log.Fatal(err)
	}
}

func (fp *FilePersister) ReadNodeState() []byte {
	fp.snapshotMu.RLock()
	defer fp.snapshotMu.RUnlock()

	stateFile := fp.directory + "/" + fp.chainName + "/node/state"
	state, err := os.ReadFile(stateFile)
	if err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}
	return state
}

func (fp *FilePersister) NodeStateSize() int {
	fp.snapshotMu.RLock()
	defer fp.snapshotMu.RUnlock()

	stateFile := fp.directory + "/" + fp.chainName + "/node/state"
	fi, err := os.Stat(stateFile)
	if err != nil {
		log.Fatal(err)
	}
	return int(fi.Size())
}

func zipSource(source, target string) error {
	// 1. Create a ZIP file and zip.Writer
	f, err := os.Create(target)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := zip.NewWriter(f)
	defer writer.Close()

	// 2. Go through all the files of the source
	return filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 3. Create a local file header
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}

		// set compression
		header.Method = zip.Deflate

		// 4. Set relative path of a file as the header name
		header.Name, err = filepath.Rel(filepath.Dir(source), path)
		if err != nil {
			return err
		}
		if info.IsDir() {
			header.Name += "/"
		}

		// 5. Create writer for the file header and save content of the file
		headerWriter, err := writer.CreateHeader(header)
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		_, err = io.Copy(headerWriter, f)
		return err
	})
}

func unzipSource(source, destination string) error {
	// 1. Open the zip file
	reader, err := zip.OpenReader(source)
	if err != nil {
		return err
	}
	defer reader.Close()

	// 2. Get the absolute destination path
	destination, err = filepath.Abs(destination)
	if err != nil {
		return err
	}

	// 3. Iterate over zip files inside the archive and unzip each of them
	for _, f := range reader.File {
		err := unzipFile(f, destination)
		if err != nil {
			return err
		}
	}

	return nil
}

func unzipFile(f *zip.File, destination string) error {
	// 4. Check if file paths are not vulnerable to Zip Slip
	filePath := filepath.Join(destination, f.Name)
	if !strings.HasPrefix(filePath, filepath.Clean(destination)+string(os.PathSeparator)) {
		return fmt.Errorf("invalid file path: %s", filePath)
	}

	// 5. Create directory tree
	if f.FileInfo().IsDir() {
		if err := os.MkdirAll(filePath, os.ModePerm); err != nil {
			return err
		}
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return err
	}

	// 6. Create a destination file for unzipped content
	destinationFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
	if err != nil {
		return err
	}
	defer destinationFile.Close()

	// 7. Unzip the content of a file and copy it to the destination file
	zippedFile, err := f.Open()
	if err != nil {
		return err
	}
	defer zippedFile.Close()

	if _, err := io.Copy(destinationFile, zippedFile); err != nil {
		return err
	}
	return nil
}

func (fp *FilePersister) SaveStateAndSnapshot(state []byte) {
	fp.snapshotMu.Lock()
	defer fp.snapshotMu.Unlock()

	stateFile := fp.directory + "/" + fp.chainName + "/node/state"
	if err := os.WriteFile(stateFile, state, 0664); err != nil {
		log.Fatal(err)
	}

	chunkDirectory := fp.directory + "/" + fp.chainName + "/chunk"
	snapshotChunkFile := fp.directory + "/" + fp.chainName + "/snapshot/chunk"
	if err := zipSource(chunkDirectory, snapshotChunkFile); err != nil {
		log.Fatal(err)
	}
}

func (fp *FilePersister) RestoreSnapshot() {
	fp.snapshotMu.Lock()
	defer fp.snapshotMu.Unlock()

	chunkDirectory := fp.directory + "/" + fp.chainName + "/chunk"
	snapshotChunkFile := fp.directory + "/" + fp.chainName + "/snapshot/chunk"
	if err := os.RemoveAll(chunkDirectory); err != nil {
		log.Fatal(err)
	}
	if err := os.MkdirAll(chunkDirectory, 0775); err != nil {
		log.Fatal(err)
	}
	if err := unzipSource(snapshotChunkFile, chunkDirectory); err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}
}

func (fp *FilePersister) WriteChunk(chunkName string, offset int, data []byte) Err {
	if offset < 0 || offset+len(data) > MaxChunkSize {
		return OutOfChunk
	}

	fp.snapshotMu.RLock()
	defer fp.snapshotMu.RUnlock()

	fp.mu.Lock()
	cmu, ok := fp.chunkMu[chunkName]
	if !ok {
		fp.chunkMu[chunkName] = &sync.RWMutex{}
		cmu = fp.chunkMu[chunkName]
	}
	fp.mu.Unlock()

	cmu.Lock()
	defer cmu.Unlock()

	chunkFile := fp.directory + "/" + fp.chainName + "/chunk/" + chunkName
	file, err := os.OpenFile(chunkFile, os.O_CREATE|os.O_WRONLY, 0664)
	if err != nil {
		log.Fatalf("hello %v\n", err)
	}

	if _, err = file.WriteAt(data, int64(offset)); err != nil {
		log.Fatal(err)
	}

	return Success
}

func (fp *FilePersister) AppendChunk(chunkName string, data []byte) (int, Err) {
	fp.snapshotMu.RLock()
	defer fp.snapshotMu.RUnlock()

	fp.mu.Lock()
	cmu, ok := fp.chunkMu[chunkName]
	if !ok {
		fp.chunkMu[chunkName] = &sync.RWMutex{}
		cmu = fp.chunkMu[chunkName]
	}
	fp.mu.Unlock()

	cmu.Lock()
	defer cmu.Unlock()

	chunkFile := fp.directory + "/" + fp.chainName + "/chunk/" + chunkName
	fi, err := os.Stat(chunkFile)
	if err != nil {
		log.Fatal(err)
	}

	offset := int(fi.Size())
	if offset+len(data) > MaxChunkSize {
		return -1, OutOfChunk
	}

	file, err := os.OpenFile(chunkFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0664)
	if err != nil {
		log.Fatal(err)
	}

	if _, err = file.Write(data); err != nil {
		log.Fatal(err)
	}

	return offset, Success
}

func (fp *FilePersister) ReadChunk(chunkName string, offset int, size int) ([]byte, Err) {
	if offset < 0 || size < 0 {
		return nil, OutOfChunk
	}

	fp.snapshotMu.RLock()
	defer fp.snapshotMu.RUnlock()

	fp.mu.Lock()
	cmu, ok := fp.chunkMu[chunkName]
	if !ok {
		fp.chunkMu[chunkName] = &sync.RWMutex{}
		cmu = fp.chunkMu[chunkName]
	}
	fp.mu.Unlock()

	cmu.RLock()
	defer cmu.RUnlock()

	chunkFile := fp.directory + "/" + fp.chainName + "/chunk/" + chunkName
	file, err := os.OpenFile(chunkFile, os.O_CREATE|os.O_RDONLY, 0664)
	if err != nil {
		log.Fatal(err)
	}

	data := make([]byte, size)
	if _, err = file.ReadAt(data, int64(offset)); err != nil {
		return nil, OutOfChunk
	}

	return data, Success
}

func (fp *FilePersister) DeleteChunk(chunkName string) {
	fp.snapshotMu.RLock()
	defer fp.snapshotMu.RUnlock()

	fp.mu.Lock()
	cmu, ok := fp.chunkMu[chunkName]
	if !ok {
		fp.chunkMu[chunkName] = &sync.RWMutex{}
		cmu = fp.chunkMu[chunkName]
	}
	fp.mu.Unlock()

	cmu.Lock()
	defer cmu.Unlock()

	chunkFile := fp.directory + "/" + fp.chainName + "/chunk/" + chunkName
	os.Remove(chunkFile)
}

func (fp *FilePersister) ChunkNames() []string {
	fp.snapshotMu.RLock()
	defer fp.snapshotMu.RUnlock()

	chunkDirectory := fp.directory + "/" + fp.chainName + "/chunk"
	directory, err := os.ReadDir(chunkDirectory)
	if err != nil {
		log.Fatal(err)
	}

	chunkNames := make([]string, len(directory))
	for i, file := range directory {
		chunkNames[i] = file.Name()
	}

	return chunkNames
}
