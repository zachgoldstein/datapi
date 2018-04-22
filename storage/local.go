package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/zachgoldstein/datatoapi/store"
)

const DefaultStorageType = "LocalFS"

var indexCount = 0

type Storage interface {
	CreateIndexes(db *store.DB, indexFormat interface{}) error
	BuildIndexFormat(db *store.DB) (interface{}, error)
	CreateIndex(db *store.DB, data string, prevPos int64, currentPos int64, indexFormat interface{}) error
	RetrieveData(block store.DataBlock) (string, error)
}

type LocalFS struct {
	File *os.File
}

type IndexData struct {
	Data  string
	Start int64
	End   int64
}

func NewLocalFS(location string) *LocalFS {
	f, err := os.Open(location)
	if err != nil {
		fmt.Println(fmt.Errorf("Couldn't find data file: %s", err))
	}
	return &LocalFS{
		File: f,
	}
}

func (fs *LocalFS) BuildIndexFormat(db *store.DB) (interface{}, error) {
	// Scan through some tuneable portion of the data
	// Build up an interface that represents the data
	// Build up meta data on db?
	// Add storm field tags for data
	var i interface{}
	return i, nil
}

func (fs *LocalFS) CreateIndexes(db *store.DB, indexFormat interface{}) error {
	scanner := bufio.NewScanner(fs.File)
	currentPos := int64(0)
	prevPos := int64(0)
	split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		advance, token, err = bufio.ScanLines(data, atEOF)
		prevPos = currentPos
		currentPos += int64(advance)
		return
	}
	scanner.Split(split)

	indexJobs := make(chan IndexData, 100)

	var wg sync.WaitGroup
	for w := 1; w <= 10; w++ {
		go IndexWorker(w, fs, db, &wg, indexFormat, indexJobs)
	}

	for scanner.Scan() {
		wg.Add(1)
		txt := scanner.Text()
		indexData := IndexData{
			Data:  txt,
			Start: prevPos,
			End:   currentPos,
		}
		indexJobs <- indexData
	}
	wg.Wait()

	return scanner.Err()
}

func IndexWorker(workerId int, fs *LocalFS, db *store.DB, wg *sync.WaitGroup, indexFormat interface{}, indexJobs <-chan IndexData) {
	for j := range indexJobs {
		err := fs.CreateIndex(db, j.Data, j.Start, j.End, indexFormat)
		if err != nil {
			fmt.Println(fmt.Errorf("Error creating index: %s", err))
		}
		wg.Done()
	}
}

func (fs *LocalFS) CreateIndex(db *store.DB, data string, prevPos int64, currentPos int64, indexFormat interface{}) error {
	indexData := store.DummyIndexData{}
	// UNMARSHALL INTO INTERFACE TYPE>>>>>
	err := json.Unmarshal([]byte(data), &indexData)
	if err != nil {
		return fmt.Errorf("Couldn't unmarshal text: %s", data)
	}
	block := store.DataBlock{
		Start: prevPos,
		End:   currentPos,
		File: store.File{
			Address: "./data/data.jsonfiles",
			Type:    DefaultStorageType,
		},
	}
	err = db.SaveIndex(&indexData, &block)
	if err != nil {
		return fmt.Errorf("Couldn't save index to bolt: %s", err)
	}

	indexCount++
	if indexCount%10 == 0 {
		fmt.Printf("saved indexCount items to db: %d \n", indexCount)
	}
	return nil
}

func (fs *LocalFS) RetrieveData(block store.DataBlock) (string, error) {
	f, err := os.Open(block.File.Address)
	if err != nil {
		return "", err
	}
	o2, err := f.Seek(block.Start, 0)
	if err != nil {
		return "", err
	}
	byteLength := block.End - block.Start
	retrievedBytes := make([]byte, byteLength)
	n2, err := f.Read(retrievedBytes)
	if err != nil {
		return "", err
	}
	fmt.Printf("%d bytes @ %d: %s\n", n2, o2, string(retrievedBytes))
	// fullRecord := DummyIndex{}
	// err = json.Unmarshal(retrievedBytes, &fullRecord)
	// if err != nil {
	// 	return "", err
	// }
	// fmt.Printf("Retrieved full record %#v \n", fullRecord)
	// exportedRecord, err := json.Marshal(fullRecord)
	// if err != nil {
	// 	return "", err
	// }
	return string(retrievedBytes), nil
}
