package storage

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"

	"github.com/zachgoldstein/datatoapi/models"
)

type LocalFS struct {
	FSLocation string
	FilePaths  []string
}

// NewLocalFS creates an instance of LocalFS
func NewLocalFS() *LocalFS {
	return &LocalFS{}
}

// Start initialises the local filesystem, testing to make sure the data is accessible
func (fs *LocalFS) Start(path string, credentials map[string]interface{}) error {
	fs.FSLocation = path
	return fs.TestData()
}

// TestData makes sure we have access to the file(s) we need to interact with
func (fs *LocalFS) TestData() error {
	f, err := os.Open(fs.FSLocation)
	if err != nil {
		return err
	}
	stat, err := f.Stat()
	if err != nil {
		fmt.Println(fmt.Errorf("Couldn't find data file: %s", err))
	}
	if stat.IsDir() {
		fmt.Println("Data is a directory, will walk path to find files")
	}

	f.Close()
	return err
}

// ScanData will read all data, serialising it into a interface{}
// and putting it on a channel for consumption. (JSONfiles are used here)
func (fs *LocalFS) ScanData(interfaceChan chan<- interface{}) error {
	fs.FilePaths = []string{}
	err := filepath.Walk(fs.FSLocation, fs.visitPath)
	if err != nil {
		return err
	}
	for _, location := range fs.FilePaths {
		f, err := os.Open(location)
		if err != nil {
			return err
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		err = WriteJSONToInterfaceChan(scanner, interfaceChan)
		if err != nil {
			return err
		}

		scanner.Split(bufio.ScanLines)
		err = scanner.Err()
		if err != nil {
			return err
		}
	}

	close(interfaceChan)
	fmt.Println("Finished scanning data into interface channel")
	return nil
}

func (fs *LocalFS) ScanDataBlocksForPath(path string, dataChan chan<- models.IndexData, blockChan chan<- models.DataBlock) error {
	fmt.Printf("Scanning data at %v \n", path)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	err = WriteJSONToDataChans(path, scanner, dataChan, blockChan)
	if err != nil {
		return err
	}

	fmt.Println("Finished scanning")
	return scanner.Err()
}

func (fs *LocalFS) visitPath(path string, f os.FileInfo, err error) error {
	fmt.Printf("Visited: %s\n", path)
	if f.IsDir() {
		return nil
	}
	fs.FilePaths = append(fs.FilePaths, path)
	return nil
}

// ScanDataBlocks will read all data, serialising the full data and blocks of data to send on channels
// Focused on Jsonfiles for now.
func (fs *LocalFS) ScanDataBlocks(dataChan chan<- models.IndexData, blockChan chan<- models.DataBlock) error {
	fs.FilePaths = []string{}
	err := filepath.Walk(fs.FSLocation, fs.visitPath)
	if err != nil {
		return err
	}
	for _, path := range fs.FilePaths {
		err := fs.ScanDataBlocksForPath(path, dataChan, blockChan)
		if err != nil {
			return err
		}
	}
	close(dataChan)
	close(blockChan)

	return nil
}

func (fs *LocalFS) RetrieveDataBlockBytes(block *models.DataBlock) ([]byte, error) {
	f, err := os.Open(block.File.Address)
	if err != nil {
		return nil, err
	}
	_, err = f.Seek(block.Start, 0)
	if err != nil {
		return nil, err
	}
	byteLength := block.End - block.Start
	retrievedBytes := make([]byte, byteLength)
	_, err = f.Read(retrievedBytes)
	if err != nil {
		return nil, err
	}

	return retrievedBytes, nil
}
