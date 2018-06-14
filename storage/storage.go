package storage

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/zachgoldstein/datatoapi/models"
)

const BLOCK_SIZE = int64(50)

// PhysicalStorer is responsible for interacting with physical storage, creating
// datablocks for indexing to reference
type PhysicalStorer interface {
	Start(path string, credentials map[string]interface{}) error
	TestData() error
	ScanData(interfaceChan chan<- interface{}) error
	ScanDataBlocks(dataChan chan<- models.IndexData, blockChan chan<- models.DataBlock) error

	RetrieveDataBlockBytes(block *models.DataBlock) ([]byte, error)

	// GetMetadata() (map[string]interface{}, error)
}

func GetRefKey(location string) string {
	return fmt.Sprintf("%s-%d-%d", location, time.Now().UnixNano(), BLOCK_SIZE)
}

func WriteJSONToInterfaceChan(scanner *bufio.Scanner, interfaceChan chan<- interface{}) error {
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		rawRecord := scanner.Bytes()
		var record interface{}
		err := json.Unmarshal(rawRecord, &record)
		if err != nil {
			return err
		}
		interfaceChan <- record
	}
	return scanner.Err()
}

func WriteJSONToDataChans(path string, scanner *bufio.Scanner, dataChan chan<- models.IndexData, blockChan chan<- models.DataBlock) error {
	log.WithFields(log.Fields{
		"objectKey": path,
	}).Info("Starting to write data to channels")

	currentPos := int64(0)
	prevPos := int64(0)
	fileEOF := false
	split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		fileEOF = atEOF
		advance, token, err = bufio.ScanLines(data, atEOF)
		prevPos = currentPos
		currentPos += int64(advance)
		return
	}
	scanner.Split(split)

	count := 0
	currentBlockPos := int64(0)
	refKey := GetRefKey(path)
	for scanner.Scan() {
		dataBytes := scanner.Bytes()
		var marshalledData interface{}
		err := json.Unmarshal(dataBytes, &marshalledData)
		if err != nil {
			log.WithError(err).Error("Error reading data")
			continue
		}

		indexData := models.IndexData{
			Data:   marshalledData.(map[string]interface{}),
			RefKey: refKey,
		}

		if (count != 0 && count%int(BLOCK_SIZE) == 0) || fileEOF {
			dataBlock := models.DataBlock{
				RefKey: refKey,
				Start:  currentBlockPos,
				End:    currentPos,
				File: models.File{
					Address: path,
					Type:    "jsonfiles",
				},
			}
			currentBlockPos = currentPos
			refKey = GetRefKey(path)
			blockChan <- dataBlock
		}

		count++
		dataChan <- indexData
	}

	dataBlock := models.DataBlock{
		RefKey: refKey,
		Start:  currentBlockPos,
		End:    currentPos,
		File: models.File{
			Address: path,
			Type:    "jsonfiles",
		},
	}
	blockChan <- dataBlock

	err := scanner.Err()
	if err != nil {
		log.WithError(err).Error("Error scanning data")
	}
	log.WithFields(log.Fields{
		"objectKey": path,
	}).Info("Finished writing data to channels")

	return err
}

func GetRecordInDataChunk(chunk []byte, searchField, searchString string) ([]byte, error) {
	scanner := bufio.NewScanner(bytes.NewReader(chunk))
	scanner.Split(bufio.ScanLines)
	log.WithFields(log.Fields{
		"searchString": searchString,
	}).Info("Checking all fields for match with search string")
	for scanner.Scan() {
		rawRecord := scanner.Bytes()
		var record map[string]interface{}
		err := json.Unmarshal(rawRecord, &record)
		if err != nil {
			continue
		}
		recordString, ok := record[searchField].(string)
		if ok && recordString == searchString {
			return rawRecord, nil
		}

		recordInt, ok := record[searchField].(int)
		if ok {
			recordString = strconv.FormatInt(int64(recordInt), 10)
			if recordString == searchString {
				return rawRecord, nil
			}
		}
		recordFloat, ok := record[searchField].(float64)
		if ok {
			recordString = strconv.FormatInt(int64(recordFloat), 10)
			if recordString == searchString {
				return rawRecord, nil
			}
		}

		recordBool, ok := record[searchField].(bool)
		if ok {
			recordString = strconv.FormatBool(recordBool)
			if recordString == searchString {
				return rawRecord, nil
			}
		}
	}
	err := fmt.Errorf("Could not find record where '%s' == '%s' in chunk", searchField, searchString)
	log.WithError(err).Error("Error searching data chunk")
	return nil, err
}

func SearchRecordInDataChunk(chunk []byte, searchString string) ([]byte, error) {
	scanner := bufio.NewScanner(bytes.NewReader(chunk))
	scanner.Split(bufio.ScanLines)
	log.WithFields(log.Fields{
		"searchString": searchString,
	}).Info("Searching for first record  in data chunk to contain search string")
	for scanner.Scan() {
		rawRecord := scanner.Bytes()
		if strings.Contains(string(rawRecord), searchString) {
			return rawRecord, nil
		}
	}
	err := fmt.Errorf("Could not find record containing '%s' in chunk", searchString)
	log.WithError(err).Error("Error searching data chunk")
	return nil, err
}
