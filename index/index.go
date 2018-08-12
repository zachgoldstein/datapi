package index

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search"
	log "github.com/sirupsen/logrus"

	"github.com/zachgoldstein/datatoapi/models"
	"github.com/zachgoldstein/datatoapi/storage"
)

type IndexStorer interface {
	Start(path string) error
	CreateNewIndexes(searchPath, dataPath string) (searchIndex, dataIndex bleve.Index, err error)
	BuildDataMapping() (*mapping.IndexMappingImpl, error)
	BuildIndexes(searchIndex, dataIndex bleve.Index) error
	GetDataBlock(refKey string) (*models.DataBlock, error)
	GetSearchIndex(uid string) (*models.IndexData, error)
	buildSearchRequest(field, searchString string) *bleve.SearchRequest
	SearchHits(searchString string, params map[string][]string) (search.DocumentMatchCollection, error)
	GetHits(field, searchString string) (search.DocumentMatchCollection, error)
}

// DefaultChanSize defines the default channel size to use when processing indexes
const DefaultChanSize = storage.BLOCK_SIZE * 2

// IndexPrintFreq defines how frequently we should print out a message when indexing
const IndexPrintFreq = 30

// Status is created and sent on a status channel when each data record is indexed
type Status struct {
	ID     string
	Status string
}

// IndexingStatus is used to keep track of aggregate information about how indexing process is going
type IndexingStatus struct {
	Mutex          *sync.Mutex
	IndexesWritten uint64
}

// IndexStore keeps track of the indexes and an object to use when interacting with physical storage
//
type IndexStore struct {
	store       storage.PhysicalStorer
	dataIndex   bleve.Index
	searchIndex bleve.Index
}

// NewIndexStore creates an IndexStore pointer with a storage object
func NewIndexStore(store storage.PhysicalStorer) *IndexStore {
	return &IndexStore{
		store: store,
	}
}

// Start will open an existing index (or create one), making it available for searching
func (is *IndexStore) Start(path string) error {
	err := is.InitIndexes(path)
	if err != nil {
		return err
	}

	log.Info("Started index store")
	return nil
}

// InitIndexes will get or create an index with a path
func (is *IndexStore) InitIndexes(path string) error {
	dataPath := filepath.Join(path, "data")
	searchPath := filepath.Join(path, "search")
	// We only check for the data index to exist.
	// Assume either both or no indexes.
	searchIndex, err := bleve.Open(searchPath)
	dataIndex, err := bleve.Open(dataPath)
	if err != nil {
		log.WithFields(log.Fields{
			"path": path,
		}).Info("Could not find indexes")

		searchIndex, dataIndex, err = is.CreateNewIndexes(searchPath, dataPath)
		if err != nil {
			return err
		}
	} else {
		log.WithFields(log.Fields{
			"path": path,
		}).Info("Found indexes")
	}
	is.searchIndex = searchIndex
	is.dataIndex = dataIndex
	return nil
}

// CreateNewIndexes creates a new index, builds a mapping for this index and populates it with all data
func (is *IndexStore) CreateNewIndexes(searchPath, dataPath string) (searchIndex, dataIndex bleve.Index, err error) {
	mapping, err := is.BuildDataMapping()
	if err != nil {
		return nil, nil, err
	}

	searchIndex, err = bleve.New(searchPath, mapping)
	if err != nil {
		return nil, nil, err
	}

	dataBlockMapping := bleve.NewIndexMapping()
	dataMapping := bleve.NewDocumentMapping()
	dataBlockMapping.AddDocumentMapping("data", dataMapping)
	strFieldMapping := bleve.NewTextFieldMapping()
	dataMapping.AddFieldMappingsAt("RefKey", strFieldMapping)

	dataIndex, err = bleve.New(dataPath, dataBlockMapping)
	if err != nil {
		return nil, nil, err
	}

	err = is.BuildIndexes(searchIndex, dataIndex)
	if err != nil {
		return nil, nil, err
	}
	return searchIndex, dataIndex, nil
}

// BuildDataMapping builds an index mapping with all the data sent over a channel
func (is *IndexStore) BuildDataMapping() (*mapping.IndexMappingImpl, error) {
	log.Info("Building index mapping")

	dataChan := make(chan models.IndexData, DefaultChanSize)
	blockChan := make(chan models.DataBlock, DefaultChanSize)
	statusChan := make(chan interface{}, DefaultChanSize)
	go is.store.ScanDataBlocks(dataChan, blockChan)

	status := &IndexingStatus{
		Mutex:          &sync.Mutex{},
		IndexesWritten: uint64(0),
	}

	go LogStatusChannel(statusChan, status)

	indexMapping := bleve.NewIndexMapping()
	dataMapping := bleve.NewDocumentMapping()
	indexMapping.AddDocumentMapping("data", dataMapping)

	go func() {
		for _ = range blockChan {
			continue
		}
	}()
	const recordsNeededForMapping = 50
	recordsScanned := 0
	for data := range dataChan {
		if recordsScanned > recordsNeededForMapping {
			break
		}
		for k, v := range data.Data {
			_, ok := v.(string)
			if ok {
				strFieldMapping := bleve.NewTextFieldMapping()
				dataMapping.AddFieldMappingsAt(k, strFieldMapping)
				continue
			}

			_, ok = v.(int)
			if ok {
				intFieldMapping := bleve.NewNumericFieldMapping()
				dataMapping.AddFieldMappingsAt(k, intFieldMapping)
				continue
			}
			_, ok = v.(float32)
			if ok {
				floatFieldMapping := bleve.NewNumericFieldMapping()
				dataMapping.AddFieldMappingsAt(k, floatFieldMapping)
				continue
			}
			_, ok = v.(float64)
			if ok {
				floatFieldMapping := bleve.NewNumericFieldMapping()
				dataMapping.AddFieldMappingsAt(k, floatFieldMapping)
				continue
			}

			_, ok = v.(bool)
			if ok {
				boolFieldMapping := bleve.NewBooleanFieldMapping()
				dataMapping.AddFieldMappingsAt(k, boolFieldMapping)
				continue
			}
		}
		recordsScanned++
	}

	log.WithFields(log.Fields{
		"numIndexes": int(status.IndexesWritten),
	}).Info("Built Indexes")
	return indexMapping, nil
}

// LogStatusChannel logs status information passed into the status channel during indexing
func LogStatusChannel(statusChan chan interface{}, currStatus *IndexingStatus) {
	for status := range statusChan {
		switch s := status.(type) {
		case Status:
			currStatus.Mutex.Lock()
			atomic.AddUint64(&currStatus.IndexesWritten, 1)
			currStatus.Mutex.Unlock()
			if int(currStatus.IndexesWritten)%IndexPrintFreq == 0 {
				log.WithFields(log.Fields{
					"numIndexes": &currStatus.IndexesWritten,
				}).Info("Writing indexes...")
			}
		case error:
			log.WithError(s).Error("Encountered error creating index")
		}
	}
}

// BuildIndexes stores indexes for data sent over the channel
func (is *IndexStore) BuildIndexes(searchIndex, dataIndex bleve.Index) error {
	log.Info("Building indexes")

	dataChan := make(chan models.IndexData, DefaultChanSize)
	blockChan := make(chan models.DataBlock, DefaultChanSize)
	statusChan := make(chan interface{}, DefaultChanSize)
	go is.store.ScanDataBlocks(dataChan, blockChan)

	var wg sync.WaitGroup
	wg.Add(2)

	status := &IndexingStatus{
		Mutex:          &sync.Mutex{},
		IndexesWritten: uint64(0),
	}

	go LogStatusChannel(statusChan, status)
	go CreateIndexFromIndexDataChan(searchIndex, &wg, "mainIndex-%d", dataChan, statusChan)
	go CreateIndexFromDataBlockChan(dataIndex, &wg, "dataBlockIndex-%d", blockChan, statusChan)
	wg.Wait()

	log.WithFields(log.Fields{
		"numIndexes": int(status.IndexesWritten),
	}).Info("Built Indexes")

	return nil
}

// CreateIndexFromIndexDataChan wraps CreateIndexFromChan for search index models
func CreateIndexFromIndexDataChan(dataIndex bleve.Index, wg *sync.WaitGroup, idFormat string, dataChan chan models.IndexData, statusChan chan interface{}) {
	genericChan := make(chan interface{}, DefaultChanSize)
	go CreateIndexFromChan(dataIndex, wg, idFormat, genericChan, statusChan)
	for dataToIndex := range dataChan {
		genericChan <- dataToIndex
	}
	close(genericChan)
	log.Info("Finished writing search indexes")
}

// CreateIndexFromDataBlockChan wraps CreateIndexFromChan for data block models
func CreateIndexFromDataBlockChan(dataIndex bleve.Index, wg *sync.WaitGroup, idFormat string, dataChan chan models.DataBlock, statusChan chan interface{}) {
	genericChan := make(chan interface{}, DefaultChanSize)
	go CreateIndexFromChan(dataIndex, wg, idFormat, genericChan, statusChan)
	for dataToIndex := range dataChan {
		genericChan <- dataToIndex
	}
	close(genericChan)
	log.Info("Finished writing datablock indexes")
}

// CreateIndexFromChan will index data passed through a channel. It attaches a unique id to the data immediately before indexing
func CreateIndexFromChan(dataIndex bleve.Index, wg *sync.WaitGroup, idFormat string, dataChan chan interface{}, statusChan chan interface{}) {
	defer wg.Done()
	for dataToIndex := range dataChan {
		id := fmt.Sprintf(idFormat, time.Now().UnixNano())
		switch dataToIndex := dataToIndex.(type) {
		case models.IndexData:
			dataToIndex.UID = id
		case models.DataBlock:
			dataToIndex.UID = id
		}
		err := dataIndex.Index(id, dataToIndex)
		if err != nil {
			statusChan <- err
		}
		statusChan <- Status{
			ID:     id,
			Status: "Success",
		}
	}
}

// GetDataBlock retrieves a data block pointing at cloud storage for a given reference key
// all search indexes are created with a reference key that points at a data block key.
func (is *IndexStore) GetDataBlock(refKey string) (*models.DataBlock, error) {
	qs := fmt.Sprintf("RefKey:%s", refKey)
	log.WithFields(log.Fields{
		"querystring": qs,
	}).Info("Searching for datablock")

	query := bleve.NewQueryStringQuery(qs)
	search := bleve.NewSearchRequest(query)
	search.Fields = []string{"*"}
	searchResults, err := is.dataIndex.Search(search)
	if err != nil {
		log.WithError(err).Error("Could not find a search index")
		return nil, err
	}
	if len(searchResults.Hits) == 0 {
		err := fmt.Errorf("No search hits found for refkey: %s", refKey)
		log.WithError(err).Error("Could not find a data block index")
		return nil, err
	}
	fields := searchResults.Hits[0].Fields
	dataBlock := &models.DataBlock{
		RefKey: fields["RefKey"].(string),
		Start:  int64(fields["Start"].(float64)),
		End:    int64(fields["End"].(float64)),
		File: models.File{
			Address: fields["File.Address"].(string),
			Type:    fields["File.Type"].(string),
		},
	}

	return dataBlock, nil
}

// GetSearchIndex will retrieve a specific search index with it's uid
func (is *IndexStore) GetSearchIndex(uid string) (*models.IndexData, error) {
	query := bleve.NewDocIDQuery([]string{uid})
	search := bleve.NewSearchRequest(query)
	search.Fields = []string{"*"}
	searchResults, err := is.searchIndex.Search(search)
	if err != nil {
		log.WithError(err).Error("Could not find a search index")
		return nil, err
	}
	if len(searchResults.Hits) == 0 {
		err := fmt.Errorf("No search hits found for uid %s", uid)
		log.WithError(err).Error("Could not find a search index")
		return nil, err
	}
	hit := searchResults.Hits[0]

	data := map[string]interface{}{}
	for k, v := range hit.Fields {
		if strings.Contains(k, "Data") {
			parts := strings.Split(k, ".")
			fieldName := parts[1]
			data[fieldName] = v
		}
	}

	log.WithFields(log.Fields{
		"uid": uid,
	}).Info("Found search index")

	return &models.IndexData{
		UID:    hit.Fields["UID"].(string),
		Data:   data,
		RefKey: hit.Fields["RefKey"].(string),
	}, nil
}

func (is *IndexStore) buildSearchRequest(field, searchString string) *bleve.SearchRequest {
	searchFloat, err := strconv.ParseFloat(searchString, 64)
	truePtr := true
	if err == nil {
		log.WithFields(log.Fields{
			"searchFloat": searchFloat,
		}).Info("Finding numeric range")
		query := bleve.NewNumericRangeInclusiveQuery(&searchFloat, &searchFloat, &truePtr, &truePtr)
		query.SetField(fmt.Sprintf("Data.%s", field))
		search := bleve.NewSearchRequest(query)
		search.Fields = []string{"*"}
		return search
	}
	searchInt, err := strconv.ParseInt(searchString, 10, 64)
	if err == nil {
		searchFloat := float64(searchInt)
		log.WithFields(log.Fields{
			"searchFloat": searchFloat,
		}).Info("Finding numeric range from int with float")

		query := bleve.NewNumericRangeInclusiveQuery(&searchFloat, &searchFloat, &truePtr, &truePtr)
		query.SetField(fmt.Sprintf("Data.%s", field))
		search := bleve.NewSearchRequest(query)
		search.Fields = []string{"*"}
		return search
	}
	searchBool, err := strconv.ParseBool(searchString)
	if err == nil {
		log.WithFields(log.Fields{
			"searchBool": searchBool,
		}).Info("Finding bool")
		query := bleve.NewBoolFieldQuery(searchBool)
		query.SetField(fmt.Sprintf("Data.%s", field))
		search := bleve.NewSearchRequest(query)
		search.Fields = []string{"*"}
		return search
	}

	searchString = strings.Replace(searchString, " ", `\ `, -1)
	qs := fmt.Sprintf("Data.%s:%s", field, searchString)
	log.WithFields(log.Fields{
		"searchString": searchString,
	}).Info("Searching with string")
	query := bleve.NewQueryStringQuery(qs)
	search := bleve.NewSearchRequest(query)
	search.Fields = []string{"*"}
	return search
}

// SearchHits checks all fields in all records for results that contain a search string.
// Used for requests of the form /search/{search}
func (is *IndexStore) SearchHits(searchString string, params map[string][]string) (search.DocumentMatchCollection, error) {
	log.WithFields(log.Fields{
		"searchString": searchString,
	}).Info("Searching for hits")
	searchString = strings.Replace(searchString, " ", `\ `, -1)
	query := bleve.NewMatchPhraseQuery(searchString)
	searchReq := bleve.NewSearchRequest(query)
	searchReq.Fields = []string{"*"}

	searchResults, err := is.searchIndex.Search(searchReq)
	if err != nil {
		log.WithError(err).Error("Error finding search Index")
		return nil, err
	}
	if len(searchResults.Hits) == 0 {
		err := fmt.Errorf("No search hits found")
		log.WithError(err).Error("Could not find a data block index")
		return nil, err
	}
	return searchResults.Hits, nil
}

// GetHits will find results where a specific field matches a search string.
// Used for requests of the form /{field}/{value}
func (is *IndexStore) GetHits(field, searchString string) (search.DocumentMatchCollection, error) {
	log.WithFields(log.Fields{
		"searchString": searchString,
		"field":        field,
	}).Info("Retrieving hits")
	searchReq := is.buildSearchRequest(field, searchString)
	searchResults, err := is.searchIndex.Search(searchReq)
	if err != nil {
		log.WithError(err).Error("Error finding search Index")
		return nil, err
	}
	if len(searchResults.Hits) == 0 {
		err := fmt.Errorf("No search hits found")
		log.WithError(err).Error("Could not find a data block index")
		return nil, err
	}
	return searchResults.Hits, nil
}
