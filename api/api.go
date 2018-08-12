package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/blevesearch/bleve/search"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"

	"github.com/zachgoldstein/datatoapi/index"
	"github.com/zachgoldstein/datatoapi/storage"
)

// API starts an http server that interacts with an index store and an interface to data storage
// in the cloud. It serves requests for specific fields
type API struct {
	indexStore  *index.IndexStore
	realStorage storage.PhysicalStorer
}

// NewAPI creates an instance of API
func NewAPI(indexStore *index.IndexStore, realStorage storage.PhysicalStorer) *API {
	return &API{
		indexStore:  indexStore,
		realStorage: realStorage,
	}
}

// Start creates our http server and starts listening for requests on a port
func (api *API) Start(port int, indexStore *index.IndexStore, physStore storage.PhysicalStorer) error {
	r := mux.NewRouter()
	r.HandleFunc("/search/{search}", api.Search)
	r.HandleFunc("/{field}/{value}", api.Get)
	r.HandleFunc("/all/{field}/{value}", api.All)

	addr := fmt.Sprintf(":%v", port)
	log.WithFields(log.Fields{
		"port": addr,
	}).Info("API Listening")

	return http.ListenAndServe(addr, r)
}

// Search will return the closest json result, looking through all fields for values
// that contain the search string.
// It will take the closest search result, retrieve the associated data block index,
// then use this to retrieve the chunk of raw data from cloud storage. It will search
// this chunk for the record we're interested in, then return that in json format.
func (api *API) Search(w http.ResponseWriter, r *http.Request) {
	log.Info("API Searching for results")
	vars := mux.Vars(r)

	hits, err := api.indexStore.SearchHits(vars["search"], r.URL.Query())
	if err != nil {
		log.WithError(err).Error("Could not find index hits")
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	log.WithFields(log.Fields{
		"hits": len(hits),
	}).Info("Retrieved hits")

	blockBytes, err := api.getDataBlockBytes(w, hits[0])
	if err != nil {
		log.WithError(err).Error("Could not get data block bytes")
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	fullRecord, err := storage.SearchRecordInDataChunk(blockBytes, vars["search"])
	if err != nil {
		log.WithError(err).Error("Could not find record in data chunk")
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if !isJSON(fullRecord) {
		err = fmt.Errorf("Retrieved record but data is malformed")
		log.WithError(err).WithFields(log.Fields{
			"record": fullRecord,
		}).Error("Retrieved record but data is malformed")
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Write([]byte(fullRecord))
}

// Get will return the closest json result, looking a specific field for values
// that contain the search string.
// It will take the closest search result, retrieve the associated data block index,
// then use this to retrieve the chunk of raw data from cloud storage. It will search
// this chunk for the record we're interested in, then return that in json format.
func (api *API) Get(w http.ResponseWriter, r *http.Request) {
	log.Info("API Retrieving results for field:value")

	vars := mux.Vars(r)

	hits, err := api.indexStore.GetHits(vars["field"], vars["value"])
	if err != nil {
		log.WithError(err).Error("Could not find index hits")
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	log.WithFields(log.Fields{
		"hits": len(hits),
	}).Info("Retrieved hits")
	blockBytes, err := api.getDataBlockBytes(w, hits[0])
	if err != nil {
		log.WithError(err).Error("Could not get data block bytes")
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	fullRecord, err := storage.GetRecordInDataChunk(blockBytes, vars["field"], vars["value"])
	if err != nil {
		log.WithError(err).Error("Could not get record in data chunk")
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if !isJSON(fullRecord) {
		err = fmt.Errorf("Retrieved record but data is malformed")
		log.WithError(err).WithFields(log.Fields{
			"record": fullRecord,
		}).Error("Retrieved record but data is malformed")
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Write([]byte(fullRecord))
}

// All will return all json results, looking for a specific field for values
// that contain the search string.
// It will take all search results, retrieve the associated data block indexes,
// then use this to retrieve the chunks of raw data from cloud storage. It will search
// this chunk for the record we're interested in, then return that in json format.
func (api *API) All(w http.ResponseWriter, r *http.Request) {
	log.Info("API Retrieving all results for field:value")

	vars := mux.Vars(r)

	hits, err := api.indexStore.GetHits(vars["field"], vars["value"])
	if err != nil {
		log.WithError(err).Error("Could not find index hits")
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	log.WithFields(log.Fields{
		"hits": len(hits),
	}).Info("Retrieved hits")

	records := [][]byte{}
	for _, hit := range hits {
		blockBytes, err := api.getDataBlockBytes(w, hit)
		if err != nil {
			log.WithError(err).Error("Could not get data block bytes")
			continue
		}
		fullRecord, err := storage.GetRecordInDataChunk(blockBytes, vars["field"], vars["value"])
		if err != nil {
			log.WithError(err).Error("Could not get record in data chunk")
			continue
		}
		if !isJSON(fullRecord) {
			err = fmt.Errorf("Retrieved record but data is malformed")
			log.WithError(err).WithFields(log.Fields{
				"record": fullRecord,
			}).Error("Retrieved record but data is malformed")
			continue
		}
		records = append(records, fullRecord)
	}
	log.WithFields(log.Fields{
		"hits": len(records),
	}).Info("Combing records")

	combinedRecords := bytes.Join(records, []byte(`,`))

	// insert '[' to the front
	combinedRecords = append(combinedRecords, 0)
	copy(combinedRecords[1:], combinedRecords[0:])
	combinedRecords[0] = byte('[')

	// append ']'
	combinedRecords = append(combinedRecords, ']')

	w.Write(combinedRecords)
}

func (api *API) getDataBlockBytes(w http.ResponseWriter, hit *search.DocumentMatch) ([]byte, error) {
	// Get the chunk of data containing the record we're interested in
	_, ok := hit.Fields["RefKey"]
	if !ok {
		err := fmt.Errorf("Could not find refKey in search hit: %v", hit)
		log.WithError(err).WithFields(log.Fields{
			"hit": hit,
		}).Error("Could not find refKey in search hit")
		return nil, err
	}
	refKey := hit.Fields["RefKey"].(string)
	dataBlock, err := api.indexStore.GetDataBlock(refKey)
	if err != nil {
		log.WithError(err).Error("Could not get data block")
		return nil, err
	}

	blockBytes, err := api.realStorage.RetrieveDataBlockBytes(dataBlock)
	if err != nil {
		log.WithError(err).Error("Could not retrieve data block bytes")
		return nil, err
	}
	return blockBytes, nil
}

func isJSON(str []byte) bool {
	var js json.RawMessage
	return json.Unmarshal(str, &js) == nil
}
