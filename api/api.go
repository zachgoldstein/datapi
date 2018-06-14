package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/blevesearch/bleve/search"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"

	"github.com/zachgoldstein/datatoapi/index"
	"github.com/zachgoldstein/datatoapi/storage"
)

type API struct {
	indexStore  *index.IndexStore
	realStorage storage.PhysicalStorer
}

func NewAPI(indexStore *index.IndexStore, realStorage storage.PhysicalStorer) *API {
	return &API{
		indexStore:  indexStore,
		realStorage: realStorage,
	}
}

func (api *API) Start(port int, indexStore *index.IndexStore, physStore storage.PhysicalStorer) error {
	r := mux.NewRouter()
	r.HandleFunc("/search/{search}", api.Search)
	r.HandleFunc("/many", api.GetMany)
	r.HandleFunc("/{field}/{value}", api.Get)

	addr := fmt.Sprintf(":%v", port)
	log.WithFields(log.Fields{
		"port": addr,
	}).Info("API Listening")

	return http.ListenAndServe(addr, r)
}

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

func (api *API) GetMany(w http.ResponseWriter, r *http.Request) {
	fmt.Println("GOT REQUEST TO GET MANY ITEMS")
}

func isJSON(str []byte) bool {
	var js json.RawMessage
	return json.Unmarshal(str, &js) == nil
}
