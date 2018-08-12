package engine

import (
	"reflect"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/zachgoldstein/datatoapi/api"
	"github.com/zachgoldstein/datatoapi/index"
	"github.com/zachgoldstein/datatoapi/storage"
)

// Engine uses the indexing store and the underlying storage to fulfil
// requests for data
type Engine struct {
	indexStore  *index.IndexStore
	realStorage storage.PhysicalStorer
	api         *api.API
	config      Config
}

type Config struct {
	IndexPath   string
	StoragePath string
	Port        int
}

// NewEngine creates an instance of Engine
func NewEngine() *Engine {
	return &Engine{}
}

// Start will start up
func (eng *Engine) Start(config Config) error {
	eng.config = config

	eng.realStorage = detectStorageType(eng.config.StoragePath)
	eng.indexStore = index.NewIndexStore(eng.realStorage)
	eng.api = api.NewAPI(eng.indexStore, eng.realStorage)

	log.WithFields(log.Fields{
		"storage": reflect.TypeOf(eng.realStorage),
	}).Info("Starting engine with storage")

	err := eng.realStorage.Start(eng.config.StoragePath, map[string]interface{}{})
	if err != nil {
		log.Panic(err)
	}
	err = eng.indexStore.Start(eng.config.IndexPath)
	if err != nil {
		log.Panic(err)
	}
	err = eng.api.Start(eng.config.Port, eng.indexStore, eng.realStorage)
	if err != nil {
		log.Panic(err)
	}
	return nil
}

func detectStorageType(storagePath string) storage.PhysicalStorer {
	if strings.Contains(storagePath, "https://s3.amazonaws.com") {
		return storage.NewAWSFS()
	} else {
		return storage.NewLocalFS()
	}

}
