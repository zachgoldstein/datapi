package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/zachgoldstein/datatoapi/storage"
	"github.com/zachgoldstein/datatoapi/store"
)

type Env struct {
	DB    store.Datastore
	Store storage.Storage
}

var dummyJSONSchema = map[string]store.SchemaItem{
	"ID": store.SchemaItem{
		Searchable: true,
		Optional:   false,
		Type:       "int64",
		Name:       "ID",
	},
	"Name": store.SchemaItem{
		Searchable: true,
		Optional:   false,
		Type:       "string",
		Name:       "Name",
	},
	"Date": store.SchemaItem{
		Searchable: true,
		Optional:   false,
		Type:       "date",
		Name:       "Date",
	},
	"TotalPlumbuses": store.SchemaItem{
		Searchable: true,
		Optional:   false,
		Type:       "int32",
		Name:       "TotalPlumbuses",
	},
	"Distance": store.SchemaItem{
		Searchable: true,
		Optional:   false,
		Type:       "float64",
		Name:       "Distance",
	},
	"HasExistentialIdentityCrisis": store.SchemaItem{
		Searchable: true,
		Optional:   false,
		Type:       "string",
		Name:       "HasExistentialIdentityCrisis",
	},

	"Address": store.SchemaItem{
		Searchable: false,
		Optional:   true,
		Type:       "string",
		Name:       "Address",
	},
	"Text": store.SchemaItem{
		Searchable: false,
		Optional:   true,
		Type:       "string",
		Name:       "Text",
	},
	"Job": store.SchemaItem{
		Searchable: false,
		Optional:   true,
		Type:       "string",
		Name:       "Job",
	},
	"PhoneNumber": store.SchemaItem{
		Searchable: false,
		Optional:   true,
		Type:       "string",
		Name:       "PhoneNumber",
	},
	"FavoriteColor": store.SchemaItem{
		Searchable: false,
		Optional:   true,
		Type:       "string",
		Name:       "FavoriteColor",
	},
	"Company": store.SchemaItem{
		Searchable: false,
		Optional:   true,
		Type:       "string",
		Name:       "Company",
	},
	"CompanyCatchPhrase": store.SchemaItem{
		Searchable: false,
		Optional:   true,
		Type:       "string",
		Name:       "CompanyCatchPhrase",
	},
	"CompanyBS": store.SchemaItem{
		Searchable: false,
		Optional:   true,
		Type:       "string",
		Name:       "CompanyBS",
	},
	"Username": store.SchemaItem{
		Searchable: false,
		Optional:   true,
		Type:       "string",
		Name:       "Username",
	},
}

func isJSON(str string) bool {
	var js json.RawMessage
	return json.Unmarshal([]byte(str), &js) == nil
}

func GetSchemaItem(r *http.Request) (store.SchemaItem, error) {
	queryField := r.URL.Query().Get("query")
	if queryField == "" {
		return store.SchemaItem{}, fmt.Errorf("Expected request to have a query request parameter: %s", r.URL.Query().Encode())
	}
	reqField := queryField
	schemaItem, ok := dummyJSONSchema[reqField]
	if !ok {
		return store.SchemaItem{}, fmt.Errorf("Request field not found in schema: %s", reqField)
	}
	return schemaItem, nil
}

func GetQueryValue(r *http.Request) (string, error) {
	queryValue := r.URL.Query().Get("value")
	if queryValue == "" {
		return "", fmt.Errorf("Expected request to have a query value parameter: %s", r.URL.Query().Encode())
	}
	return queryValue, nil
}

func (env *Env) GetMany(w http.ResponseWriter, r *http.Request) {
	schemaItem, err := GetSchemaItem(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	queryValue, err := GetQueryValue(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	index, err := env.DB.GetOneIndex(schemaItem, queryValue)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fullRecord, err := env.Store.RetrieveData(index.DataBlock)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if !isJSON(fullRecord) {
		err := fmt.Errorf("Retrieved record but data is malformed:\n%s", fullRecord)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte(fullRecord))
}

// GetOne retrieves and returns a single record from the store
func (env *Env) GetOne(w http.ResponseWriter, r *http.Request) {
	schemaItem, err := GetSchemaItem(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	queryValue, err := GetQueryValue(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	index, err := env.DB.GetOneIndex(schemaItem, queryValue)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fullRecord, err := env.Store.RetrieveData(index.DataBlock)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if !isJSON(fullRecord) {
		err := fmt.Errorf("Retrieved record but data is malformed:\n%s", fullRecord)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte(fullRecord))
}

// GetMany retrieves a set of results
// Multiple possible values could be requested, or our request could have multiple matching records
// Skip, Limit and Reverse, gt, ls, sort
// func (env *Env) GetMany(w http.ResponseWriter, r *http.Request) {}
