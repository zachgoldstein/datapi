package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/asdine/storm"
	"github.com/asdine/storm/q"
	"github.com/gorilla/mux"
)

type Datastore interface {
	GetOneIndex(schemaItem schemaItem, value interface{}) (*DummyIndex, error)
}

type DB struct {
	*storm.DB
}

type Env struct {
	db Datastore
}

type schemaItem struct {
	Searchable bool   `json:"searchable"`
	Optional   bool   `json:"optional"`
	Type       string `json:"type"`
	Name       string `json:"name"`
}

var dummyJSONSchema = map[string]schemaItem{
	"ID": schemaItem{
		Searchable: true,
		Optional:   false,
		Type:       "int64",
		Name:       "ID",
	},
	"Name": schemaItem{
		Searchable: true,
		Optional:   false,
		Type:       "string",
		Name:       "Name",
	},
	"Date": schemaItem{
		Searchable: true,
		Optional:   false,
		Type:       "date",
		Name:       "Date",
	},
	"TotalPlumbuses": schemaItem{
		Searchable: true,
		Optional:   false,
		Type:       "int32",
		Name:       "TotalPlumbuses",
	},
	"HasExistentialIdentityCrisis": schemaItem{
		Searchable: true,
		Optional:   false,
		Type:       "boolean",
		Name:       "HasExistentialIdentityCrisis",
	},

	"Address": schemaItem{
		Searchable: false,
		Optional:   true,
		Type:       "string",
		Name:       "Address",
	},
	"Text": schemaItem{
		Searchable: false,
		Optional:   true,
		Type:       "string",
		Name:       "Text",
	},
	"Job": schemaItem{
		Searchable: false,
		Optional:   true,
		Type:       "string",
		Name:       "Job",
	},
	"PhoneNumber": schemaItem{
		Searchable: false,
		Optional:   true,
		Type:       "string",
		Name:       "PhoneNumber",
	},
	"FavoriteColor": schemaItem{
		Searchable: false,
		Optional:   true,
		Type:       "string",
		Name:       "FavoriteColor",
	},
	"Company": schemaItem{
		Searchable: false,
		Optional:   true,
		Type:       "string",
		Name:       "Company",
	},
	"CompanyCatchPhrase": schemaItem{
		Searchable: false,
		Optional:   true,
		Type:       "string",
		Name:       "CompanyCatchPhrase",
	},
	"CompanyBS": schemaItem{
		Searchable: false,
		Optional:   true,
		Type:       "string",
		Name:       "CompanyBS",
	},
	"Username": schemaItem{
		Searchable: false,
		Optional:   true,
		Type:       "string",
		Name:       "Username",
	},
}

type DataBlock struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
	File  File
}

type File struct {
	Address string `json:"address"`
	Type    string `json:"type"`
}

type DummyIndex struct {
	DataBlock DataBlock
	// Below are eventually going to be generated values
	ID                           int    `storm:"id" json:"id"` // primary key
	Name                         string `storm:"index" json:"name"`
	Date                         string `storm:"unique" json:"date"`
	TotalPlumbuses               int    `storm:"index" json:"total_plumbuses"`
	HasExistentialIdentityCrisis bool   `storm:"index" json:"has_existential_identity_crisis"`

	// Below are optional fields
	Address            string `json:"address,omitempty"`
	Text               string `json:"text,omitempty"`
	Job                string `json:"job,omitempty"`
	PhoneNumber        string `json:"phone_number,omitempty"`
	FavoriteColor      string `json:"favorite_color,omitempty"`
	Company            string `json:"company,omitempty"`
	CompanyCatchPhrase string `json:"company_catch_phrase,omitempty"`
	CompanyBS          string `json:"company_bs,omitempty"`
	Username           string `json:"username,omitempty"`
}

func (db *DB) GetOneIndex(schemaItem schemaItem, value interface{}) (*DummyIndex, error) {
	if schemaItem.Searchable == false {
		return nil, fmt.Errorf("Field in schema is not searchable: %s", schemaItem.Name)
	}

	var retrievedIndex DummyIndex
	// cast value to some type.... TEMP doing this for integer value
	val := value.(string)
	intValue, err := strconv.Atoi(val)
	if err != nil {
		return &DummyIndex{}, fmt.Errorf("Couldn't parse int from string: %s", err)
	}

	err = db.One(schemaItem.Name, intValue, &retrievedIndex)
	if err != nil {
		return &DummyIndex{}, fmt.Errorf("Couldn't retrieve index from bolt: %s", err)
	}
	return &retrievedIndex, nil
}

func RetrieveActualData(dataBlock DataBlock) (fullRecord string, err error) {
	f, err := os.Open("./data/data.jsonfiles")
	if err != nil {
		return "", err
	}
	o2, err := f.Seek(dataBlock.Start, 0)
	if err != nil {
		return "", err
	}
	byteLength := dataBlock.End - dataBlock.Start
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

func (env *Env) GetOne(w http.ResponseWriter, r *http.Request) {
	// Find request field

	queryField := r.URL.Query().Get("query")
	if queryField == "" {
		err := fmt.Errorf("Expected request to have a query request parameter: %s", r.URL.Query().Encode())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	reqField := queryField
	schemaItem, ok := dummyJSONSchema[reqField]
	if !ok {
		err := fmt.Errorf("Request field not found in schema: %s", reqField)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	queryValue := r.URL.Query().Get("value")
	if queryValue == "" {
		err := fmt.Errorf("Expected request to have a query value parameter: %s", r.URL.Query().Encode())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	index, err := env.db.GetOneIndex(schemaItem, queryValue)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fullRecord, err := RetrieveActualData(index.DataBlock)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Write([]byte(fullRecord))
}

func main() {
	fmt.Println("Starting datatoapi")

	////////// CREATING INDEXES

	db, err := storm.Open("datatoapi.db")
	if err != nil {
		fmt.Println(fmt.Errorf("An error occurred opening the database: %s", err))
	}
	defer db.Close()
	envDB := &DB{db}
	env := &Env{envDB}

	f, err := os.Open("./data/data.jsonfiles")
	if err != nil {
		fmt.Println(fmt.Errorf("Couldn't find data file: %s", err))
	}
	scanner := bufio.NewScanner(f)
	// Create a custom split function by wrapping the existing ScanWords function.
	currentPos := int64(0)
	prevPos := int64(0)
	split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		advance, token, err = bufio.ScanLines(data, atEOF)
		prevPos = currentPos
		currentPos += int64(advance)

		return
	}
	// Set the split function for the scanning operation.
	scanner.Split(split)
	// Validate the input
	// for scanner.Scan() {
	// fmt.Printf("%s\n", scanner.Text())
	// }

	for scanner.Scan() {
		idx := DummyIndex{}
		txt := scanner.Text()
		err = json.Unmarshal([]byte(txt), &idx)
		if err != nil {
			fmt.Println(fmt.Errorf("Couldn't unmarshal text: %s", txt))
		}
		idx.DataBlock = DataBlock{
			Start: prevPos,
			End:   currentPos,
			File: File{
				Address: "./data/data.jsonfiles",
				Type:    "LocalFS",
			},
		}
		// fmt.Printf("Creating index %#v \n", idx)
		err = db.Save(&idx)
		if err != nil {
			fmt.Println(fmt.Errorf("Couldn't save index to bolt: %s", err))
		}
		// fmt.Println(idx)
	}

	if err = scanner.Err(); err != nil {
		fmt.Printf("Error scanning data: %s \n", err)
	}

	////////// RETRIEVING INDEXES

	var retrievedIndex DummyIndex
	err = db.One("ID", 1000100, &retrievedIndex)
	if err != nil {
		fmt.Println(fmt.Errorf("Couldn't retrieve index from bolt: %s", err))
	}

	fmt.Println("Retrieved Index", retrievedIndex)

	var idxs []DummyIndex
	err = db.Select(
		q.Gte("TotalPlumbuses", 500000),
		q.Lte("TotalPlumbuses", 510000),
	).Find(&idxs)
	if err != nil {
		fmt.Println(fmt.Errorf("Couldn't retrieve indexes from bolt: %s", err))
	}
	fmt.Println("Retrieving indexes with select gte & lte...")
	for _, retreivedIdx := range idxs {
		fmt.Println("Retrieved Index", retreivedIdx)
	}

	////////// Retrieve full record

	f, err = os.Open("./data/data.jsonfiles")
	if err != nil {
		fmt.Println("huh?")
	}
	o2, err := f.Seek(retrievedIndex.DataBlock.Start, 0)
	if err != nil {
		fmt.Println("huh?")
	}
	byteLength := retrievedIndex.DataBlock.End - retrievedIndex.DataBlock.Start
	b2 := make([]byte, byteLength)
	n2, err := f.Read(b2)
	if err != nil {
		fmt.Println("huh?")
	}
	fmt.Printf("%d bytes @ %d: %s\n", n2, o2, string(b2))
	fullRecord := DummyIndex{}
	err = json.Unmarshal(b2, &fullRecord)
	if err != nil {
		fmt.Println("Could not unmarshal full record")
	}
	fmt.Printf("Retrieved full record %#v \n", fullRecord)
	exportedRecord, err := json.Marshal(fullRecord)
	if err != nil {
		fmt.Println("Could not marshal exported record")
	}
	fmt.Printf("Marshalled full record %s \n", string(exportedRecord))

	// Start API based on our schema
	r := mux.NewRouter()
	// Routes consist of a path and a handler function.
	r.HandleFunc("/", env.GetOne)

	// Bind to a port and pass our router in
	fmt.Println("Listening on port 8123")
	log.Fatal(http.ListenAndServe(":8123", r))

	// fmt.Println("Creating indexes...")
	// fmt.Println("50 indexes of 1000 created")
	// fmt.Println("Created indexes")
}
