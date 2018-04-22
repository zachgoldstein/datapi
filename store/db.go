package store

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/asdine/storm"
	"github.com/asdine/storm/codec/msgpack"
	"github.com/coreos/bbolt"
)

type Datastore interface {
	GetOneIndex(schemaItem SchemaItem, value interface{}) (*Index, error)
	GetManyIndexes(schemaItem SchemaItem, values []interface{}) ([]*Index, error)
	SaveIndex(data *DummyIndexData, block *DataBlock) error
}

type DB struct {
	*storm.DB
}

type SchemaItem struct {
	Searchable bool   `json:"searchable"`
	Optional   bool   `json:"optional"`
	Type       string `json:"type"`
	Name       string `json:"name"`
}

type DataBlock struct {
	Pk       int    `storm:"id,increment"`
	RefIndex uint32 `storm:"unique"`
	Start    int64
	End      int64
	File     File
}

type File struct {
	Address string `json:"address"`
	Type    string `json:"type"`
}

type DummyIndexData struct {
	// Below are eventually going to be generated values
	Pk                           uint32  `storm:"id"` // primary key
	ID                           int     `storm:"index" json:"ID"`
	Name                         string  `storm:"index" json:"name"`
	Date                         string  `storm:"index" json:"date"`
	TotalPlumbuses               int     `storm:"index" json:"total_plumbuses"`
	Distance                     float64 `storm:"index" json:"distance"`
	HasExistentialIdentityCrisis bool    `storm:"index" json:"has_existential_identity_crisis"` // BOOL indexes are broken

	// Below are optional fields
	// Address            string `json:"address,omitempty"`
	// Text               string `json:"text,omitempty"`
	// Job                string `json:"job,omitempty"`
	// PhoneNumber        string `json:"phone_number,omitempty"`
	// FavoriteColor      string `json:"favorite_color,omitempty"`
	// Company            string `json:"company,omitempty"`
	// CompanyCatchPhrase string `json:"company_catch_phrase,omitempty"`
	// CompanyBS          string `json:"company_bs,omitempty"`
	// Username           string `json:"username,omitempty"`
}

type Index struct {
	Data      DummyIndexData `storm:"inline"`
	DataBlock DataBlock
}

func OpenDatabase(location string) (*DB, error) {
	db, err := storm.Open(
		location,
		storm.Codec(msgpack.Codec),
		// storm.Batch(),
		storm.BoltOptions(0600, &bolt.Options{
			Timeout:        1 * time.Second,
			NoFreelistSync: true,
		}),
	)
	// db, err := storm.Open(location)
	if err != nil {
		return nil, fmt.Errorf("An error occurred opening the database: %s", err)
	}
	return &DB{db}, nil
}

func convertToSchemaType(typeName, stringValue string) (interface{}, error) {
	if typeName == "string" {
		return stringValue, nil
	} else if typeName == "int64" {
		int64Value, convErr := strconv.ParseInt(stringValue, 10, 64)
		if convErr != nil {
			return &Index{}, fmt.Errorf("Couldn't cast value to int64: %s", convErr)
		}
		return int64Value, nil
	} else if typeName == "int32" {
		int32Value, convErr := strconv.ParseInt(stringValue, 10, 32)
		if convErr != nil {
			return &Index{}, fmt.Errorf("Couldn't cast value to int64: %s", convErr)
		}
		return int32Value, nil
	} else if typeName == "float32" {
		float32Value, convErr := strconv.ParseFloat(stringValue, 32)
		if convErr != nil {
			return &Index{}, fmt.Errorf("Couldn't cast value to float32: %s", convErr)
		}
		return float32Value, nil
	} else if typeName == "float64" {
		float64Value, convErr := strconv.ParseFloat(stringValue, 64)
		if convErr != nil {
			return &Index{}, fmt.Errorf("Couldn't cast value to float64: %s", convErr)
		}
		return float64Value, nil
	} else if typeName == "date" {
		// possibleDateLayout := []string{
		// 	time.ANSIC,
		// 	time.UnixDate,
		// 	time.RubyDate,
		// 	time.RFC822,
		// 	time.RFC822Z,
		// 	time.RFC850,
		// 	time.RFC1123,
		// 	time.RFC1123Z,
		// 	time.RFC3339,
		// 	time.RFC3339Nano,
		// 	time.Kitchen,
		// 	time.Stamp,
		// 	time.StampMilli,
		// 	time.StampMicro,
		// 	time.StampNano,
		// 	"2006-01-02",
		// 	"2018-03-06T18:16:38.684434",
		// 	"2006-01-02T15:04:05.999999",
		// 	"2006-01-02T15:04:05.999:00",
		// }
		// var convErr error
		// var dateValue time.Time
		// for _, dateLayout := range possibleDateLayout {
		// 	dateValue, convErr = time.Parse(dateLayout, stringValue)
		// 	if convErr == nil {
		// 		fmt.Printf("found date in format %s \n", dateLayout)
		// 		break
		// 	}
		// }
		// if convErr != nil {
		// 	return &Index{}, fmt.Errorf("Couldn't cast value to date: %s", convErr)
		// }
		// err = db.One(schemaItem.Name, dateValue, &indexData)
		return stringValue, nil
	} else if typeName == "boolean" {
		boolValue, convErr := strconv.ParseBool(stringValue)
		if convErr != nil {
			return &Index{}, fmt.Errorf("Couldn't cast value to boolean: %s", convErr)
		}
		fmt.Printf("Looking for %s with %t \n", stringValue, boolValue)
		return boolValue, nil
	}
	return nil, fmt.Errorf("Couldn't find a type to convert value into: %s", stringValue)
}

func (db *DB) GetManyIndexes(schemaItem SchemaItem, values []interface{}) ([]*Index, error) {
	indexes := []*Index{}
	for v := range values {
		index, err := db.GetOneIndex(schemaItem, v)
		if err != nil {
			fmt.Printf("Error encountered looking for index, continuing. Value: %#v, Error: %s \n", v, err)
			continue
		}
		indexes = append(indexes, index)
	}
	if len(indexes) == 0 {
		return indexes, fmt.Errorf("No indexes found")
	}
	return indexes, nil
}

func (db *DB) GetOneIndex(schemaItem SchemaItem, value interface{}) (*Index, error) {
	if schemaItem.Searchable == false {
		return nil, fmt.Errorf("Field in schema is not searchable: %s", schemaItem.Name)
	}

	stringValue, ok := value.(string)
	if !ok {
		return &Index{}, fmt.Errorf("Could not convert value to string: %s", value)
	}

	var indexData DummyIndexData
	val, err := convertToSchemaType(schemaItem.Type, stringValue)
	if err != nil {
		return &Index{}, fmt.Errorf("Couldn't convert to schema type: %s", err)
	}
	err = db.One(schemaItem.Name, val, &indexData)
	if err != nil {
		return &Index{}, fmt.Errorf("Couldn't retrieve index from bolt: %s", err)
	}

	fmt.Printf("Retrieved %#v \n", indexData)
	var indexDataBlock DataBlock
	err = db.One("RefIndex", indexData.Pk, &indexDataBlock)
	if err != nil {
		return &Index{}, fmt.Errorf("Couldn't retrieve data block from bolt: %s", err)
	}
	fmt.Printf("Retrieved %#v \n", indexDataBlock)

	return &Index{
		Data:      indexData,
		DataBlock: indexDataBlock,
	}, nil
}

func (db *DB) SaveIndex(data *DummyIndexData, block *DataBlock) error {
	timestamp := time.Now().Unix()
	indexKey := fmt.Sprintf("%s-%d-%d-%d", block.File.Address, block.Start, block.End, timestamp)
	h := fnv.New32a()
	h.Write([]byte(indexKey))

	data.Pk = h.Sum32()
	fmt.Printf("Saving this data %#v \n", data)
	err := db.Save(data)
	if err != nil {
		return err
	}

	block.RefIndex = h.Sum32()
	return db.Save(block)
}

// func (db *DB) SaveIndex(data *interface{}, block *DataBlock) error {
// 	t := reflect.TypeOf(data)
// 	for i := 0; i < t.NumField(); i++ {
// 		// Get the field, returns https://golang.org/pkg/reflect/#StructField
// 		field := t.Field(i)
// 		field.Tag = "`json:name-field`"
// 		// FIX ME, ADD STORM TAG HERE
//
// 		// fmt.Printf("%d. %v (%v), tag: '%v'\n", i+1, field.Name, field.Type.Name(), tag)
// 		fmt.Printf("%d. %v (%v) \n", i+1, field.Name, field.Type.Name())
// 	}
//
// 	timestamp := time.Now().Unix()
// 	indexKey := fmt.Sprintf("%s-%d-%d-%d", block.File.Address, block.Start, block.End, timestamp)
// 	h := fnv.New32a()
// 	h.Write([]byte(indexKey))
//
// 	data.Pk = h.Sum32()
// 	fmt.Printf("Saving this data %#v \n", data)
// 	err := db.Save(data)
// 	if err != nil {
// 		return err
// 	}
//
// 	block.RefIndex = h.Sum32()
// 	return db.Save(block)
// }
