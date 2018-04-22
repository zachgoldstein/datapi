package main

import (
	"fmt"
	"log"

	"github.com/gorilla/mux"

	"github.com/zachgoldstein/datatoapi/api"
	"github.com/zachgoldstein/datatoapi/storage"
	"github.com/zachgoldstein/datatoapi/store"

	"net/http"
	_ "net/http/pprof"
)

const DBLocation = "datatoapi.db"
const FSLocation = "./data/data.jsonfiles"

func main() {
	fmt.Println("Starting datatoapi")

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	db, err := store.OpenDatabase(DBLocation)
	defer db.Close()
	if err != nil {
		fmt.Println(err)
		return
	}

	fs := storage.NewLocalFS(FSLocation)
	// fs.BuildIndexFormat(db)
	var iFmt interface{}
	err = fs.CreateIndexes(db, iFmt)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	env := &api.Env{
		DB:    db,
		Store: fs,
	}

	// Start API based on our schema
	r := mux.NewRouter()
	// Routes consist of a path and a handler function.
	r.HandleFunc("/", env.GetOne)
	r.HandleFunc("/many", env.GetMany)

	// Bind to a port and pass our router in
	fmt.Println("Listening on port 8123")
	log.Fatal(http.ListenAndServe(":8123", r))

}
