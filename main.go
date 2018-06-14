package main

import (
	"flag"
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/zachgoldstein/datatoapi/engine"
)

const TitleASCII = `

██████╗  █████╗ ████████╗ █████╗ ██████╗ ██╗
██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗██╔══██╗██║
██║  ██║███████║   ██║   ███████║██████╔╝██║
██║  ██║██╔══██║   ██║   ██╔══██║██╔═══╝ ██║
██████╔╝██║  ██║   ██║   ██║  ██║██║     ██║
╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚═╝     ╚═╝

`

func main() {

	var port = flag.Int("port", 8123, "What port will Datapi run on?")
	var indexPath = flag.String("index", "./datatoapi.index", "Where will indexes be stored?")
	var storagePath = flag.String("storage", "https://s3.amazonaws.com/datatoapi/data.jsonfiles", "Where is the data you'd like to expose stored?")
	var logType = flag.String("logType", "normal", "What type of logs should datapi output? Options are normal, json")
	// "storagePath": "./data/dataDir/",

	flag.Parse()
	if *logType == "json" {
		log.SetFormatter(&log.JSONFormatter{})
	}

	config := engine.Config{
		IndexPath:   *indexPath,
		StoragePath: *storagePath,
		Port:        *port,
	}
	fmt.Println(TitleASCII)
	log.WithFields(log.Fields{
		"indexPath":   config.IndexPath,
		"storagePath": config.StoragePath,
		"port":        config.Port,
	}).Info("Starting Datatoapi")

	datatoapiEngine := engine.NewEngine()
	datatoapiEngine.Start(config)
}
