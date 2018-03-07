# Useful Development Notes

Generating local dummy data:

```
python ./scripts/createDummyData.py
```

Print all values for a key:
```
cat ./data.jsonfiles | jq '.name' -c
```

Print all items that contain the "job" field
```
cat ./data.jsonfiles | jq 'select(.job != null)' -c | wc -l
```

Count number of items that contain the "job" field
```
cat ./data.jsonfiles | jq 'select(.job != null)' -c | wc -l
```

To retrieve data from the API:
```
curl 'http://localhost:8123/?query=ID&value=1000100'
```

Profiling notes:

☁  datatoapi [master] ⚡ go-torch -u http://localhost:6060                    
INFO[18:22:15] Run pprof command: go tool pprof -raw -seconds 30 http://localhost:6060/debug/pprof/profile
FATAL[18:22:45] Failed: could not generate flame graph: Cannot find flamegraph scripts in the PATH or current directory. You can download the script at https://github.com/brendangregg/FlameGraph. These scripts should be added to your PATH or in the directory where go-torch is executed. Alternatively, you can run go-torch with the --raw flag.
☁  datatoapi [master] ⚡ cd $GOPATH/src/github.com/uber/go-torch
☁  go-torch [master] git clone https://github.com/brendangregg/FlameGraph.git


Creating a malformed response
- Generate data
- Start server
- Generate data again
- Replace old data with new data
- Issue request to server

storing as a date type into storm/bolt does not return values:
`Couldn't retrieve index from bolt: not found`
Store them as strings instead

Same for float32, store as float64
```
Looking for Distance with 0.873794
INTERNAL Looking for 0.8737940192222595 with Distance
```

Same for bool, store as string
-> This is problematic. We have to build a check for this into anything storing/retrieving from the db
