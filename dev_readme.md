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
