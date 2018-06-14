package models

type DataBlock struct {
	UID    string
	RefKey string
	Start  int64
	End    int64
	File   File
}

type File struct {
	Address string
	Type    string
}

type IndexData struct {
	UID    string
	Data   map[string]interface{}
	RefKey string
}
