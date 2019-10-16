package lungo

import (
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

type Data struct {
	Databases map[string]*DatabaseData
}

func NewData() *Data {
	return &Data{
		Databases: make(map[string]*DatabaseData),
	}
}

type DatabaseData struct {
	Name        string
	Collections map[string]*CollectionData
}

func NewDatabaseData(name string) *DatabaseData {
	return &DatabaseData{
		Name:        name,
		Collections: make(map[string]*CollectionData),
	}
}

type CollectionData struct {
	Name      string
	Documents []bson.M
	Indexes   []IndexData
}

func NewCollectionData(name string) *CollectionData {
	return &CollectionData{
		Name: name,
	}
}

type IndexData struct {
	Keys       []string
	Background bool
	Unique     bool
	Name       string
	Partial    bson.M
	Sparse     bool
	Expiry     time.Duration
}
