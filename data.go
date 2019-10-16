package lungo

import (
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

func (d *Data) Clone() *Data {
	// create new data
	data := NewData()

	// clone databases
	for name, db := range d.Databases {
		data.Databases[name] = db.Clone()
	}

	return data
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

func (d *DatabaseData) Clone() *DatabaseData {
	// create new data
	data := NewDatabaseData(d.Name)

	// clone collections
	for name, coll := range d.Collections {
		data.Collections[name] = coll.Clone()
	}

	return data
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

func (d *CollectionData) Clone() *CollectionData {
	// create new data
	data := NewCollectionData(d.Name)

	// clone documents
	copy(data.Documents, d.Documents)

	// clone indexes
	copy(data.Indexes, d.Indexes)

	return data
}

type IndexData struct {
	Name       string
	Keys       bson.D
	Unique     bool
}
