package lungo

import (
	"go.mongodb.org/mongo-driver/bson"
)

type Data struct {
	Namespaces map[string]*Namespace
}

func NewData() *Data {
	return &Data{
		Namespaces: make(map[string]*Namespace),
	}
}

func (d *Data) Clone() *Data {
	// create new data
	data := NewData()

	// clone namespaces
	for name, namespace := range d.Namespaces {
		data.Namespaces[name] = namespace.Clone()
	}

	return data
}

type Namespace struct {
	Name      string
	Documents []bson.M
	Indexes   []Index
}

func NewNamespace(name string) *Namespace {
	return &Namespace{
		Name: name,
	}
}

func (d *Namespace) Clone() *Namespace {
	// create new data
	data := NewNamespace(d.Name)

	// clone documents
	copy(data.Documents, d.Documents)

	// clone indexes
	copy(data.Indexes, d.Indexes)

	return data
}

type Index struct {
	Name   string
	Keys   bson.D
	Unique bool
}
