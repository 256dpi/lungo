package lungo

import (
	"strings"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
	"github.com/256dpi/lungo/mongokit"
)

// Handle is a two component identifier for namespaces where the first part is
// the database and the second the collection.
type Handle [2]string

// String will return the string form of the handle.
func (h Handle) String() string {
	return strings.Join(h[:], ".")
}

// Dataset is the top level object per database that contains all data.
type Dataset struct {
	Namespaces map[Handle]*Namespace
}

// NewDataset creates and returns a new dataset.
func NewDataset() *Dataset {
	return &Dataset{
		Namespaces: make(map[Handle]*Namespace),
	}
}

// Clone will cone the dataset. Namespaces need to be cloned separately.
func (d *Dataset) Clone() *Dataset {
	// create clone
	clone := NewDataset()

	// copy namespaces
	for name, namespace := range d.Namespaces {
		clone.Namespaces[name] = namespace
	}

	return clone
}

// Namespace holds documents and indexes.
type Namespace struct {
	// The document set.
	Documents *bsonkit.Set

	// The indexes.
	Indexes map[string]*mongokit.Index
}

// NewNamespace creates and returns a new namespace.
func NewNamespace() *Namespace {
	// create default index
	index, err := mongokit.CreateIndex(bsonkit.Convert(bson.M{"_id": int32(1)}), true)
	if err != nil {
		panic(err) // should not happen
	}

	return &Namespace{
		Documents: bsonkit.NewSet(nil),
		Indexes: map[string]*mongokit.Index{
			"_id_": index,
		},
	}
}

// Clone will clone the namespace.
func (n *Namespace) Clone() *Namespace {
	// create new namespace
	clone := &Namespace{
		Documents: n.Documents.Clone(),
		Indexes:   map[string]*mongokit.Index{},
	}

	// clone indexes
	for name, index := range n.Indexes {
		clone.Indexes[name] = index.Clone()
	}

	return clone
}
