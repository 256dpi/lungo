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

// Oplog is the handle for the local oplog namespace.
var Oplog = Handle{"local", "oplog"}

// Dataset is the top level object per database that contains all data.
type Dataset struct {
	Namespaces map[Handle]*Namespace
}

// NewDataset creates and returns a new dataset.
func NewDataset() *Dataset {
	return &Dataset{
		Namespaces: map[Handle]*Namespace{
			Oplog: NewNamespace(Oplog, false),
		},
	}
}

// Clone will clone the dataset. Namespaces need to be cloned separately.
func (d *Dataset) Clone() *Dataset {
	// create clone
	clone := &Dataset{
		Namespaces: make(map[Handle]*Namespace, len(d.Namespaces)),
	}

	// copy namespaces
	for name, namespace := range d.Namespaces {
		clone.Namespaces[name] = namespace
	}

	return clone
}

// Namespace holds documents and indexes.
type Namespace struct {
	Handle    Handle
	Documents *bsonkit.Set
	Indexes   map[string]*mongokit.Index
}

// NewNamespace creates and returns a new namespace.
func NewNamespace(handle Handle, idIndex bool) *Namespace {
	// create namespace
	ns := &Namespace{
		Handle:    handle,
		Documents: bsonkit.NewSet(nil),
		Indexes:   map[string]*mongokit.Index{},
	}

	// add default index if requested
	if idIndex {
		ns.Indexes["_id_"], _ = mongokit.CreateIndex(mongokit.IndexConfig{
			Key: bsonkit.Convert(bson.M{
				"_id": int32(1),
			}),
			Unique: true,
		})
	}

	return ns
}

// Clone will clone the namespace.
func (n *Namespace) Clone() *Namespace {
	// create new namespace
	clone := &Namespace{
		Handle:    n.Handle,
		Documents: n.Documents.Clone(),
		Indexes:   map[string]*mongokit.Index{},
	}

	// clone indexes
	for name, index := range n.Indexes {
		clone.Indexes[name] = index.Clone()
	}

	return clone
}
