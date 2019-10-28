package lungo

import (
	"strings"

	"github.com/256dpi/lungo/bsonkit"
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
	Indexes map[string]*bsonkit.Index
}

// NewNamespace creates and returns a new namespace.
func NewNamespace() *Namespace {
	return &Namespace{
		Documents: bsonkit.NewSet(nil),
		Indexes: map[string]*bsonkit.Index{
			"_id_": bsonkit.NewIndex(true, []bsonkit.Column{
				{Path: "_id"},
			}),
		},
	}
}

// Clone will clone the namespace.
func (n *Namespace) Clone() *Namespace {
	// create new namespace
	clone := &Namespace{
		Documents: n.Documents.Clone(),
		Indexes:   map[string]*bsonkit.Index{},
	}

	// clone indexes
	for name, index := range n.Indexes {
		clone.Indexes[name] = index.Clone()
	}

	return clone
}
