package lungo

import (
	"strings"

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
	Handle     Handle
	Collection *mongokit.Collection
}

// NewNamespace creates and returns a new namespace.
func NewNamespace(handle Handle, idIndex bool) *Namespace {
	return &Namespace{
		Handle:     handle,
		Collection: mongokit.NewCollection(idIndex),
	}
}

// Clone will clone the namespace.
func (n *Namespace) Clone() *Namespace {
	return &Namespace{
		Handle:     n.Handle,
		Collection: n.Collection.Clone(),
	}
}
