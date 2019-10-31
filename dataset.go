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
	Namespaces map[Handle]*mongokit.Collection
}

// NewDataset creates and returns a new dataset.
func NewDataset() *Dataset {
	return &Dataset{
		Namespaces: map[Handle]*mongokit.Collection{
			Oplog: mongokit.NewCollection(false),
		},
	}
}

// Clone will clone the dataset. Namespaces need to be cloned separately.
func (d *Dataset) Clone() *Dataset {
	// create clone
	clone := &Dataset{
		Namespaces: make(map[Handle]*mongokit.Collection, len(d.Namespaces)),
	}

	// copy namespaces
	for name, namespace := range d.Namespaces {
		clone.Namespaces[name] = namespace
	}

	return clone
}
