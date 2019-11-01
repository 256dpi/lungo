package lungo

import (
	"fmt"
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

// Validate will validate the handle.
func (h Handle) Validate(needCollection bool) error {
	// check database
	if h[0] == "" {
		return fmt.Errorf("missing database in handle")
	}

	// check collection
	if needCollection && h[1] == "" {
		return fmt.Errorf("missing collection in handle")
	}

	return nil
}

// Oplog is the handle for the local oplog namespace.
var Oplog = Handle{"local", "oplog"}

// Catalog is the top level object per database that contains all data.
type Catalog struct {
	Namespaces map[Handle]*mongokit.Collection
}

// NewCatalog creates and returns a new catalog.
func NewCatalog() *Catalog {
	return &Catalog{
		Namespaces: map[Handle]*mongokit.Collection{
			Oplog: mongokit.NewCollection(false),
		},
	}
}

// Clone will clone the catalog. Namespaces need to be cloned separately.
func (d *Catalog) Clone() *Catalog {
	// create clone
	clone := &Catalog{
		Namespaces: make(map[Handle]*mongokit.Collection, len(d.Namespaces)),
	}

	// copy namespaces
	for name, namespace := range d.Namespaces {
		clone.Namespaces[name] = namespace
	}

	return clone
}
