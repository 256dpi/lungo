package lungo

import (
	"fmt"
	"strings"
	"time"

	"github.com/256dpi/lungo/bsonkit"
	"github.com/256dpi/lungo/mongokit"
)

// File is a format for storing catalogs in a single structure.
type File struct {
	Namespaces map[string]FileNamespace `bson:"namespaces"`
}

// FileNamespace is a single namespace stored in a file.
type FileNamespace struct {
	Documents bsonkit.List         `bson:"documents"`
	Indexes   map[string]FileIndex `bson:"indexes"`
}

// FileIndex is a single index stored in a file.
type FileIndex struct {
	Key     bsonkit.Doc   `bson:"key"`
	Unique  bool          `bson:"unique"`
	Partial bsonkit.Doc   `bson:"partial"`
	Expiry  time.Duration `bson:"expiry"`
}

// BuildFile will build a new file from the provided catalog.
func BuildFile(catalog *Catalog) *File {
	// prepare file
	file := &File{
		Namespaces: map[string]FileNamespace{},
	}

	// add namespaces
	for handle, namespace := range catalog.Namespaces {
		// collect indexes
		indexes := map[string]FileIndex{}
		for name, index := range namespace.Indexes {
			// get config
			config := index.Config()

			// add index
			indexes[name] = FileIndex{
				Key:     config.Key,
				Unique:  config.Unique,
				Partial: config.Partial,
				Expiry:  config.Expiry,
			}
		}

		// add namespace
		file.Namespaces[handle.String()] = FileNamespace{
			Documents: namespace.Documents.List,
			Indexes:   indexes,
		}
	}

	return file
}

// BuildCatalog will build a new catalog from the file.
func (f *File) BuildCatalog() (*Catalog, error) {
	// create catalog
	catalog := NewCatalog()

	// process namespaces
	for name, ns := range f.Namespaces {
		// split name
		segments := strings.SplitN(name, ".", 2)
		if len(segments) != 2 {
			return nil, fmt.Errorf("invalid namespace name %q", name)
		}

		// prepare handle
		handle := Handle{segments[0], segments[1]}

		// create namespace
		namespace := mongokit.NewCollection(false)

		// add documents
		namespace.Documents = bsonkit.NewSet(ns.Documents)

		// add indexes
		for name, idx := range ns.Indexes {
			// create index
			index, err := mongokit.CreateIndex(mongokit.IndexConfig{
				Key:     idx.Key,
				Unique:  idx.Unique,
				Partial: idx.Partial,
				Expiry:  idx.Expiry,
			})
			if err != nil {
				return nil, err
			}

			// build index
			ok, err := index.Build(ns.Documents)
			if err != nil {
				return nil, err
			} else if !ok {
				return nil, fmt.Errorf("duplicate document for index %q", name)
			}

			// add index
			namespace.Indexes[name] = index
		}

		// add namespace
		catalog.Namespaces[handle] = namespace
	}

	return catalog, nil
}
