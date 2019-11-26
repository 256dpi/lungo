package lungo

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
	"github.com/256dpi/lungo/dbkit"
	"github.com/256dpi/lungo/mongokit"
)

// Store is the interface that describes storage adapters.
type Store interface {
	Load() (*Catalog, error)
	Store(*Catalog) error
}

// MemoryStore holds the catalog in memory.
type MemoryStore struct {
	catalog *Catalog
}

// NewMemoryStore creates and returns a new memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		catalog: NewCatalog(),
	}
}

// Load will return the catalog.
func (m MemoryStore) Load() (*Catalog, error) {
	return m.catalog, nil
}

// Store will store the catalog.
func (m MemoryStore) Store(data *Catalog) error {
	m.catalog = data
	return nil
}

// File is the format of the file stored by the file store.
type File struct {
	Namespaces map[string]FileNamespace `bson:"namespaces"`
}

// FileNamespace is a single namespace stored in a file by the file store.
type FileNamespace struct {
	Documents bsonkit.List         `bson:"documents"`
	Indexes   map[string]FileIndex `bson:"indexes"`
}

// FileIndex is a single index stored in a file by the file store.
type FileIndex struct {
	Key     bsonkit.Doc   `bson:"key"`
	Unique  bool          `bson:"unique"`
	Partial bsonkit.Doc   `bson:"partial"`
	Expiry  time.Duration `bson:"expiry"`
}

// FileStore writes the catalog to a single file on disk.
type FileStore struct {
	path string
	mode os.FileMode
}

// NewFileStore creates and returns a new file store.
func NewFileStore(path string, mode os.FileMode) *FileStore {
	return &FileStore{
		path: path,
		mode: mode,
	}
}

// Load will read the catalog from disk and return it.
func (s *FileStore) Load() (*Catalog, error) {
	// load file
	buf, err := ioutil.ReadFile(s.path)
	if os.IsNotExist(err) {
		return NewCatalog(), nil
	} else if err != nil {
		return nil, err
	}

	// decode file
	var file File
	err = bson.Unmarshal(buf, &file)
	if err != nil {
		return nil, err
	}

	// create catalog
	catalog := NewCatalog()

	// process namespaces
	for name, ns := range file.Namespaces {
		// create handle
		segments := strings.Split(name, ".")
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

// Store will atomically write the catalog to disk.
func (s *FileStore) Store(data *Catalog) error {
	// create file
	file := File{
		Namespaces: map[string]FileNamespace{},
	}

	// add namespaces
	for handle, namespace := range data.Namespaces {
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

	// encode file
	buf, err := bson.Marshal(file)
	if err != nil {
		return err
	}

	// write file
	err = dbkit.AtomicWriteFile(s.path, bytes.NewReader(buf), s.mode)
	if err != nil {
		return err
	}

	return nil
}
