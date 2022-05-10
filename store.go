package lungo

import (
	"bytes"
	"io/ioutil"
	"os"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/dbkit"
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
func (m *MemoryStore) Load() (*Catalog, error) {
	return m.catalog, nil
}

// Store will store the catalog.
func (m *MemoryStore) Store(data *Catalog) error {
	m.catalog = data
	return nil
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

// Load will read the catalog from disk and return it. If no file exists at the
// specified location an empty catalog is returned.
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

	// build catalog from file
	catalog, err := file.BuildCatalog()
	if err != nil {
		return nil, err
	}

	return catalog, nil
}

// Store will atomically write the catalog to disk.
func (s *FileStore) Store(catalog *Catalog) error {
	// build file from catalog
	file := BuildFile(catalog)

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
