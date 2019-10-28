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
	Load() (*Dataset, error)
	Store(*Dataset) error
}

// MemoryStore holds the dataset in memory.
type MemoryStore struct {
	dataset *Dataset
}

// NewMemoryStore creates and returns a new memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		dataset: NewDataset(),
	}
}

// Load will return the dataset.
func (m MemoryStore) Load() (*Dataset, error) {
	return m.dataset, nil
}

// Store will store the dataset.
func (m MemoryStore) Store(data *Dataset) error {
	m.dataset = data
	return nil
}

// FileStore writes the dataset to a single file on disk.
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

// Load will read the dataset from disk and return it.
func (s *FileStore) Load() (*Dataset, error) {
	// load file
	buf, err := ioutil.ReadFile(s.path)
	if os.IsNotExist(err) {
		return NewDataset(), nil
	} else if err != nil {
		return nil, err
	}

	// decode dataset
	var dataset Dataset
	err = bson.Unmarshal(buf, &dataset)
	if err != nil {
		return nil, err
	}

	// prepare
	dataset.Prepare()

	return &dataset, nil
}

// Store will atomically write the dataset to disk.
func (s *FileStore) Store(data *Dataset) error {
	// encode dataset
	buf, err := bson.Marshal(data)
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
