package lungo

import (
	"bytes"
	"io/ioutil"
	"os"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/dbkit"
)

type Store interface {
	Load() (*Dataset, error)
	Store(*Dataset) error
}

type MemoryStore struct {
	dataset *Dataset
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		dataset: NewDataset(),
	}
}

func (m MemoryStore) Load() (*Dataset, error) {
	return m.dataset, nil
}

func (m MemoryStore) Store(data *Dataset) error {
	m.dataset = data
	return nil
}

type SingleFileStore struct {
	path string
	mode os.FileMode
}

func NewSingleFileStore(path string, mode os.FileMode) *SingleFileStore {
	return &SingleFileStore{
		path: path,
		mode: mode,
	}
}

func (s *SingleFileStore) Load() (*Dataset, error) {
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

func (s *SingleFileStore) Store(data *Dataset) error {
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
