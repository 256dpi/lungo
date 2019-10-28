package lungo

import (
	"bytes"
	"io/ioutil"
	"os"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/dbkit"
)

type Store interface {
	Load() (*Data, error)
	Store(*Data) error
}

type MemoryStore struct {
	data *Data
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data: NewData(),
	}
}

func (m MemoryStore) Load() (*Data, error) {
	return m.data, nil
}

func (m MemoryStore) Store(data *Data) error {
	m.data = data
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

func (s *SingleFileStore) Load() (*Data, error) {
	// load file
	buf, err := ioutil.ReadFile(s.path)
	if os.IsNotExist(err) {
		return NewData(), nil
	} else if err != nil {
		return nil, err
	}

	// decode data
	var data Data
	err = bson.Unmarshal(buf, &data)
	if err != nil {
		return nil, err
	}

	// prepare
	data.Prepare()

	return &data, nil
}

func (s *SingleFileStore) Store(data *Data) error {
	// encode data
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
