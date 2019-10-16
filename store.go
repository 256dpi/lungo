package lungo

import (
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

type IndexData struct {
	Keys       []string
	Background bool
	Unique     bool
	Name       string
	Partial    bson.M
	Sparse     bool
	Expiry     time.Duration
}

type CollectionData struct {
	Name      string
	Documents []bson.M
	Indexes   []IndexData
}

type DatabaseData struct {
	Name        string
	Collections map[string]*CollectionData
}

type Data struct {
	Databases map[string]*DatabaseData
}

type Store interface {
	Load() (*Data, error)
	Store(*Data) error
}

type SingleFileStore struct {
}

func NewSingleFileStore() *SingleFileStore {
	return &SingleFileStore{}
}

func (s *SingleFileStore) Load() (*Data, error) {
	panic("implement me")
}

func (s *SingleFileStore) Store(*Data) error {
	panic("implement me")
}
