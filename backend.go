package lungo

import (
	"fmt"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/mongokit"
)

type Backend struct {
	store Store
	data  *Data
	mutex sync.Mutex
}

func newBackend(store Store) *Backend {
	return &Backend{
		store: store,
	}
}

func (b *Backend) setup() error {
	// load data
	data, err := b.store.Load()
	if err != nil {
		return err
	}

	// set data
	b.data = data

	return nil
}

func (b *Backend) listCollections(db string, query bson.D) ([]bson.D, error) {
	// acquire mutex
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// prepare list
	list := make([]bson.D, 0)

	// TODO: Add more collection infos.

	// add documents
	for ns := range b.data.Namespaces {
		if strings.HasPrefix(ns, db) {
			list = append(list, bson.D{
				bson.E{Key: "name", Value: strings.TrimPrefix(ns, db)[1:]},
				bson.E{Key: "type", Value: "collection"},
				bson.E{Key: "options", Value: bson.D{}},
				bson.E{Key: "info", Value: bson.D{
					bson.E{Key: "readOnly", Value: false},
				}},
			})
		}
	}

	// filter list
	list, err := mongokit.Filter(list, query)
	if err != nil {
		return nil, err
	}

	return list, nil
}

func (b *Backend) find(ns string, query bson.D) ([]bson.D, error) {
	// acquire mutex
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// check namespace
	if b.data.Namespaces[ns] == nil {
		return nil, nil
	}

	// filter documents
	list, err := mongokit.Filter(b.data.Namespaces[ns].Documents, query)
	if err != nil {
		return nil, err
	}

	return list, nil
}

func (b *Backend) insertOne(ns string, doc bson.D) error {
	// acquire mutex
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// check if namespace exists
	if b.data.Namespaces[ns] != nil {
		// check primary index
		if b.data.Namespaces[ns].primaryIndex.Has(&primaryIndexItem{doc: doc}) {
			return fmt.Errorf("document with same _id exists already")
		}

		// TODO: Check secondary indexes.
	}

	// clone data
	temp := b.data.Clone()

	// create or clone namespace
	if temp.Namespaces[ns] == nil {
		temp.Namespaces[ns] = NewNamespace(ns)
	} else {
		temp.Namespaces[ns] = temp.Namespaces[ns].Clone()
	}

	// add document
	temp.Namespaces[ns].Documents = append(temp.Namespaces[ns].Documents, doc)

	// update primary index
	temp.Namespaces[ns].primaryIndex.ReplaceOrInsert(&primaryIndexItem{doc: doc})

	// write data
	err := b.store.Store(temp)
	if err != nil {
		return err
	}

	// set new data
	b.data = temp

	return nil
}
