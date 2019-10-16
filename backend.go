package lungo

import (
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
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

func (b *Backend) find(ns string, qry bson.M) (ICursor, error) {
	// acquire mutex
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// check namespace
	if b.data.Namespaces[ns] == nil {
		return &staticCursor{}, nil
	}

	// TODO: Apply query.

	return &staticCursor{
		list: b.data.Namespaces[ns].Documents,
		pos:  0,
	}, nil
}

func (b *Backend) insertOne(ns string, doc bson.M) error {
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
