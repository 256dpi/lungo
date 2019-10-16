package lungo

import (
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

	// ensure namespace
	if b.data.Namespaces[ns] == nil {
		b.data.Namespaces[ns] = NewNamespace(ns)
	}

	// TODO: Check indexes (unique id).

	// clone data
	temp := b.data.Clone()

	// add document
	temp.Namespaces[ns].Documents = append(b.data.Namespaces[ns].Documents, doc)

	// write data
	err := b.store.Store(temp)
	if err != nil {
		return err
	}

	// set new data
	b.data = temp

	return nil
}
