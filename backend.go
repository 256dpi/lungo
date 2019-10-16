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

func (b *Backend) find(db, coll string, qry bson.M) (ICursor, error) {
	// acquire mutex
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// check database
	if b.data.Databases[db] == nil {
		// TODO: What does mongo do?
		return &staticCursor{}, nil
	}

	// check collection
	if b.data.Databases[db].Collections[coll] == nil {
		// TODO: What does mongo do?
		return &staticCursor{}, nil
	}

	// TODO: Apply query.

	return &staticCursor{
		list: b.data.Databases[db].Collections[coll].Documents,
		pos:  0,
	}, nil
}

func (b *Backend) insertOne(db, coll string, doc bson.M) error {
	// acquire mutex
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// ensure database
	if b.data.Databases[db] == nil {
		b.data.Databases[db] = NewDatabaseData(db)
	}

	// ensure collection
	if b.data.Databases[db].Collections[coll] == nil {
		b.data.Databases[db].Collections[coll] = NewCollectionData(coll)
	}

	// TODO: Check indexes (unique id).

	// clone data
	temp := b.data.Clone()

	// add document
	temp.Databases[db].Collections[coll].Documents = append(b.data.Databases[db].Collections[coll].Documents, doc)

	// write data
	err := b.store.Store(temp)
	if err != nil {
		return err
	}

	// set new data
	b.data = temp

	return nil
}
