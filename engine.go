package lungo

import (
	"fmt"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/mongokit"
)

type engine struct {
	store Store
	data  *Data
	mutex sync.Mutex
}

func createEngine(store Store) (*engine, error) {
	// create engine
	e := &engine{
		store: store,
	}

	// load data
	data, err := e.store.Load()
	if err != nil {
		return nil, err
	}

	// set data
	e.data = data

	return e, nil
}

func (e *engine) listDatabases(query bson.D) ([]bson.D, error) {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// sort namespaces
	sort := map[string][]*Namespace{}
	for _, ns := range e.data.Namespaces {
		name := strings.Split(ns.Name, ".")[0]
		sort[name] = append(sort[name], ns)
	}

	// prepare list
	var list []bson.D
	for name, nss := range sort {
		// check emptiness
		empty := true
		for _, ns := range nss {
			if len(ns.Documents) > 0 {
				empty = false
			}
		}

		// add specification
		list = append(list, bson.D{
			bson.E{Key: "name", Value: name},
			bson.E{Key: "sizeOnDisk", Value: 42},
			bson.E{Key: "empty", Value: empty},
		})
	}

	// filter list
	list, err := mongokit.Filter(list, query)
	if err != nil {
		return nil, err
	}

	return list, nil
}

func (e *engine) dropDatabase(name string) error {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// drop all namespaces
	for ns := range e.data.Namespaces {
		if strings.Split(ns, ".")[0] == name {
			delete(e.data.Namespaces, ns)
		}
	}

	return nil
}

func (e *engine) listCollections(db string, query bson.D) ([]bson.D, error) {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// prepare list
	list := make([]bson.D, 0)

	// TODO: Add more collection infos.

	// add documents
	for ns := range e.data.Namespaces {
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

func (e *engine) dropCollection(ns string) error {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// drop all namespaces
	for name := range e.data.Namespaces {
		if name == ns {
			delete(e.data.Namespaces, name)
		}
	}

	return nil
}

func (e *engine) find(ns string, query bson.D) ([]bson.D, error) {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check namespace
	if e.data.Namespaces[ns] == nil {
		return nil, nil
	}

	// filter documents
	list, err := mongokit.Filter(e.data.Namespaces[ns].Documents, query)
	if err != nil {
		return nil, err
	}

	return list, nil
}

func (e *engine) insert(ns string, docs []bson.D) error {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check if namespace exists
	if e.data.Namespaces[ns] != nil {
		// check primary index
		for _, doc := range docs {
			if e.data.Namespaces[ns].primaryIndex.Has(&primaryIndexItem{doc: doc}) {
				return fmt.Errorf("document with same _id exists already")
			}
		}

		// TODO: Check secondary indexes.
	}

	// clone data
	temp := e.data.Clone()

	// create or clone namespace
	if temp.Namespaces[ns] == nil {
		temp.Namespaces[ns] = NewNamespace(ns)
	} else {
		temp.Namespaces[ns] = temp.Namespaces[ns].Clone()
	}

	// add documents
	for _, doc := range docs {
		// add document
		temp.Namespaces[ns].Documents = append(temp.Namespaces[ns].Documents, doc)

		// update primary index
		temp.Namespaces[ns].primaryIndex.ReplaceOrInsert(&primaryIndexItem{doc: doc})
	}

	// write data
	err := e.store.Store(temp)
	if err != nil {
		return err
	}

	// set new data
	e.data = temp

	return nil
}
