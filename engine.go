package lungo

import (
	"fmt"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
	"github.com/256dpi/lungo/mongokit"
)

type result struct {
	matched  int
	modified int
}

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

func (e *engine) listDatabases(query bsonkit.Doc) (bsonkit.List, error) {
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
	var list bsonkit.List
	for name, nss := range sort {
		// check emptiness
		empty := true
		for _, ns := range nss {
			if len(ns.Documents) > 0 {
				empty = false
			}
		}

		// add specification
		list = append(list, &bson.D{
			bson.E{Key: "name", Value: name},
			bson.E{Key: "sizeOnDisk", Value: 42},
			bson.E{Key: "empty", Value: empty},
		})
	}

	// filter list
	list, err := mongokit.Filter(list, query, 0)
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

func (e *engine) listCollections(db string, query bsonkit.Doc) (bsonkit.List, error) {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// prepare list
	list := make(bsonkit.List, 0, len(e.data.Namespaces))

	// TODO: Add more collection infos.

	// add documents
	for ns := range e.data.Namespaces {
		if strings.HasPrefix(ns, db) {
			list = append(list, &bson.D{
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
	list, err := mongokit.Filter(list, query, 0)
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

func (e *engine) numDocuments(ns string) int {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check namespace
	namespace, ok := e.data.Namespaces[ns]
	if !ok {
		return 0
	}

	return len(namespace.Documents)
}

func (e *engine) delete(ns string, query bsonkit.Doc, limit int) (int, error) {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check namespace
	if e.data.Namespaces[ns] == nil {
		return 0, nil
	}

	// filter documents
	list, err := mongokit.Filter(e.data.Namespaces[ns].Documents, query, limit)
	if err != nil {
		return 0, err
	}

	// clone data and namespace
	clone := e.data.Clone()
	clone.Namespaces[ns] = clone.Namespaces[ns].Clone()

	// remove documents
	clone.Namespaces[ns].Documents = bsonkit.Difference(clone.Namespaces[ns].Documents, list)

	// update primary index
	for _, doc := range list {
		clone.Namespaces[ns].primaryIndex.Delete(doc)
	}

	// write data
	err = e.store.Store(clone)
	if err != nil {
		return 0, err
	}

	// set new data
	e.data = clone

	return len(list), nil
}

func (e *engine) find(ns string, query bsonkit.Doc, limit int) (bsonkit.List, error) {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check namespace
	if e.data.Namespaces[ns] == nil {
		return nil, nil
	}

	// filter documents
	list, err := mongokit.Filter(e.data.Namespaces[ns].Documents, query, limit)
	if err != nil {
		return nil, err
	}

	return list, nil
}

func (e *engine) insert(ns string, docs bsonkit.List) error {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// clone data
	clone := e.data.Clone()

	// create or clone namespace
	if clone.Namespaces[ns] == nil {
		clone.Namespaces[ns] = NewNamespace(ns)
	} else {
		clone.Namespaces[ns] = clone.Namespaces[ns].Clone()
	}

	// add documents
	for _, doc := range docs {
		// check primary index
		if clone.Namespaces[ns].primaryIndex.Has(doc) {
			return fmt.Errorf("document with same _id exists already")
		}

		// add document
		clone.Namespaces[ns].Documents = append(clone.Namespaces[ns].Documents, doc)

		// update primary index
		clone.Namespaces[ns].primaryIndex.Set(doc)
	}

	// write data
	err := e.store.Store(clone)
	if err != nil {
		return err
	}

	// set new data
	e.data = clone

	return nil
}

func (e *engine) replace(ns string, query, repl bsonkit.Doc) (*result, error) {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check namespace
	if e.data.Namespaces[ns] == nil {
		return &result{}, nil
	}

	// filter documents
	list, err := mongokit.Filter(e.data.Namespaces[ns].Documents, query, 1)
	if err != nil {
		return nil, err
	}

	// check list
	if len(list) == 0 {
		return &result{}, nil
	}

	// clone data and namespace
	clone := e.data.Clone()
	clone.Namespaces[ns] = clone.Namespaces[ns].Clone()

	// replace document
	var oldDoc bsonkit.Doc
	for i, doc := range clone.Namespaces[ns].Documents {
		if doc == list[0] {
			oldDoc = doc
			clone.Namespaces[ns].Documents[i] = repl
		}
	}

	// remove old doc from index
	clone.Namespaces[ns].primaryIndex.Delete(oldDoc)

	// check index
	if clone.Namespaces[ns].primaryIndex.Has(repl) {
		return nil, fmt.Errorf("document with same _id exists already")
	}

	// update primary index
	clone.Namespaces[ns].primaryIndex.Set(repl)

	// write data
	err = e.store.Store(clone)
	if err != nil {
		return nil, err
	}

	// set new data
	e.data = clone

	return &result{
		matched:  len(list),
		modified: 1,
	}, nil
}
