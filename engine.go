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
	matched  bsonkit.List
	replaced bsonkit.Doc
	updated  bsonkit.List
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

func (e *engine) find(ns string, query, sort bsonkit.Doc, limit int) (*result, error) {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check namespace
	if e.data.Namespaces[ns] == nil {
		return &result{}, nil
	}

	// get documents
	list := e.data.Namespaces[ns].Documents

	// sort documents
	var err error
	if sort != nil && len(*sort) > 0 {
		list, err = mongokit.Sort(list, sort)
		if err != nil {
			return nil, err
		}
	}

	// filter documents
	list, err = mongokit.Filter(list, query, limit)
	if err != nil {
		return nil, err
	}

	return &result{matched: list}, nil
}

func (e *engine) insert(ns string, list bsonkit.List) error {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check ids
	for _, doc := range list {
		if bsonkit.Get(doc, "_id") == bsonkit.Missing {
			return fmt.Errorf("document is missng the _id field")
		}
	}

	// clone data
	clone := e.data.Clone()

	// create or clone namespace
	var namespace *Namespace
	if clone.Namespaces[ns] == nil {
		namespace = NewNamespace(ns)
		clone.Namespaces[ns] = namespace
	} else {
		namespace = clone.Namespaces[ns].Clone()
		clone.Namespaces[ns] = namespace
	}

	// add documents
	for _, doc := range list {
		// add document to primary index
		if !namespace.primaryIndex.Set(doc) {
			return fmt.Errorf("document with same _id exists already")
		}

		// add document
		namespace.Documents = append(namespace.Documents, doc)

		// add to list index
		namespace.listIndex[doc] = len(namespace.Documents) - 1
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

func (e *engine) replace(ns string, query, sort, repl bsonkit.Doc) (*result, error) {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check namespace
	if e.data.Namespaces[ns] == nil {
		return &result{}, nil
	}

	// get documents
	list := e.data.Namespaces[ns].Documents

	// sort documents
	var err error
	if sort != nil && len(*sort) > 0 {
		list, err = mongokit.Sort(list, sort)
		if err != nil {
			return nil, err
		}
	}

	// filter documents
	list, err = mongokit.Filter(list, query, 1)
	if err != nil {
		return nil, err
	}

	// check list
	if len(list) == 0 {
		return &result{}, nil
	}

	// set missing id or check existing id
	replID := bsonkit.Get(repl, "_id")
	if replID == bsonkit.Missing {
		err = bsonkit.Set(repl, "_id", bsonkit.Get(list[0], "_id"), true)
		if err != nil {
			return nil, err
		}
	} else if replID != bsonkit.Get(list[0], "_id") {
		return nil, fmt.Errorf("document _id is immutable")
	}

	// clone data
	clone := e.data.Clone()

	// clone namespace
	namespace := clone.Namespaces[ns].Clone()
	clone.Namespaces[ns] = namespace

	// get document position
	position := namespace.listIndex[list[0]]

	// remove old document from list and primary index
	namespace.primaryIndex.Delete(list[0])
	delete(namespace.listIndex, list[0])

	// replace document
	namespace.Documents[position] = repl

	// add new document to list and primary index
	namespace.primaryIndex.Set(repl)
	namespace.listIndex[repl] = position

	// write data
	err = e.store.Store(clone)
	if err != nil {
		return nil, err
	}

	// set new data
	e.data = clone

	return &result{
		matched:  list,
		replaced: repl,
	}, nil
}

func (e *engine) update(ns string, query, sort, update bsonkit.Doc, limit int) (*result, error) {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check namespace
	if e.data.Namespaces[ns] == nil {
		return &result{}, nil
	}

	// get documents
	list := e.data.Namespaces[ns].Documents

	// sort documents
	var err error
	if sort != nil && len(*sort) > 0 {
		list, err = mongokit.Sort(list, sort)
		if err != nil {
			return nil, err
		}
	}

	// filter documents
	list, err = mongokit.Filter(list, query, limit)
	if err != nil {
		return nil, err
	}

	// check list
	if len(list) == 0 {
		return &result{}, nil
	}

	// clone documents
	newList := bsonkit.CloneList(list)

	// update documents
	err = mongokit.Update(newList, update, false)
	if err != nil {
		return nil, err
	}

	// clone data
	clone := e.data.Clone()

	// clone namespace
	namespace := clone.Namespaces[ns].Clone()
	clone.Namespaces[ns] = namespace

	// get document positions
	positions := make([]int, 0, len(list))
	for _, doc := range list {
		positions = append(positions, namespace.listIndex[doc])
	}

	// remove old docs from list and primary index
	for _, doc := range list {
		delete(namespace.listIndex, doc)
		namespace.primaryIndex.Delete(doc)
	}

	// add new docs to primary index
	for _, doc := range newList {
		if !namespace.primaryIndex.Set(doc) {
			return nil, fmt.Errorf("document with same _id exists already")
		}
	}

	// replace documents and update list index
	for i, doc := range newList {
		namespace.Documents[positions[i]] = doc
		namespace.listIndex[doc] = positions[i]
	}

	// write data
	err = e.store.Store(clone)
	if err != nil {
		return nil, err
	}

	// set new data
	e.data = clone

	return &result{
		matched: list,
		updated: newList,
	}, nil
}

func (e *engine) delete(ns string, query, sort bsonkit.Doc, limit int) (*result, error) {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check namespace
	if e.data.Namespaces[ns] == nil {
		return nil, nil
	}

	// get documents
	list := e.data.Namespaces[ns].Documents

	// sort documents
	var err error
	if sort != nil && len(*sort) > 0 {
		list, err = mongokit.Sort(list, sort)
		if err != nil {
			return nil, err
		}
	}

	// filter documents
	list, err = mongokit.Filter(list, query, limit)
	if err != nil {
		return nil, err
	}

	// build list index
	listIndex := map[bsonkit.Doc]bool{}
	for _, doc := range list {
		listIndex[doc] = true
	}

	// clone data
	clone := e.data.Clone()

	// clone namespace
	namespace := clone.Namespaces[ns].Clone()
	clone.Namespaces[ns] = namespace

	// copy documents to keep
	documents := make(bsonkit.List, 0, len(namespace.Documents)-len(list))
	for _, doc := range namespace.Documents {
		if !listIndex[doc] {
			documents = append(documents, doc)
		}
	}

	// set new documents
	namespace.Documents = documents

	// update list and primary index
	for _, doc := range list {
		delete(namespace.listIndex, doc)
		namespace.primaryIndex.Delete(doc)
	}

	// write data
	err = e.store.Store(clone)
	if err != nil {
		return nil, err
	}

	// set new data
	e.data = clone

	return &result{matched: list}, nil
}
