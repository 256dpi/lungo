package lungo

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/256dpi/lungo/bsonkit"
	"github.com/256dpi/lungo/mongokit"
)

// TODO: Combine ListDatabases(), ListCollections(), NumDocuments() into Info().

type Result struct {
	Matched  bsonkit.List
	Modified bsonkit.List
	Upserted bsonkit.Doc
}

type Engine struct {
	store Store
	data  *Data
	mutex sync.Mutex
}

func CreateEngine(store Store) (*Engine, error) {
	// create engine
	e := &Engine{
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

func (e *Engine) ListDatabases(query bsonkit.Doc) (bsonkit.List, error) {
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
			if len(ns.Documents.List) > 0 {
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

func (e *Engine) ListCollections(db string, query bsonkit.Doc) (bsonkit.List, error) {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// prepare list
	list := make(bsonkit.List, 0, len(e.data.Namespaces))

	// TODO: Add more collection infos.

	// add documents
	for ns := range e.data.Namespaces {
		if strings.HasPrefix(ns, db+".") {
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

func (e *Engine) NumDocuments(ns string) int {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check namespace
	namespace, ok := e.data.Namespaces[ns]
	if !ok {
		return 0
	}

	return len(namespace.Documents.List)
}

func (e *Engine) Find(ns string, query, sort bsonkit.Doc, skip, limit int) (*Result, error) {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check namespace
	if e.data.Namespaces[ns] == nil {
		return &Result{}, nil
	}

	// get documents
	list := e.data.Namespaces[ns].Documents.List

	// sort documents
	var err error
	if sort != nil && len(*sort) > 0 {
		list, err = mongokit.Sort(list, sort)
		if err != nil {
			return nil, err
		}
	}

	// apply skip
	if skip > len(list) {
		list = nil
	} else {
		list = list[skip:]
	}

	// filter documents
	list, err = mongokit.Filter(list, query, limit)
	if err != nil {
		return nil, err
	}

	return &Result{Matched: list}, nil
}

func (e *Engine) Insert(ns string, list bsonkit.List, ordered bool) (*Result, error) {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// clone list
	list = bsonkit.CloneList(list)

	// ensure ids
	for _, doc := range list {
		// ensure object id
		if bsonkit.Get(doc, "_id") == bsonkit.Missing {
			err := bsonkit.Put(doc, "_id", primitive.NewObjectID(), true)
			if err != nil {
				return nil, err
			}
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

	// prepare result
	var err error
	result := &Result{}

	// insert documents
	for _, doc := range list {
		// add document to primary index
		if !namespace.primaryIndex.Add(doc) {
			err = fmt.Errorf("document with same _id exists already")
			if ordered {
				break
			} else {
				continue
			}
		}

		// add document
		namespace.Documents.Add(doc)

		// add to list
		result.Modified = append(result.Modified, doc)
	}

	// check if documents have been inserted
	if len(result.Modified) > 0 {
		// write data
		err := e.store.Store(clone)
		if err != nil {
			return nil, err
		}

		// set new data
		e.data = clone
	}

	return result, err
}

func (e *Engine) Replace(ns string, query, sort, repl bsonkit.Doc, upsert bool) (*Result, error) {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// clone replacement
	repl = bsonkit.Clone(repl)

	// get documents
	var list bsonkit.List
	if e.data.Namespaces[ns] != nil {
		list = e.data.Namespaces[ns].Documents.List
	}

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
		// handle upsert
		if upsert {
			return e.upsert(ns, query, repl, nil)
		}

		return &Result{}, nil
	}

	// set missing id or check existing id
	replID := bsonkit.Get(repl, "_id")
	if replID == bsonkit.Missing {
		err = bsonkit.Put(repl, "_id", bsonkit.Get(list[0], "_id"), true)
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

	// update primary index
	namespace.primaryIndex.Remove(list[0])
	if !namespace.primaryIndex.Add(repl) {
		return nil, fmt.Errorf("document with same _id exists already")
	}

	// replace document
	namespace.Documents.Replace(list[0], repl)

	// write data
	err = e.store.Store(clone)
	if err != nil {
		return nil, err
	}

	// set new data
	e.data = clone

	return &Result{
		Matched:  list,
		Modified: bsonkit.List{repl},
	}, nil
}

func (e *Engine) Update(ns string, query, sort, update bsonkit.Doc, limit int, upsert bool) (*Result, error) {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// get documents
	var list bsonkit.List
	if e.data.Namespaces[ns] != nil {
		list = e.data.Namespaces[ns].Documents.List
	}

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
		// handle upsert
		if upsert {
			return e.upsert(ns, query, nil, update)
		}

		return &Result{}, nil
	}

	// clone documents
	newList := bsonkit.CloneList(list)

	// update documents
	err = mongokit.Update(newList, update, false)
	if err != nil {
		return nil, err
	}

	// check ids
	for i, doc := range newList {
		if bsonkit.Get(doc, "_id") != bsonkit.Get(list[i], "_id") {
			return nil, fmt.Errorf("document _id is immutable")
		}
	}

	// clone data
	clone := e.data.Clone()

	// clone namespace
	namespace := clone.Namespaces[ns].Clone()
	clone.Namespaces[ns] = namespace

	// remove old docs from primary index
	for _, doc := range list {
		namespace.primaryIndex.Remove(doc)
	}

	// add new docs to primary index
	for _, doc := range newList {
		if !namespace.primaryIndex.Add(doc) {
			return nil, fmt.Errorf("document with same _id exists already")
		}
	}

	// replace documents
	for i, doc := range newList {
		namespace.Documents.Replace(list[i], doc)
	}

	// write data
	err = e.store.Store(clone)
	if err != nil {
		return nil, err
	}

	// set new data
	e.data = clone

	return &Result{
		Matched:  list,
		Modified: newList,
	}, nil
}

func (e *Engine) upsert(ns string, query, repl, update bsonkit.Doc) (*Result, error) {
	// extract query
	doc, err := mongokit.Extract(query)
	if err != nil {
		return nil, err
	}

	// set replacement if present
	if repl != nil {
		// get ids
		queryID := bsonkit.Get(doc, "_id")
		replID := bsonkit.Get(repl, "_id")

		// check ids
		if queryID != bsonkit.Missing && replID != bsonkit.Missing {
			if bsonkit.Compare(replID, queryID) != 0 {
				return nil, fmt.Errorf("query _id and replacement _id must match")
			}
		}

		// clone replacement
		doc = bsonkit.Clone(repl)

		// add repl or query id if present
		if replID != bsonkit.Missing {
			err = bsonkit.Put(doc, "_id", replID, true)
			if err != nil {
				return nil, err
			}
		} else if queryID != bsonkit.Missing {
			err = bsonkit.Put(doc, "_id", queryID, true)
			if err != nil {
				return nil, err
			}
		}
	}

	// apply update if present
	if update != nil {
		err = mongokit.Apply(doc, update, true)
		if err != nil {
			return nil, err
		}
	}

	// generate object id if missing
	if bsonkit.Get(doc, "_id") == bsonkit.Missing {
		err := bsonkit.Put(doc, "_id", primitive.NewObjectID(), true)
		if err != nil {
			return nil, err
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

	// add document to primary index
	if !namespace.primaryIndex.Add(doc) {
		return nil, fmt.Errorf("document with same _id exists already")
	}

	// add document
	namespace.Documents.Add(doc)

	// write data
	err = e.store.Store(clone)
	if err != nil {
		return nil, err
	}

	// set new data
	e.data = clone

	return &Result{
		Upserted: doc,
	}, nil
}

func (e *Engine) Delete(ns string, query, sort bsonkit.Doc, limit int) (*Result, error) {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check namespace
	if e.data.Namespaces[ns] == nil {
		return nil, nil
	}

	// get documents
	list := e.data.Namespaces[ns].Documents.List

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

	// remove documents
	for _, doc := range list {
		namespace.Documents.Remove(doc)
	}

	// update primary index
	for _, doc := range list {
		namespace.primaryIndex.Remove(doc)
	}

	// write data
	err = e.store.Store(clone)
	if err != nil {
		return nil, err
	}

	// set new data
	e.data = clone

	return &Result{Matched: list}, nil
}

func (e *Engine) Drop(ns string) error {
	// acquire mutex
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// quote all meta characters
	pattern := regexp.QuoteMeta(ns)

	// replace wildcards
	pattern = strings.ReplaceAll(pattern, `\*`, ".*")

	// compile regexp
	regex := regexp.MustCompile(fmt.Sprintf("^%s$", pattern))

	// drop all matching namespaces
	for name := range e.data.Namespaces {
		if regex.MatchString(name) {
			delete(e.data.Namespaces, name)
		}
	}

	return nil
}
