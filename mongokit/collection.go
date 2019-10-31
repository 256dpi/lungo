package mongokit

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/256dpi/lungo/bsonkit"
)

// Result is returned by collection operations.
type Result struct {
	// The list of found or deleted documents.
	Matched bsonkit.List

	// The list of inserted, replaced or updated documents.
	Modified bsonkit.List

	// The upserted document.
	Upserted bsonkit.Doc

	// The changes applied to updated documents.
	Changes []*Changes
}

// Collection combines and set and multiple indexes to form a basic MongoDB like
// collection that offers basic CRUD capabilities.
type Collection struct {
	Documents *bsonkit.Set
	Indexes   map[string]*Index
}

// NewCollection will create and return a new collection.
func NewCollection(idIndex bool) *Collection {
	// create collection
	coll := &Collection{
		Documents: bsonkit.NewSet(nil),
		Indexes:   map[string]*Index{},
	}

	// add default index if requested
	if idIndex {
		coll.Indexes["_id_"], _ = CreateIndex(IndexConfig{
			Key: bsonkit.Convert(bson.M{
				"_id": int32(1),
			}),
			Unique: true,
		})
	}

	return coll
}

// Find will lookup the documents that match the specified query.
func (c *Collection) Find(query, sort bsonkit.Doc, skip, limit int) (*Result, error) {
	// get documents
	list := c.Documents.List

	// sort documents
	var err error
	if sort != nil && len(*sort) > 0 {
		list, err = Sort(list, sort)
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
	list, err = Filter(list, query, limit)
	if err != nil {
		return nil, err
	}

	return &Result{
		Matched: list,
	}, nil
}

// Insert will add the specified document to the collection.
func (c *Collection) Insert(doc bsonkit.Doc) (*Result, error) {
	// ensure object id
	if bsonkit.Get(doc, "_id") == bsonkit.Missing {
		_, err := bsonkit.Put(doc, "_id", primitive.NewObjectID(), true)
		if err != nil {
			return nil, err
		}
	}

	// add document to all indexes
	for name, index := range c.Indexes {
		ok, err := index.Add(doc)
		if err != nil {
			return nil, err
		} else if !ok {
			return nil, fmt.Errorf("duplicate document for index %q", name)
		}
	}

	// add document
	if !c.Documents.Add(doc) {
		return nil, fmt.Errorf("unable to add document to collection")
	}

	return &Result{
		Modified: bsonkit.List{doc},
	}, nil
}

// Replace will lookup the first document that matches the query and if found
// replace it with the specified document.
func (c *Collection) Replace(query, repl, sort bsonkit.Doc) (*Result, error) {
	// get documents
	list := c.Documents.List

	// sort documents
	var err error
	if sort != nil && len(*sort) > 0 {
		list, err = Sort(list, sort)
		if err != nil {
			return nil, err
		}
	}

	// filter documents
	list, err = Filter(list, query, 1)
	if err != nil {
		return nil, err
	}

	// check list
	if len(list) == 0 {
		return &Result{}, nil
	}

	// set missing id or check existing id
	replID := bsonkit.Get(repl, "_id")
	if replID == bsonkit.Missing {
		_, err = bsonkit.Put(repl, "_id", bsonkit.Get(list[0], "_id"), true)
		if err != nil {
			return nil, err
		}
	} else if replID != bsonkit.Get(list[0], "_id") {
		return nil, fmt.Errorf("document _id is immutable")
	}

	// update indexes
	for name, index := range c.Indexes {
		// remove old document
		ok, err := index.Remove(list[0])
		if err != nil {
			return nil, err
		} else if !ok {
			return nil, fmt.Errorf("unable to remove document from index %q", name)
		}

		// add replacement
		ok, err = index.Add(repl)
		if err != nil {
			return nil, err
		} else if !ok {
			return nil, fmt.Errorf("duplicate document for index %q", name)
		}
	}

	// replace document
	if !c.Documents.Replace(list[0], repl) {
		return nil, fmt.Errorf("unable to replace document in collection")
	}

	return &Result{
		Matched:  list,
		Modified: bsonkit.List{repl},
	}, nil
}

// Update will lookup all documents that match the specified query and update
// them according to the update document.
func (c *Collection) Update(query, update, sort bsonkit.Doc, limit int) (*Result, error) {
	// get documents
	list := c.Documents.List

	// sort documents
	var err error
	if sort != nil && len(*sort) > 0 {
		list, err = Sort(list, sort)
		if err != nil {
			return nil, err
		}
	}

	// filter documents
	list, err = Filter(list, query, limit)
	if err != nil {
		return nil, err
	}

	// check list
	if len(list) == 0 {
		return &Result{}, nil
	}

	// clone documents
	newList := bsonkit.CloneList(list)

	// update documents
	changes, err := Update(newList, update, false)
	if err != nil {
		return nil, err
	}

	// check ids
	for i, doc := range newList {
		if bsonkit.Get(doc, "_id") != bsonkit.Get(list[i], "_id") {
			return nil, fmt.Errorf("document _id is immutable")
		}
	}

	// remove old docs from indexes
	for _, doc := range list {
		for name, index := range c.Indexes {
			ok, err := index.Remove(doc)
			if err != nil {
				return nil, err
			} else if !ok {
				return nil, fmt.Errorf("unable to remove document document from index %q", name)
			}
		}
	}

	// add new docs to indexes
	for _, doc := range newList {
		for name, index := range c.Indexes {
			ok, err := index.Add(doc)
			if err != nil {
				return nil, err
			} else if !ok {
				return nil, fmt.Errorf("duplicate document for index %q", name)
			}
		}
	}

	// replace documents
	for i, doc := range newList {
		if !c.Documents.Replace(list[i], doc) {
			return nil, fmt.Errorf("unable to replace document in collection")
		}
	}

	return &Result{
		Matched:  list,
		Modified: newList,
		Changes:  changes,
	}, nil
}

// Upsert will insert a document based on the specified query and either the
// replacement document or update document.
func (c *Collection) Upsert(query, repl, update bsonkit.Doc) (*Result, error) {
	// extract query
	doc, err := Extract(query)
	if err != nil {
		return nil, err
	}

	// check exclusiveness
	if repl != nil && update != nil {
		return nil, fmt.Errorf("cannot upsert with replacement and update")
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
			_, err = bsonkit.Put(doc, "_id", replID, true)
			if err != nil {
				return nil, err
			}
		} else if queryID != bsonkit.Missing {
			_, err = bsonkit.Put(doc, "_id", queryID, true)
			if err != nil {
				return nil, err
			}
		}
	}

	// apply update if present
	if update != nil {
		_, err = Apply(doc, update, true)
		if err != nil {
			return nil, err
		}
	}

	// generate object id if missing
	if bsonkit.Get(doc, "_id") == bsonkit.Missing {
		_, err := bsonkit.Put(doc, "_id", primitive.NewObjectID(), true)
		if err != nil {
			return nil, err
		}
	}

	// add document to indexes
	for name, index := range c.Indexes {
		ok, err := index.Add(doc)
		if err != nil {
			return nil, err
		} else if !ok {
			return nil, fmt.Errorf("duplicate document for index %q", name)
		}
	}

	// add document
	if !c.Documents.Add(doc) {
		return nil, fmt.Errorf("unable to add document to collection")
	}

	return &Result{
		Upserted: doc,
	}, nil
}

// Delete will remove all documents that match the specified query.
func (c *Collection) Delete(query, sort bsonkit.Doc, limit int) (*Result, error) {
	// get documents
	list := c.Documents.List

	// sort documents
	var err error
	if sort != nil && len(*sort) > 0 {
		list, err = Sort(list, sort)
		if err != nil {
			return nil, err
		}
	}

	// filter documents
	list, err = Filter(list, query, limit)
	if err != nil {
		return nil, err
	}

	// update indexes
	for _, doc := range list {
		for name, index := range c.Indexes {
			ok, err := index.Remove(doc)
			if err != nil {
				return nil, err
			} else if !ok {
				return nil, fmt.Errorf("unable to remove document from index %q", name)
			}
		}
	}

	// remove documents
	for _, doc := range list {
		if !c.Documents.Remove(doc) {
			return nil, fmt.Errorf("unable to remove document from collection")
		}
	}

	return &Result{
		Matched: list,
	}, nil
}

// CreateIndex will create and build and index based on the specified configuration.
func (c *Collection) CreateIndex(name string, config IndexConfig) (string, error) {
	// check duplicate
	for name, index := range c.Indexes {
		if bsonkit.Compare(*config.Key, *index.Config().Key) == 0 {
			return "", fmt.Errorf("existing index %q has same key", name)
		}
	}

	// create index
	index, err := CreateIndex(config)
	if err != nil {
		return "", err
	}

	// use generated name if missing
	if name == "" {
		name = index.Name()
	}

	// add index
	c.Indexes[name] = index

	// build index
	ok, err := index.Build(c.Documents.List)
	if err != nil {
		return "", err
	} else if !ok {
		return "", fmt.Errorf("duplicate document for index %q", name)
	}

	return name, nil
}

// DropIndex will drop the specific index or drop all indexes if no name has
// been specified.
func (c *Collection) DropIndex(name string) ([]string, error) {
	// collect dropped
	var dropped []string

	// drop single index
	if name != "" {
		// check existence
		if _, ok := c.Indexes[name]; !ok {
			return nil, fmt.Errorf("missing index %q", name)
		}

		// drop index
		delete(c.Indexes, name)

		// add name
		dropped = append(dropped, name)
	}

	// drop all indexes
	if name == "" {
		for name := range c.Indexes {
			if name != "_id_" {
				// drop index
				delete(c.Indexes, name)

				// add name
				dropped = append(dropped, name)
			}
		}
	}

	return dropped, nil
}

// Clone will clone the collection.
func (c *Collection) Clone() *Collection {
	// create new collection
	clone := &Collection{
		Documents: c.Documents.Clone(),
		Indexes:   map[string]*Index{},
	}

	// clone indexes
	for name, index := range c.Indexes {
		clone.Indexes[name] = index.Clone()
	}

	return clone
}
