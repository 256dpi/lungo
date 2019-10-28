package bsonkit

import (
	"sync"

	"github.com/tidwall/btree"
)

// TODO: Add a fast build method.

type entry struct {
	set *Set
}

func (i *entry) Less(item btree.Item, ctx interface{}) bool {
	// coerce item
	j := item.(*entry)

	// coerce index
	index := ctx.(*Index)

	// get order
	order := Order(i.set.List[0], j.set.List[0], index.Columns)

	return order < 0
}

// Index is a basic btree based index for documents.
type Index struct {
	// Whether documents must have unique value.
	Unique bool `bson:"unique"`

	// The columns that specify the index.
	Columns []Column `bson:"columns"`

	btree    *btree.BTree `bson:"-"`
	sentinel *entry       `bson:"-"`
	mutex    sync.Mutex   `bson:"-"`
}

// NewIndex creates and returns a new index.
func NewIndex(unique bool, columns []Column) *Index {
	return (&Index{
		Unique:  unique,
		Columns: columns,
	}).Prepare(nil)
}

// Prepare will reset the index and build it form the specified list of documents.
func (i *Index) Prepare(list List) *Index {
	// create btree
	i.btree = btree.New(64, i)

	// create sentinel
	i.sentinel = &entry{
		set: NewSet(make(List, 1)),
	}

	// add documents
	for _, doc := range list {
		i.Add(doc)
	}

	return i
}

// Add will add the document to index. May return false if the document has
// already been added to the index.
func (i *Index) Add(doc Doc) bool {
	// acquire mutex
	i.mutex.Lock()
	defer i.mutex.Unlock()

	// prepare sentinel entry
	i.sentinel.set.List[0] = doc

	// check if index already has an item
	item := i.btree.Get(i.sentinel)

	// just add a new entry if missing
	if item == nil {
		i.btree.ReplaceOrInsert(&entry{
			set: NewSet(List{doc}),
		})
		return true
	}

	// return false if index is unique
	if i.Unique {
		return false
	}

	// get existing entry
	entry := item.(*entry)

	// add document to existing entry
	ok := entry.set.Add(doc)
	if !ok {
		return false
	}

	return true
}

// Has returns whether the specified document has been added to the index.
func (i *Index) Has(doc Doc) bool {
	// acquire mutex
	i.mutex.Lock()
	defer i.mutex.Unlock()

	// prepare sentinel entry
	i.sentinel.set.List[0] = doc

	// check if index already has an item
	item := i.btree.Get(i.sentinel)

	// return if there is no item
	if item == nil {
		return false
	}

	// do not check identify if unique
	if i.Unique {
		return true
	}

	// get entry
	entry := item.(*entry)

	// check index
	_, ok := entry.set.Index[doc]

	return ok
}

// Remove will remove a document from the index. May return false if the document
// has not yet been added to the index.
func (i *Index) Remove(doc Doc) bool {
	// acquire mutex
	i.mutex.Lock()
	defer i.mutex.Unlock()

	// prepare sentinel entry
	i.sentinel.set.List[0] = doc

	// check if index already has an item
	item := i.btree.Get(i.sentinel)

	// return if there is no item
	if item == nil {
		return false
	}

	// get entry
	entry := item.(*entry)

	// check existence
	_, ok := entry.set.Index[doc]
	if !ok {
		return false
	}

	// remove entry if last in list
	if len(entry.set.List) == 1 {
		i.btree.Delete(entry)
		return true
	}

	// remove from set
	ok = entry.set.Remove(doc)
	if !ok {
		return false
	}

	return true
}

// Clone will clone the index. Mutating the new index will not mutate the original
// index.
func (i *Index) Clone() *Index {
	// acquire mutex
	i.mutex.Lock()
	defer i.mutex.Unlock()

	// create clone
	clone := NewIndex(i.Unique, i.Columns)

	// copy entries
	i.btree.Ascend(func(i btree.Item) bool {
		clone.btree.ReplaceOrInsert(&entry{
			set: i.(*entry).set.Clone(),
		})
		return true
	})

	return clone
}
