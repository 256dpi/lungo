package bsonkit

import (
	"sync"

	"github.com/tidwall/btree"
)

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
	Unique bool

	// The columns that specify the index.
	Columns []Column

	btree    *btree.BTree
	sentinel *entry
	mutex    sync.Mutex
}

// NewIndex creates and returns a new index.
func NewIndex(unique bool, columns []Column) *Index {
	// create index
	index := &Index{
		Unique:  unique,
		Columns: columns,
	}

	// create btree
	index.btree = btree.New(64, index)

	// create sentinel
	index.sentinel = &entry{
		set: NewSet(make(List, 1)),
	}

	return index
}

// Build will build the index from the specified list. It may return false if
// there was an unique constraint error when building the index.
func (i *Index) Build(list List) bool {
	// TODO: Add a fast build method.

	// add documents
	for _, doc := range list {
		if !i.Add(doc) {
			return false
		}
	}

	return true
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
