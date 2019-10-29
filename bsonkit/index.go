package bsonkit

import (
	"sync"

	"github.com/tidwall/btree"
)

type entry struct {
	idx *Index
	doc Doc
}

func (i *entry) Less(item btree.Item, ctx interface{}) bool {
	// coerce item
	j := item.(*entry)

	// get order
	order := Order(i.doc, j.doc, i.idx.columns)
	if order != 0 {
		return order < 0
	}

	// check document identity if not unique
	if !i.idx.unique && i.doc != j.doc {
		return true
	}

	return false
}

// Index is a basic btree based index for documents.
type Index struct {
	unique   bool
	columns  []Column
	btree    *btree.BTree
	sentinel *entry
	mutex    sync.Mutex
}

// NewIndex creates and returns a new index.
func NewIndex(unique bool, columns []Column) *Index {
	return &Index{
		unique:   unique,
		columns:  columns,
		btree:    btree.New(64, nil),
		sentinel: &entry{},
	}
}

// Build will build the index from the specified list. It may return false if
// there was an unique constraint error when building the index.
func (i *Index) Build(list List) bool {
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
	i.sentinel.idx = i
	i.sentinel.doc = doc

	// check if index already has an entry
	item := i.btree.Get(i.sentinel)
	if item != nil {
		return false
	}

	// otherwise add entry
	i.btree.ReplaceOrInsert(&entry{idx: i, doc: doc})

	return true
}

// Has returns whether the specified document has been added to the index.
func (i *Index) Has(doc Doc) bool {
	// acquire mutex
	i.mutex.Lock()
	defer i.mutex.Unlock()

	// prepare sentinel entry
	i.sentinel.idx = i
	i.sentinel.doc = doc

	// check if index already has an item
	item := i.btree.Get(i.sentinel)
	if item != nil {
		return true
	}

	return false
}

// Remove will remove a document from the index. May return false if the document
// has not yet been added to the index.
func (i *Index) Remove(doc Doc) bool {
	// acquire mutex
	i.mutex.Lock()
	defer i.mutex.Unlock()

	// prepare sentinel entry
	i.sentinel.idx = i
	i.sentinel.doc = doc

	// check if index already has an item
	item := i.btree.Get(i.sentinel)
	if item == nil {
		return false
	}

	// otherwise remove entry
	i.btree.Delete(item)

	return true
}

// Clone will clone the index. Mutating the new index will not mutate the original
// index.
func (i *Index) Clone() *Index {
	// acquire mutex
	i.mutex.Lock()
	defer i.mutex.Unlock()

	return &Index{
		unique:   i.unique,
		columns:  i.columns,
		btree:    i.btree.Clone(),
		sentinel: &entry{},
	}
}
