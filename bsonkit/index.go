package bsonkit

import (
	"github.com/256dpi/btree"
)

type entry struct {
	doc Doc
}

func (i *entry) Less(item btree.Item, ctx interface{}) bool {
	// coerce item
	j := item.(*entry)

	// coerce index
	index := ctx.(*Index)

	// get order
	order := Order(i.doc, j.doc, index.columns)
	if order != 0 {
		return order < 0
	}

	// check document identity if not unique
	if !index.unique && i.doc != j.doc {
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
}

// NewIndex creates and returns a new index.
func NewIndex(unique bool, columns []Column) *Index {
	// create index
	index := &Index{
		unique:   unique,
		columns:  columns,
		sentinel: &entry{},
	}

	// create btree
	index.btree = btree.New(64, index)

	return index
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
	// prepare sentinel
	i.sentinel.doc = doc

	// check if index already has an entry
	item := i.btree.Get(i.sentinel)
	if item != nil {
		return false
	}

	// otherwise add entry
	i.btree.ReplaceOrInsert(&entry{doc: doc})

	return true
}

// Has returns whether the specified document has been added to the index.
func (i *Index) Has(doc Doc) bool {
	// prepare sentinel
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
	// prepare sentinel
	i.sentinel.doc = doc

	// remove entry
	item := i.btree.Delete(i.sentinel)
	if item == nil {
		return false
	}

	return true
}

// Clone will clone the index. Mutating the new index will not mutate the original
// index.
func (i *Index) Clone() *Index {
	// create clone
	clone := &Index{
		unique:   i.unique,
		columns:  i.columns,
		btree:    i.btree.Clone(),
		sentinel: &entry{},
	}

	// clone btree
	clone.btree = i.btree.Clone()

	// update context
	clone.btree.SetContext(clone)

	return clone
}
