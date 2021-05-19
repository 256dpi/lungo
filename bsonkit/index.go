package bsonkit

import "github.com/tidwall/btree"

// Index is a basic btree based index for documents. The index is not safe from
// concurrent access.
type Index struct {
	unique  bool
	columns []Column
	btree   *btree.BTree
}

// NewIndex creates and returns a new index.
func NewIndex(unique bool, columns []Column) *Index {
	// create index
	index := &Index{
		unique:  unique,
		columns: columns,
	}

	// create btree
	index.btree = btree.New(func(a, b interface{}) bool {
		return index.less(a.(Doc), b.(Doc))
	})

	return index
}

// Build will build the index from the specified list. It may return false if
// there was an unique constraint error when building the index. If an error
// is returned the index only has some of the provided documents added.
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
	// check if index already has an entry
	item := i.btree.Get(doc)
	if item != nil {
		return false
	}

	// otherwise add entry
	i.btree.Set(doc)

	return true
}

// Has returns whether the specified document has been added to the index.
func (i *Index) Has(doc Doc) bool {
	// check if index already has an item
	item := i.btree.Get(doc)
	if item != nil {
		return true
	}

	return false
}

// Remove will remove a document from the index. May return false if the document
// has not yet been added to the index.
func (i *Index) Remove(doc Doc) bool {
	// remove entry
	item := i.btree.Delete(doc)
	if item == nil {
		return false
	}

	return true
}

// List will return an ascending list of all documents in the index.
func (i *Index) List() List {
	// prepare list
	list := make(List, 0, i.btree.Len())

	// walk index
	i.btree.Ascend(nil, func(item interface{}) bool {
		list = append(list, item.(Doc))
		return true
	})

	return list
}

// Clone will clone the index. Mutating the new index will not mutate the original
// index.
func (i *Index) Clone() *Index {
	// create clone
	clone := &Index{
		unique:  i.unique,
		columns: i.columns,
		btree:   i.btree.Copy(),
	}

	// update less
	clone.btree.SetLess(func(a, b interface{}) bool {
		return clone.less(a.(Doc), b.(Doc))
	})

	return clone
}

func (i *Index) less(a, b Doc) bool {
	return Order(a, b, i.columns, !i.unique) < 0
}
