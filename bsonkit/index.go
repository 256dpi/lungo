package bsonkit

import "github.com/tidwall/btree"

// Index is a basic btree based index for documents. The index is not safe from
// concurrent access.
type Index struct {
	btree *btree.BTreeG[Doc]
}

// NewIndex creates and returns a new index.
func NewIndex(unique bool, columns []Column) *Index {
	return &Index{
		btree: btree.NewBTreeG[Doc](func(a, b Doc) bool {
			return Order(a, b, columns, !unique) < 0
		}),
	}
}

// Build will build the index from the specified list. It may return false if
// there was a unique constraint error when building the index. If an error
// is returned the index only has some documents added.
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
	item, _ := i.btree.Get(doc)
	if item != nil {
		return false
	}

	// otherwise, add entry
	i.btree.Set(doc)

	return true
}

// Has returns whether the specified document has been added to the index.
func (i *Index) Has(doc Doc) bool {
	// check if index already has an item
	item, _ := i.btree.Get(doc)
	return item != nil
}

// Remove will remove a document from the index. May return false if the document
// has not yet been added to the index.
func (i *Index) Remove(doc Doc) bool {
	// remove entry
	item, _ := i.btree.Delete(doc)
	return item != nil
}

// List will return an ascending list of all documents in the index.
func (i *Index) List() List {
	// prepare list
	list := make(List, 0, i.btree.Len())

	// walk index
	i.btree.Scan(func(item Doc) bool {
		list = append(list, item)
		return true
	})

	return list
}

// Clone will clone the index. Mutating the new index will not mutate the original
// index.
func (i *Index) Clone() *Index {
	// create clone
	clone := &Index{
		btree: i.btree.Copy(),
	}

	return clone
}
