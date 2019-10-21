package lungo

import (
	"github.com/tidwall/btree"

	"github.com/256dpi/lungo/bsonkit"
)

type uniqueIndexItem struct {
	doc bsonkit.Doc
}

func (i *uniqueIndexItem) Less(item btree.Item, ctx interface{}) bool {
	// coerce item
	j := item.(*uniqueIndexItem)

	// coerce columns
	columns := ctx.([]bsonkit.Column)

	// get order
	order := bsonkit.Order(i.doc, j.doc, columns)

	return order < 0
}

type uniqueIndex struct {
	btree *btree.BTree
}

func newUniqueIndex(columns []bsonkit.Column) *uniqueIndex {
	return &uniqueIndex{
		btree: btree.New(64, columns),
	}
}

func (i *uniqueIndex) Set(doc bsonkit.Doc) bool {
	return i.btree.ReplaceOrInsert(&uniqueIndexItem{doc: doc}) == nil
}

func (i *uniqueIndex) Has(doc bsonkit.Doc) bool {
	return i.btree.Has(&uniqueIndexItem{doc: doc})
}

func (i *uniqueIndex) Delete(doc bsonkit.Doc) {
	i.btree.Delete(&uniqueIndexItem{doc: doc})
}

func (i *uniqueIndex) Clone() *uniqueIndex {
	return &uniqueIndex{
		btree: i.btree.Clone(),
	}
}
