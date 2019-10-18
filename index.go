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

	// coerce context
	path := ctx.(string)

	// get ids
	id1 := bsonkit.Get(i.doc, path)
	id2 := bsonkit.Get(j.doc, path)

	// compare ids
	ret := bsonkit.Compare(id1, id2)

	return ret < 0
}

type uniqueIndex struct {
	btree *btree.BTree
}

func newUniqueIndex(path string) *uniqueIndex {
	return &uniqueIndex{
		btree: btree.New(64, path),
	}
}

func (i *uniqueIndex) Fill(list bsonkit.List) {
	for _, doc := range list {
		i.btree.ReplaceOrInsert(&uniqueIndexItem{doc: doc})
	}
}

func (i *uniqueIndex) Set(doc bsonkit.Doc) {
	i.btree.ReplaceOrInsert(&uniqueIndexItem{doc: doc})
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
