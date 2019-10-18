package lungo

import (
	"github.com/tidwall/btree"

	"github.com/256dpi/lungo/bsonkit"
)

type uniqueIndexItem struct {
	doc bsonkit.Doc
}

func (i *uniqueIndexItem) Less(item btree.Item, ctx interface{}) bool {
	// coerce item and context
	j := item.(*uniqueIndexItem)
	path := ctx.(string)

	// get values
	v1 := bsonkit.Get(i.doc, path)
	v2 := bsonkit.Get(j.doc, path)

	// compare values
	ret := bsonkit.Compare(v1, v2)

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
		i.Set(doc)
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
