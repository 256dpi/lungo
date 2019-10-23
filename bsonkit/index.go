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
	order := Order(i.set.List[0], j.set.List[0], index.columns)

	return order < 0
}

type Index struct {
	unique   bool
	columns  []Column
	btree    *btree.BTree
	sentinel *entry
	mutex    sync.Mutex
}

func NewIndex(unique bool, columns []Column) *Index {
	// create index
	index := &Index{
		unique:  unique,
		columns: columns,
		sentinel: &entry{
			set: NewSet(make(List, 1)),
		},
	}

	// create btree
	index.btree = btree.New(64, index)

	return index
}

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
	if i.unique {
		return false
	}

	// get existing entry
	entry := item.(*entry)

	// add document to existing entry
	entry.set.Add(doc)

	return true
}

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
	if i.unique {
		return true
	}

	// get entry
	entry := item.(*entry)

	// check index
	_, ok := entry.set.Index[doc]

	return ok
}

func (i *Index) Remove(doc Doc) {
	// acquire mutex
	i.mutex.Lock()
	defer i.mutex.Unlock()

	// prepare sentinel entry
	i.sentinel.set.List[0] = doc

	// check if index already has an item
	item := i.btree.Get(i.sentinel)

	// return if there is no item
	if item == nil {
		return
	}

	// get entry
	entry := item.(*entry)

	// check existence
	_, ok := entry.set.Index[doc]
	if !ok {
		return
	}

	// remove entry if last in list
	if len(entry.set.List) == 1 {
		i.btree.Delete(entry)
		return
	}

	// remove from set
	entry.set.Remove(doc)
}

func (i *Index) Clone() *Index {
	// acquire mutex
	i.mutex.Lock()
	defer i.mutex.Unlock()

	// create new index
	index := NewIndex(i.unique, i.columns)

	// copy over items
	i.btree.Ascend(func(i btree.Item) bool {
		// clone entry
		clone := &entry{set: i.(*entry).set.Clone()}

		// add clone
		index.btree.ReplaceOrInsert(clone)

		return true
	})

	return index
}
