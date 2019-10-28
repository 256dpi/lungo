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

	// coerce tree
	tree := ctx.(*Tree)

	// get order
	order := Order(i.set.List[0], j.set.List[0], tree.Columns)

	return order < 0
}

type Tree struct {
	Unique  bool     `bson:"unique"`
	Columns []Column `bson:"columns"`

	btree    *btree.BTree `bson:"-"`
	sentinel *entry       `bson:"-"`
	mutex    sync.Mutex   `bson:"-"`
}

func NewTree(unique bool, columns []Column) *Tree {
	return (&Tree{
		Unique:  unique,
		Columns: columns,
	}).Prepare(nil)
}

func (t *Tree) Prepare(list List) *Tree {
	// create btree
	t.btree = btree.New(64, t)

	// create sentinel
	t.sentinel = &entry{
		set: NewSet(make(List, 1)),
	}

	// add documents
	for _, doc := range list {
		t.Add(doc)
	}

	return t
}

func (t *Tree) Add(doc Doc) bool {
	// acquire mutex
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// prepare sentinel entry
	t.sentinel.set.List[0] = doc

	// check if tree already has an item
	item := t.btree.Get(t.sentinel)

	// just add a new entry if missing
	if item == nil {
		t.btree.ReplaceOrInsert(&entry{
			set: NewSet(List{doc}),
		})
		return true
	}

	// return false if tree is unique
	if t.Unique {
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

func (t *Tree) Has(doc Doc) bool {
	// acquire mutex
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// prepare sentinel entry
	t.sentinel.set.List[0] = doc

	// check if tree already has an item
	item := t.btree.Get(t.sentinel)

	// return if there is no item
	if item == nil {
		return false
	}

	// do not check identify if unique
	if t.Unique {
		return true
	}

	// get entry
	entry := item.(*entry)

	// check index
	_, ok := entry.set.Index[doc]

	return ok
}

func (t *Tree) Remove(doc Doc) bool {
	// acquire mutex
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// prepare sentinel entry
	t.sentinel.set.List[0] = doc

	// check if tree already has an item
	item := t.btree.Get(t.sentinel)

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
		t.btree.Delete(entry)
		return true
	}

	// remove from set
	ok = entry.set.Remove(doc)
	if !ok {
		return false
	}

	return true
}

func (t *Tree) Clone() *Tree {
	// acquire mutex
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// create clone
	clone := NewTree(t.Unique, t.Columns)

	// copy entries
	t.btree.Ascend(func(i btree.Item) bool {
		clone.btree.ReplaceOrInsert(&entry{
			set: i.(*entry).set.Clone(),
		})
		return true
	})

	return clone
}
