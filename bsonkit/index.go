package bsonkit

import (
	"unsafe"

	"github.com/tidwall/btree"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// indexEntry is one (key tuple, doc) pair stored in the index btree. A document
// with array values at indexed paths produces multiple entries — one per
// element combination — to mirror MongoDB's multikey indexing.
type indexEntry struct {
	keys []interface{}
	doc  Doc
}

// Index is a basic btree based index for documents. The index is not safe from
// concurrent access.
type Index struct {
	unique  bool
	columns []Column
	btree   *btree.BTreeG[indexEntry]
}

// NewIndex creates and returns a new index.
func NewIndex(unique bool, columns []Column) *Index {
	// prepare less function
	less := func(a, b indexEntry) bool {
		for i, col := range columns {
			res := Compare(a.keys[i], b.keys[i])
			if col.Reverse {
				res = -res
			}
			if res != 0 {
				return res < 0
			}
		}
		// tiebreak by document identity so multiple non-unique entries with
		// equal keys can coexist; for unique indexes the conflict is detected
		// before insert
		return uintptr(unsafe.Pointer(a.doc)) < uintptr(unsafe.Pointer(b.doc))
	}

	return &Index{
		btree:   btree.NewBTreeG[indexEntry](less),
		columns: columns,
		unique:  unique,
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
// already been added or, for unique indexes, conflicts with another document.
func (i *Index) Add(doc Doc) bool {
	// get tuples
	tuples := i.tuples(doc)

	// already added?
	if _, ok := i.btree.Get(indexEntry{keys: tuples[0], doc: doc}); ok {
		return false
	}

	// for unique, ensure no key collides with another doc
	if i.unique {
		for _, t := range tuples {
			if i.hasKey(t) {
				return false
			}
		}
	}

	// add tuples
	for _, t := range tuples {
		i.btree.Set(indexEntry{keys: t, doc: doc})
	}

	return true
}

// Has returns whether the specified document has been added to the index. For
// unique indexes it returns true when any document with matching key values is
// indexed, mirroring uniqueness semantics.
func (i *Index) Has(doc Doc) bool {
	// get tuples
	tuples := i.tuples(doc)

	// for unique, check by key only
	if i.unique {
		return i.hasKey(tuples[0])
	}

	// otherwise check by identity
	_, ok := i.btree.Get(indexEntry{keys: tuples[0], doc: doc})

	return ok
}

// Remove will remove a document from the index. May return false if the
// document has not yet been added to the index.
func (i *Index) Remove(doc Doc) bool {
	// get tuples
	tuples := i.tuples(doc)

	// check if added
	if _, ok := i.btree.Get(indexEntry{keys: tuples[0], doc: doc}); !ok {
		return false
	}

	// remove all entries
	for _, t := range tuples {
		i.btree.Delete(indexEntry{keys: t, doc: doc})
	}

	return true
}

// List will return an ascending list of all documents in the index. Documents
// with array values appear once even though they are stored under multiple
// entries.
func (i *Index) List() List {
	// prepare list and dedup set
	seen := make(map[Doc]struct{}, i.btree.Len())
	list := make(List, 0, i.btree.Len())

	// walk index, dedup by doc
	i.btree.Scan(func(e indexEntry) bool {
		if _, ok := seen[e.doc]; !ok {
			seen[e.doc] = struct{}{}
			list = append(list, e.doc)
		}
		return true
	})

	return list
}

// Clone will clone the index. Mutating the new index will not mutate the original
// index.
func (i *Index) Clone() *Index {
	// create clone
	clone := &Index{
		btree:   i.btree.Copy(),
		columns: i.columns,
		unique:  i.unique,
	}

	return clone
}

// tuples generates the index keys for a document. Array values at indexed
// paths are expanded one element per tuple, taking the Cartesian product
// across columns to mirror MongoDB's multikey indexing. Always returns at
// least one tuple.
//
// TODO: Reject parallel arrays — MongoDB errors when a compound index would
// need to be multikey on more than one field of the same document.
func (i *Index) tuples(doc Doc) [][]interface{} {
	// start with one empty tuple
	tuples := [][]interface{}{
		make([]interface{}, 0, len(i.columns)),
	}

	// extend with each column
	for _, col := range i.columns {
		// get value at path, collecting through arrays of embedded documents
		// and flattening nested arrays so multikey works on paths like
		// "pets.name" or "pets.tags"
		v, _ := All(doc, col.Path, true, true)

		// expand arrays; an empty array indexes under itself, distinct from
		// Missing, matching MongoDB's empty-array key
		var values []interface{}
		if a, ok := v.(bson.A); ok {
			if len(a) == 0 {
				values = []interface{}{a}
			} else {
				values = a
			}
		} else {
			values = []interface{}{v}
		}

		// extend tuples with Cartesian product
		next := make([][]interface{}, 0, len(tuples)*len(values))
		for _, t := range tuples {
			for _, val := range values {
				nt := make([]interface{}, len(t)+1)
				copy(nt, t)
				nt[len(t)] = val
				next = append(next, nt)
			}
		}
		tuples = next
	}

	return tuples
}

// hasKey returns whether any entry with the given key tuple exists. It is
// used to detect unique-index collisions independent of document identity.
func (i *Index) hasKey(keys []interface{}) bool {
	// probe ascending from a nil-doc entry to land on the smallest entry
	// with matching keys (any doc) or, if none match, the next greater entry
	probe := indexEntry{keys: keys}

	// check first item for key equality
	var found bool
	i.btree.Ascend(probe, func(e indexEntry) bool {
		for j := range i.columns {
			if Compare(e.keys[j], keys[j]) != 0 {
				return false
			}
		}
		found = true
		return false
	})

	return found
}
