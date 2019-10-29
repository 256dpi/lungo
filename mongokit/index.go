package mongokit

import (
	"strconv"
	"strings"

	"github.com/256dpi/lungo/bsonkit"
)

// TODO: Return Match errors?

// TODO: Whitelist Match operators?

// IndexConfig defines an index configuration.
type IndexConfig struct {
	// The index key.
	Key bsonkit.Doc

	// Whether the index is unique.
	Unique bool

	// The partial index filter.
	Partial bsonkit.Doc
}

// Index is an index for documents that supports MongoDB features.
type Index struct {
	config  IndexConfig
	columns []bsonkit.Column
	base    *bsonkit.Index
}

// CreateIndex will create and return a new index.
func CreateIndex(config IndexConfig) (*Index, error) {
	// clone key and partial
	config.Key = bsonkit.Clone(config.Key)
	config.Partial = bsonkit.Clone(config.Partial)

	// parse columns
	columns, err := Columns(config.Key)
	if err != nil {
		return nil, err
	}

	// create index
	index := &Index{
		config:  config,
		columns: columns,
		base:    bsonkit.NewIndex(config.Unique, columns),
	}

	return index, nil
}

// Build will build the index from the specified list. It may return false if
// there was an unique constraint error when building the index.
func (i *Index) Build(list bsonkit.List) bool {
	return i.base.Build(list)
}

// Add will add the document to index. May return false if the document has
// already been added to the index. If the document has been skipped due to a
// partial filter true is returned.
func (i *Index) Add(doc bsonkit.Doc) bool {
	// skip documents that do not match partial expression
	if i.config.Partial != nil {
		ok, _ := Match(doc, i.config.Partial)
		if !ok {
			return true
		}
	}

	return i.base.Add(doc)
}

// Has returns whether the specified document has been added to the index.
func (i *Index) Has(doc bsonkit.Doc) bool {
	// skip documents that do not match partial expression
	if i.config.Partial != nil {
		ok, _ := Match(doc, i.config.Partial)
		if !ok {
			return false
		}
	}

	return i.base.Has(doc)
}

// Remove will remove a document from the index. May return false if the document
// has not yet been added to the index.
func (i *Index) Remove(doc bsonkit.Doc) bool {
	// skip documents that do not match partial expression
	if i.config.Partial != nil {
		ok, _ := Match(doc, i.config.Partial)
		if !ok {
			return true
		}
	}

	return i.base.Remove(doc)
}

// Config will return the index configuration.
func (i *Index) Config() IndexConfig {
	return IndexConfig{
		Key:     bsonkit.Clone(i.config.Key),
		Unique:  i.config.Unique,
		Partial: bsonkit.Clone(i.config.Partial),
	}
}

// Name will return the computed index name.
func (i *Index) Name() string {
	// generate name
	segments := make([]string, 0, len(i.columns)*2)
	for _, column := range i.columns {
		var dir = 1
		if column.Reverse {
			dir = -1
		}
		segments = append(segments, column.Path, strconv.Itoa(dir))
	}

	// assemble name
	name := strings.Join(segments, "_")

	return name
}

// Clone will clone the index. Mutating the new index will not mutate the
// original index.
func (i *Index) Clone() *Index {
	return &Index{
		config:  i.config,
		columns: i.columns,
		base:    i.base.Clone(),
	}
}
