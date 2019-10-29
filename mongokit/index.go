package mongokit

import (
	"strconv"
	"strings"

	"github.com/256dpi/lungo/bsonkit"
)

// Index is an index for documents that supports MongoDB features.
type Index struct {
	key     bsonkit.Doc
	columns []bsonkit.Column
	unique  bool

	*bsonkit.Index
}

// CreateIndex will create and return a new index.
func CreateIndex(key bsonkit.Doc, unique bool) (*Index, error) {
	// clone key
	key = bsonkit.Clone(key)

	// parse columns
	columns, err := Columns(key)
	if err != nil {
		return nil, err
	}

	// create index
	index := &Index{
		key:     key,
		columns: columns,
		unique:  unique,
		Index:   bsonkit.NewIndex(unique, columns),
	}

	return index, nil
}

// Config will return the index configuration.
func (i *Index) Config() (bsonkit.Doc, bool) {
	return bsonkit.Clone(i.key), i.unique
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
		key:     i.key,
		columns: i.columns,
		unique:  i.unique,
		Index:   i.Index.Clone(),
	}
}
