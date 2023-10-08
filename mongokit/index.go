package mongokit

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

// TODO: Allow-list supported query operators?

// IndexConfig defines an index configuration.
type IndexConfig struct {
	// The index key.
	Key bsonkit.Doc

	// Whether the index is unique.
	Unique bool

	// The partial index filter.
	Partial bsonkit.Doc

	// The time after documents expire.
	Expiry time.Duration
}

// Equal will compare to configurations and return whether they are equal.
func (c IndexConfig) Equal(d IndexConfig) bool {
	// check key
	if bsonkit.Compare(*c.Key, *d.Key) != 0 {
		return false
	}

	// check unique
	if c.Unique != d.Unique {
		return false
	}

	// check partials
	var p1, p2 bson.D
	if c.Partial != nil {
		p1 = *c.Partial
	}
	if d.Partial != nil {
		p2 = *d.Partial
	}
	if bsonkit.Compare(p1, p2) != 0 {
		return false
	}

	// check expiry
	if c.Expiry != d.Expiry {
		return false
	}

	return true
}

// Name will return the computed index name.
func (c IndexConfig) Name() (string, error) {
	// get columns
	columns, err := Columns(c.Key)
	if err != nil {
		return "", err
	}

	// generate name
	segments := make([]string, 0, len(columns)*2)
	for _, column := range columns {
		var dir = 1
		if column.Reverse {
			dir = -1
		}
		segments = append(segments, column.Path, strconv.Itoa(dir))
	}

	// assemble name
	name := strings.Join(segments, "_")

	return name, nil
}

// Index is an index for documents that supports MongoDB features. The index is
// not safe from concurrent access and does not roll back changes on errors.
// Therefore, the recommended approach is to clone the index before making changes.
type Index struct {
	config  IndexConfig
	columns []bsonkit.Column
	base    *bsonkit.Index
}

// CreateIndex will create and return a new index.
func CreateIndex(config IndexConfig) (*Index, error) {
	// check key
	if len(*config.Key) == 0 {
		return nil, fmt.Errorf("empty index key")
	}

	// clone key and partial
	config.Key = bsonkit.Clone(config.Key)
	config.Partial = bsonkit.Clone(config.Partial)

	// parse columns
	columns, err := Columns(config.Key)
	if err != nil {
		return nil, err
	}

	// enforce single field ttl index
	if config.Expiry > 0 && len(*config.Key) > 1 {
		return nil, fmt.Errorf("invalid expiring compound index")
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
// there was a unique constraint error when building the index.
func (i *Index) Build(list bsonkit.List) (bool, error) {
	// add documents
	for _, doc := range list {
		ok, err := i.Add(doc)
		if err != nil {
			return false, err
		} else if !ok {
			return false, nil
		}
	}

	return true, nil
}

// Add will add the document to index. May return false if the document has
// already been added to the index. If the document has been skipped due to a
// partial filter true is returned.
func (i *Index) Add(doc bsonkit.Doc) (bool, error) {
	// skip documents that do not match partial expression
	if i.config.Partial != nil {
		ok, err := Match(doc, i.config.Partial)
		if err != nil {
			return false, err
		} else if !ok {
			return true, nil
		}
	}

	return i.base.Add(doc), nil
}

// Has returns whether the specified document has been added to the index.
func (i *Index) Has(doc bsonkit.Doc) (bool, error) {
	// skip documents that do not match partial expression
	if i.config.Partial != nil {
		ok, err := Match(doc, i.config.Partial)
		if err != nil {
			return false, err
		} else if !ok {
			return false, nil
		}
	}

	return i.base.Has(doc), nil
}

// Remove will remove a document from the index. May return false if the document
// has not yet been added to the index.
func (i *Index) Remove(doc bsonkit.Doc) (bool, error) {
	// skip documents that do not match partial expression
	if i.config.Partial != nil {
		ok, err := Match(doc, i.config.Partial)
		if err != nil {
			return false, err
		} else if !ok {
			return true, nil
		}
	}

	return i.base.Remove(doc), nil
}

// List will return an ascending list of all documents in the index.
func (i *Index) List() bsonkit.List {
	return i.base.List()
}

// Config will return the index configuration.
func (i *Index) Config() IndexConfig {
	return IndexConfig{
		Key:     bsonkit.Clone(i.config.Key),
		Unique:  i.config.Unique,
		Partial: bsonkit.Clone(i.config.Partial),
		Expiry:  i.config.Expiry,
	}
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
