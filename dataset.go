package lungo

import (
	"strings"

	"github.com/256dpi/lungo/bsonkit"
)

// NS is a namespace identifier
type NS [2]string

// String will return the string form of the ns.
func (ns NS) String() string {
	return strings.Join(ns[:], ".")
}

// Dataset is the top level object per database that contains all data.
type Dataset struct {
	Namespaces map[NS]*Namespace `bson:"namespaces"`
}

// NewDataset creates and returns a new dataset.
func NewDataset() *Dataset {
	return (&Dataset{}).Prepare()
}

// Prepare will prepare the dataset.
func (d *Dataset) Prepare() *Dataset {
	// ensure namespaces
	if d.Namespaces == nil {
		d.Namespaces = make(map[NS]*Namespace)
	}

	// init namespaces
	for _, namespace := range d.Namespaces {
		namespace.Prepare()
	}

	return d
}

// Clone will cone the dataset. Namespaces need to be cloned separately.
func (d *Dataset) Clone() *Dataset {
	// create clone
	clone := &Dataset{
		Namespaces: map[NS]*Namespace{},
	}

	// copy namespaces
	for name, namespace := range d.Namespaces {
		clone.Namespaces[name] = namespace
	}

	return clone
}

// Namespace holds documents and indexes.
type Namespace struct {
	// The document set.
	Documents *bsonkit.Set `bson:"documents"`

	// The indexes.
	Indexes map[string]*bsonkit.Index `bson:"indexes"`
}

// NewNamespace creates and returns a new namespace.
func NewNamespace() *Namespace {
	return (&Namespace{
		Documents: bsonkit.NewSet(nil),
		Indexes: map[string]*bsonkit.Index{
			"_id_": bsonkit.NewIndex(true, []bsonkit.Column{
				{Path: "_id"},
			}),
		},
	}).Prepare()
}

// Prepare will prepare the namespace.
func (n *Namespace) Prepare() *Namespace {
	// prepare indexes
	for _, index := range n.Indexes {
		index.Prepare(n.Documents.List)
	}

	return n
}

// Clone will clone the namespace.
func (n *Namespace) Clone() *Namespace {
	// create new namespace
	clone := &Namespace{
		Documents: n.Documents.Clone(),
		Indexes:   map[string]*bsonkit.Index{},
	}

	// clone indexes
	for name, index := range n.Indexes {
		clone.Indexes[name] = index.Clone()
	}

	return clone
}
