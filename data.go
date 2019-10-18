package lungo

import (
	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

type Data struct {
	Namespaces map[string]*Namespace `bson:"namespaces"`
}

func NewData() *Data {
	return (&Data{}).Prepare()
}

func (d *Data) Prepare() *Data {
	// ensure namespaces
	if d.Namespaces == nil {
		d.Namespaces = make(map[string]*Namespace)
	}

	// init namespaces
	for _, namespace := range d.Namespaces {
		namespace.Prepare()
	}

	return d
}

func (d *Data) Clone() *Data {
	// create new data
	data := &Data{
		Namespaces: map[string]*Namespace{},
	}

	// copy namespaces
	for name, namespace := range d.Namespaces {
		data.Namespaces[name] = namespace
	}

	return data
}

type Namespace struct {
	Name      string       `bson:"name"`
	Documents bsonkit.List `bson:"documents"`
	Indexes   []Index      `bson:"indexes"`

	primaryIndex *uniqueIndex `bson:"-"`
}

func NewNamespace(name string) *Namespace {
	return (&Namespace{Name: name}).Prepare()
}

func (n *Namespace) Prepare() *Namespace {
	// create and fill primary index
	n.primaryIndex = newUniqueIndex("_id")
	n.primaryIndex.Fill(n.Documents)

	return n
}

func (n *Namespace) Clone() *Namespace {
	// create new namespace
	clone := &Namespace{
		Name: n.Name,
	}

	// copy documents
	clone.Documents = make(bsonkit.List, len(n.Documents))
	copy(clone.Documents, n.Documents)

	// copy indexes
	clone.Indexes = make([]Index, len(n.Indexes))
	copy(clone.Indexes, n.Indexes)

	// clone primary index
	clone.primaryIndex = n.primaryIndex.Clone()

	return clone
}

type Index struct {
	Name   string `bson:"name"`
	Keys   bson.D `bson:"keys"`
	Unique bool   `bson:"unique"`
}
