package lungo

import (
	"bytes"

	"github.com/tidwall/btree"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
	Name      string   `bson:"name"`
	Documents []bson.M `bson:"documents"`
	Indexes   []Index  `bson:"indexes"`

	primaryIndex *btree.BTree `bson:"-"`
}

func NewNamespace(name string) *Namespace {
	return (&Namespace{Name: name}).Prepare()
}

func (n *Namespace) Prepare() *Namespace {
	// create primary index
	n.primaryIndex = btree.New(64, "_id")

	// index all documents
	for _, doc := range n.Documents {
		n.primaryIndex.ReplaceOrInsert(&primaryIndexItem{doc: doc})
	}

	return n
}

func (n *Namespace) Clone() *Namespace {
	// create new namespace
	data := &Namespace{
		Name: n.Name,
	}

	// copy documents
	copy(data.Documents, n.Documents)

	// copy indexes
	copy(data.Indexes, n.Indexes)

	// clone primary index
	data.primaryIndex = n.primaryIndex.Clone()

	return data
}

type Index struct {
	Name   string `bson:"name"`
	Keys   bson.D `bson:"keys"`
	Unique bool   `bson:"unique"`
}

type primaryIndexItem struct {
	doc bson.M
}

func (i *primaryIndexItem) Less(item btree.Item, _ interface{}) bool {
	// coerce item
	j := item.(*primaryIndexItem)

	// get ids
	id1 := i.doc["_id"].(primitive.ObjectID)
	id2 := j.doc["_id"].(primitive.ObjectID)

	// compare ids
	ret := bytes.Compare(id1[:], id2[:]) < 0

	return ret
}
