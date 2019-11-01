package lungo

import (
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
	"github.com/256dpi/lungo/mongokit"
)

// Opcode defines the type of an operation.
type Opcode int

// The available opcodes.
const (
	Insert Opcode = iota
	Replace
	Update
	Delete
)

// Strings returns the opcode name.
func (c Opcode) String() string {
	switch c {
	case Insert:
		return "insert"
	case Replace:
		return "replace"
	case Update:
		return "update"
	case Delete:
		return "delete"
	default:
		return ""
	}
}

// Operation defines a single operation.
type Operation struct {
	// The opcode.
	Opcode Opcode

	// The filter document (replace, update, delete).
	Filter bsonkit.Doc

	// The insert, update or replacement document.
	Document bsonkit.Doc

	// Whether an upsert should be performed (replace, update).
	Upsert bool

	// The limit (one, many).
	Limit int
}

// Result describes the outcome of an operation.
type Result struct {
	// The list of matched documents.
	Matched bsonkit.List

	// The list of inserted, replace or updated documents.
	Modified bsonkit.List

	// The upserted document.
	Upserted bsonkit.Doc

	// The error that occurred during the operation.
	Error error
}

// Transaction buffers multiple changes to a catalog.
type Transaction struct {
	catalog *Catalog
	dirty   bool
	mutex   sync.RWMutex
}

// NewTransaction creates and returns a new transaction.
func NewTransaction(catalog *Catalog) *Transaction {
	return &Transaction{
		catalog: catalog,
	}
}

// Find will query documents from a namespace. See Engine.Find for more details.
func (t *Transaction) Find(handle Handle, query, sort bsonkit.Doc, skip, limit int) (*Result, error) {
	// acquire read lock
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	// validate handle
	err := handle.Validate(true)
	if err != nil {
		return nil, err
	}

	// check namespace
	if t.catalog.Namespaces[handle] == nil {
		return &Result{}, nil
	}

	// find documents
	res, err := t.catalog.Namespaces[handle].Find(query, sort, skip, limit)
	if err != nil {
		return nil, err
	}

	return &Result{
		Matched: res.Matched,
	}, nil
}

// Bulk performs the specified operations in one go. See Engine.Bulk for more
// details.
func (t *Transaction) Bulk(handle Handle, ops []Operation, ordered bool) ([]Result, error) {
	// acquire write lock
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// validate handle
	err := handle.Validate(true)
	if err != nil {
		return nil, err
	}

	// clone catalog
	clone := t.catalog.Clone()

	// create or clone namespace
	var namespace *mongokit.Collection
	if clone.Namespaces[handle] == nil {
		namespace = mongokit.NewCollection(true)
		clone.Namespaces[handle] = namespace
	} else {
		namespace = clone.Namespaces[handle].Clone()
		clone.Namespaces[handle] = namespace
	}

	// clone oplog
	oplog := clone.Namespaces[Oplog].Clone()
	clone.Namespaces[Oplog] = oplog

	// collect changes
	changes := 0

	// prepare results
	results := make([]Result, 0, len(ops))

	// process models
	for _, op := range ops {
		// prepare variables
		var res *Result
		var err error

		// run operation
		switch op.Opcode {
		case Insert:
			res, err = t.insert(handle, oplog, namespace, op.Document)
		case Replace:
			res, err = t.replace(handle, oplog, namespace, op.Filter, op.Document, nil, op.Upsert)
		case Update:
			res, err = t.update(handle, oplog, namespace, op.Filter, op.Document, nil, op.Upsert, op.Limit)
		case Delete:
			res, err = t.delete(handle, oplog, namespace, op.Filter, nil, op.Limit)
		default:
			return nil, fmt.Errorf("unsupported bulk opcode %q", op.Opcode.String())
		}

		// check error
		if err != nil {
			// append error
			results = append(results, Result{
				Error: err,
			})

			// stop if ordered
			if ordered {
				break
			}
		} else {
			// append result
			results = append(results, *res)

			// update changes
			changes += len(res.Modified)
			if res.Upserted != nil {
				changes++
			} else if op.Opcode == Delete {
				changes += len(res.Matched)
			}
		}
	}

	// set catalog and flag
	if changes > 0 {
		t.catalog = clone
		t.dirty = true
	}

	return results, nil
}

// Insert will insert the specified documents. See Engine.Insert for more details.
func (t *Transaction) Insert(handle Handle, list bsonkit.List, ordered bool) (*Result, error) {
	// acquire write lock
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// validate handle
	err := handle.Validate(true)
	if err != nil {
		return nil, err
	}

	// clone list
	list = bsonkit.CloneList(list)

	// clone catalog
	clone := t.catalog.Clone()

	// create or clone namespace
	var namespace *mongokit.Collection
	if clone.Namespaces[handle] == nil {
		namespace = mongokit.NewCollection(true)
		clone.Namespaces[handle] = namespace
	} else {
		namespace = clone.Namespaces[handle].Clone()
		clone.Namespaces[handle] = namespace
	}

	// clone oplog
	oplog := clone.Namespaces[Oplog].Clone()
	clone.Namespaces[Oplog] = oplog

	// prepare result
	result := &Result{}

	// insert documents
	for _, doc := range list {
		// perform insert
		res, err := t.insert(handle, oplog, namespace, doc)
		if err != nil {
			// set error
			if result.Error == nil {
				result.Error = err
			}

			// stop if ordered
			if ordered {
				break
			}
		} else {
			// merge result
			result.Modified = append(result.Modified, res.Modified...)
		}
	}

	// set catalog and flag
	if len(result.Modified) > 0 {
		t.catalog = clone
		t.dirty = true
	}

	return result, nil
}

func (t *Transaction) insert(handle Handle, oplog, namespace *mongokit.Collection, doc bsonkit.Doc) (*Result, error) {
	// insert document
	res, err := namespace.Insert(doc)
	if err != nil {
		return nil, err
	}

	// append oplog
	t.append(oplog, handle, "insert", doc, nil)

	return &Result{
		Modified: res.Modified,
	}, nil
}

// Replace will replace the first matching document with the specified
// replacement document. See Engine.Replace for more details.
func (t *Transaction) Replace(handle Handle, query, sort, repl bsonkit.Doc, upsert bool) (*Result, error) {
	// acquire write lock
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// validate handle
	err := handle.Validate(true)
	if err != nil {
		return nil, err
	}

	// check namespace
	if t.catalog.Namespaces[handle] == nil && !upsert {
		return &Result{}, nil
	}

	// clone replacement
	repl = bsonkit.Clone(repl)

	// clone catalog
	clone := t.catalog.Clone()

	// create or clone namespace
	var namespace *mongokit.Collection
	if clone.Namespaces[handle] == nil {
		namespace = mongokit.NewCollection(true)
		clone.Namespaces[handle] = namespace
	} else {
		namespace = clone.Namespaces[handle].Clone()
		clone.Namespaces[handle] = namespace
	}

	// clone oplog
	oplog := clone.Namespaces[Oplog].Clone()
	clone.Namespaces[Oplog] = oplog

	// perform replace
	res, err := t.replace(handle, oplog, namespace, query, repl, sort, upsert)
	if err != nil {
		return nil, err
	}

	// set catalog and flag
	if len(res.Modified) > 0 || res.Upserted != nil {
		t.catalog = clone
		t.dirty = true
	}

	return res, nil
}

func (t *Transaction) replace(handle Handle, oplog, namespace *mongokit.Collection, query, repl, sort bsonkit.Doc, upsert bool) (*Result, error) {
	// replace document
	res, err := namespace.Replace(query, repl, sort)
	if err != nil {
		return nil, err
	}

	// perform upsert
	if len(res.Modified) == 0 && upsert {
		res, err = namespace.Upsert(query, repl, nil)
		if err != nil {
			return nil, err
		}

		// append oplog
		t.append(oplog, handle, "insert", res.Upserted, nil)

		return &Result{
			Upserted: res.Upserted,
		}, nil
	}

	// append oplog
	if len(res.Modified) > 0 {
		t.append(oplog, handle, "replace", res.Modified[0], nil)
	}

	return &Result{
		Matched:  res.Matched,
		Modified: res.Modified,
	}, nil
}

// Update will apply the update to all matching documents. See Engine.Update for
// more details.
func (t *Transaction) Update(handle Handle, query, sort, update bsonkit.Doc, limit int, upsert bool) (*Result, error) {
	// acquire write lock
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// validate handle
	err := handle.Validate(true)
	if err != nil {
		return nil, err
	}

	// check namespace
	if t.catalog.Namespaces[handle] == nil && !upsert {
		return &Result{}, nil
	}

	// clone catalog
	clone := t.catalog.Clone()

	// create or clone namespace
	var namespace *mongokit.Collection
	if clone.Namespaces[handle] == nil {
		namespace = mongokit.NewCollection(true)
		clone.Namespaces[handle] = namespace
	} else {
		namespace = clone.Namespaces[handle].Clone()
		clone.Namespaces[handle] = namespace
	}

	// clone oplog
	oplog := clone.Namespaces[Oplog].Clone()
	clone.Namespaces[Oplog] = oplog

	// perform update
	res, err := t.update(handle, oplog, namespace, query, update, sort, upsert, limit)
	if err != nil {
		return nil, err
	}

	// set catalog and flag
	if len(res.Modified) > 0 || res.Upserted != nil {
		t.catalog = clone
		t.dirty = true
	}

	return res, nil
}

func (t *Transaction) update(handle Handle, oplog, namespace *mongokit.Collection, query, update, sort bsonkit.Doc, upsert bool, limit int) (*Result, error) {
	// perform update
	res, err := namespace.Update(query, update, sort, limit)
	if err != nil {
		return nil, err
	}

	// perform upsert
	if len(res.Modified) == 0 && upsert {
		res, err = namespace.Upsert(query, nil, update)
		if err != nil {
			return nil, err
		}

		// append oplog
		t.append(oplog, handle, "insert", res.Upserted, nil)

		return &Result{
			Upserted: res.Upserted,
		}, nil
	}

	// append oplog
	for i, doc := range res.Modified {
		t.append(oplog, handle, "update", doc, res.Changes[i])
	}

	return &Result{
		Matched:  res.Matched,
		Modified: res.Modified,
	}, nil
}

// Delete will remove all matching documents. See Engine.Delete for more details.
func (t *Transaction) Delete(handle Handle, query, sort bsonkit.Doc, limit int) (*Result, error) {
	// acquire write lock
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// validate handle
	err := handle.Validate(true)
	if err != nil {
		return nil, err
	}

	// check namespace
	if t.catalog.Namespaces[handle] == nil {
		return &Result{}, nil
	}

	// clone catalog
	clone := t.catalog.Clone()

	// clone namespace
	namespace := clone.Namespaces[handle].Clone()
	clone.Namespaces[handle] = namespace

	// clone oplog
	oplog := clone.Namespaces[Oplog].Clone()
	clone.Namespaces[Oplog] = oplog

	// perform delete
	res, err := t.delete(handle, oplog, namespace, query, sort, limit)
	if err != nil {
		return nil, err
	}

	// set catalog and flag
	if len(res.Matched) > 0 {
		t.catalog = clone
		t.dirty = true
	}

	return res, nil
}

func (t *Transaction) delete(handle Handle, oplog, namespace *mongokit.Collection, query, sort bsonkit.Doc, limit int) (*Result, error) {
	// perform delete
	res, err := namespace.Delete(query, sort, limit)
	if err != nil {
		return nil, err
	}

	// append oplog
	for _, doc := range res.Matched {
		t.append(oplog, handle, "delete", doc, nil)
	}

	return &Result{
		Matched: res.Matched,
	}, nil
}

// Drop will drop namespaces. See Engine.Drop for more details.
func (t *Transaction) Drop(handle Handle) error {
	// acquire write lock
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// validate handle
	err := handle.Validate(false)
	if err != nil {
		return err
	}

	// clone catalog
	clone := t.catalog.Clone()

	// clone oplog
	oplog := clone.Namespaces[Oplog].Clone()
	clone.Namespaces[Oplog] = oplog

	// collect dropped
	dropped := 0

	// drop all matching namespaces
	for ns := range clone.Namespaces {
		if ns == handle || handle[1] == "" && ns[0] == handle[0] {
			// delete namespace
			delete(clone.Namespaces, ns)
			dropped++

			// append oplog
			t.append(oplog, ns, "drop", nil, nil)
		}
	}

	// append oplog if database has been dropped
	if handle[1] == "" && dropped > 0 {
		t.append(oplog, handle, "dropDatabase", nil, nil)
	}

	// set catalog and flag
	if dropped > 0 {
		t.catalog = clone
		t.dirty = true
	}

	return nil
}

func (t *Transaction) append(oplog *mongokit.Collection, handle Handle, op string, doc bsonkit.Doc, changes *mongokit.Changes) {
	// get time
	now := bsonkit.Now()

	// prepare ns
	ns := bson.M{"db": handle[0]}
	if handle[1] != "" {
		ns["coll"] = handle[1]
	}

	// prepare event
	event := bson.M{
		"ns": ns,
		"_id": bson.M{
			"ts": now,
		},
		"clusterTime":   now,
		"operationType": op,
	}

	// add document info
	if doc != nil {
		// add document key
		event["documentKey"] = bson.M{
			"_id": bsonkit.Get(doc, "_id"),
		}

		// add full document
		if op == "insert" || op == "replace" || op == "update" {
			event["fullDocument"] = *doc
		}
	}

	// add changes
	if changes != nil {
		// collect remove fields
		removed := make([]string, 0, len(changes.Removed))
		for field := range changes.Removed {
			removed = append(removed, field)
		}

		// add update description
		event["updateDescription"] = bson.M{
			"updatedFields": changes.Updated,
			"removedFields": removed,
		}
	}

	// add event
	oplog.Documents.Add(bsonkit.Convert(event))

	// resize oplog
	for len(oplog.Documents.List) > 1000 {
		oplog.Documents.Remove(oplog.Documents.List[0])
	}
}

// ListDatabases will return a list of all databases. See Engine.ListDatabases
// for more details.
func (t *Transaction) ListDatabases(query bsonkit.Doc) (bsonkit.List, error) {
	// acquire read lock
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	// sort namespaces
	sort := map[string][]*mongokit.Collection{}
	for ns, namespace := range t.catalog.Namespaces {
		sort[ns[0]] = append(sort[ns[0]], namespace)
	}

	// prepare list
	var list bsonkit.List
	for name, nss := range sort {
		// check emptiness
		empty := true
		for _, ns := range nss {
			if len(ns.Documents.List) > 0 {
				empty = false
			}
		}

		// add specification
		list = append(list, &bson.D{
			bson.E{Key: "name", Value: name},
			bson.E{Key: "sizeOnDisk", Value: 0},
			bson.E{Key: "empty", Value: empty},
		})
	}

	// filter list
	list, err := mongokit.Filter(list, query, 0)
	if err != nil {
		return nil, err
	}

	return list, nil
}

// ListCollections will return a list of all collections See Engine.ListCollections
// for more details..
func (t *Transaction) ListCollections(handle Handle, query bsonkit.Doc) (bsonkit.List, error) {
	// acquire read lock
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	// validate handle
	err := handle.Validate(false)
	if err != nil {
		return nil, err
	}

	// prepare list
	list := make(bsonkit.List, 0, len(t.catalog.Namespaces))

	// add documents
	for ns := range t.catalog.Namespaces {
		if ns[0] == handle[0] {
			list = append(list, &bson.D{
				bson.E{Key: "name", Value: ns[1]},
				bson.E{Key: "type", Value: "collection"},
				bson.E{Key: "options", Value: bson.D{}},
				bson.E{Key: "info", Value: bson.D{
					bson.E{Key: "uuid", Value: ns.String()},
					bson.E{Key: "readOnly", Value: false},
				}},
				bson.E{Key: "idIndex", Value: bson.D{
					bson.E{Key: "v", Value: 2},
					bson.E{Key: "key", Value: bson.D{
						bson.E{Key: "_id", Value: 1},
					}},
					bson.E{Key: "name", Value: "_id_"},
					bson.E{Key: "namespace", Value: ns.String()},
				}},
			})
		}
	}

	// filter list
	list, err = mongokit.Filter(list, query, 0)
	if err != nil {
		return nil, err
	}

	return list, nil
}

// CountDocuments will return the number of documents. See Engine.CountDocuments
// for more details.
func (t *Transaction) CountDocuments(handle Handle) (int, error) {
	// acquire read lock
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	// validate handle
	err := handle.Validate(true)
	if err != nil {
		return 0, err
	}

	// check namespace
	namespace, ok := t.catalog.Namespaces[handle]
	if !ok {
		return 0, nil
	}

	return len(namespace.Documents.List), nil
}

// ListIndexes will return a list of indexes. See Engine.ListIndexes for more
// details.
func (t *Transaction) ListIndexes(handle Handle) (bsonkit.List, error) {
	// acquire read lock
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	// validate handle
	err := handle.Validate(true)
	if err != nil {
		return nil, err
	}

	// check namespace
	if t.catalog.Namespaces[handle] == nil {
		return nil, fmt.Errorf("missing namespace %q", handle.String())
	}

	// get namespace
	namespace := t.catalog.Namespaces[handle]

	// prepare list
	var list bsonkit.List
	for name, index := range namespace.Indexes {
		// get config
		config := index.Config()

		// create spec
		spec := bson.D{
			bson.E{Key: "v", Value: 2},
			bson.E{Key: "key", Value: *config.Key},
			bson.E{Key: "name", Value: name},
			bson.E{Key: "ns", Value: handle.String()},
		}

		// add unique
		if config.Unique && name != "_id_" {
			spec = append(spec, bson.E{Key: "unique", Value: true})
		}

		// add partial
		if config.Partial != nil {
			spec = append(spec, bson.E{Key: "partialFilterExpression", Value: *config.Partial})
		}

		// add specification
		list = append(list, &spec)
	}

	// sort list
	bsonkit.Sort(list, []bsonkit.Column{
		{Path: "name"},
	})

	return list, nil
}

// CreateIndex will create the specified index. See Engine.CreateIndex for more
// details.
func (t *Transaction) CreateIndex(handle Handle, key bsonkit.Doc, name string, unique bool, partial bsonkit.Doc) (string, error) {
	// acquire write lock
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// validate handle
	err := handle.Validate(true)
	if err != nil {
		return "", err
	}

	// clone catalog
	clone := t.catalog.Clone()

	// create or clone namespace
	var namespace *mongokit.Collection
	if clone.Namespaces[handle] == nil {
		namespace = mongokit.NewCollection(true)
		clone.Namespaces[handle] = namespace
	} else {
		namespace = clone.Namespaces[handle].Clone()
		clone.Namespaces[handle] = namespace
	}

	// create index
	name, err = namespace.CreateIndex(name, mongokit.IndexConfig{
		Key:     key,
		Unique:  unique,
		Partial: partial,
	})
	if err != nil {
		return "", err
	}

	// set catalog and flag
	t.catalog = clone
	t.dirty = true

	return name, nil
}

// DropIndex will drop the specified index. See Engine.DropIndex for more details.
func (t *Transaction) DropIndex(handle Handle, name string) error {
	// acquire write lock
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// validate handle
	err := handle.Validate(true)
	if err != nil {
		return err
	}

	// check namespace
	if t.catalog.Namespaces[handle] == nil {
		return fmt.Errorf("missing namespace %q", handle.String())
	}

	// clone catalog
	clone := t.catalog.Clone()

	// clone namespace
	namespace := clone.Namespaces[handle].Clone()
	clone.Namespaces[handle] = namespace

	// drop index
	dropped, err := namespace.DropIndex(name)
	if err != nil {
		return err
	}

	// set catalog and flag
	if len(dropped) > 0 {
		t.catalog = clone
		t.dirty = true
	}

	return nil
}

// Dirty will return whether the transaction contains changes.
func (t *Transaction) Dirty() bool {
	// acquire read lock
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return t.dirty
}

// Catalog will return the modified catalog by the transaction.
func (t *Transaction) Catalog() *Catalog {
	// acquire read lock
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return t.catalog
}
