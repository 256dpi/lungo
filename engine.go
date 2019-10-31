package lungo

import (
	"errors"
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/256dpi/lungo/bsonkit"
	"github.com/256dpi/lungo/mongokit"
)

// ErrInvalidHandle is returned if a specified handle is invalid.
var ErrInvalidHandle = errors.New("invalid handle")

// ErrEngineClosed is returned if the engine has been closed.
var ErrEngineClosed = errors.New("engine closed")

// Result is returned by some engine operations.
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

// Options is used to configure an engine.
type Options struct {
	// The store used by the engine to load and store the dataset.
	Store Store
}

// Engine manages the dataset loaded from a store and provides the various
// MongoDB style CRUD operations.
type Engine struct {
	store   Store
	dataset *Dataset
	streams map[*Stream]struct{}
	closed  bool
	mutex   sync.RWMutex
}

// CreateEngine will create and return an engine with a loaded dataset from the
// store.
func CreateEngine(opts Options) (*Engine, error) {
	// create engine
	e := &Engine{
		store:   opts.Store,
		streams: map[*Stream]struct{}{},
	}

	// load dataset
	data, err := e.store.Load()
	if err != nil {
		return nil, err
	}

	// set dataset
	e.dataset = data

	return e, nil
}

// Find will query documents from a namespace. Sort, skip and limit may be
// supplied to modify the result. The returned results will contain the matched
// list of documents.
func (e *Engine) Find(handle Handle, query, sort bsonkit.Doc, skip, limit int) (*Result, error) {
	// acquire read lock
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	// check if closed
	if e.closed {
		return nil, ErrEngineClosed
	}

	// check handle
	if handle[0] == "" || handle[1] == "" {
		return nil, ErrInvalidHandle
	}

	// check namespace
	if e.dataset.Namespaces[handle] == nil {
		return &Result{}, nil
	}

	// find documents
	res, err := e.dataset.Namespaces[handle].Find(query, sort, skip, limit)
	if err != nil {
		return nil, err
	}

	return &Result{
		Matched: res.Matched,
	}, nil
}

// Bulk performs the specified operations in one go. If ordered is true the
// process is aborted on the first error.
func (e *Engine) Bulk(handle Handle, ops []Operation, ordered bool) ([]Result, error) {
	// acquire write lock
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check if closed
	if e.closed {
		return nil, ErrEngineClosed
	}

	// check handle
	if handle[0] == "" || handle[1] == "" {
		return nil, ErrInvalidHandle
	}

	// clone dataset
	clone := e.dataset.Clone()

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
			res, err = e.insert(handle, oplog, namespace, op.Document)
		case Replace:
			res, err = e.replace(handle, oplog, namespace, op.Filter, op.Document, nil, op.Upsert)
		case Update:
			res, err = e.update(handle, oplog, namespace, op.Filter, op.Document, nil, op.Upsert, op.Limit)
		case Delete:
			res, err = e.delete(handle, oplog, namespace, op.Filter, nil, op.Limit)
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

	// check if changed
	if changes > 0 {
		// write dataset
		err := e.store.Store(clone)
		if err != nil {
			return nil, err
		}

		// set new dataset
		e.dataset = clone

		// broadcast change
		e.broadcast()
	}

	return results, nil
}

// Insert will insert the specified documents into the namespace. The engine
// will automatically generate an object id per document if it is missing. If
// ordered ist enabled the operation is aborted on the first error and the
// result returned. Otherwise, the engine will try to insert all documents. The
// returned results will contain the inserted documents and potential errors.
func (e *Engine) Insert(handle Handle, list bsonkit.List, ordered bool) (*Result, error) {
	// acquire write lock
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check if closed
	if e.closed {
		return nil, ErrEngineClosed
	}

	// check handle
	if handle[0] == "" || handle[1] == "" {
		return nil, ErrInvalidHandle
	}

	// clone list
	list = bsonkit.CloneList(list)

	// clone dataset
	clone := e.dataset.Clone()

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
		res, err := e.insert(handle, oplog, namespace, doc)
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

	// check if documents have been inserted
	if len(result.Modified) > 0 {
		// write dataset
		err := e.store.Store(clone)
		if err != nil {
			return nil, err
		}

		// set new dataset
		e.dataset = clone

		// broadcast change
		e.broadcast()
	}

	return result, nil
}

func (e *Engine) insert(handle Handle, oplog, namespace *mongokit.Collection, doc bsonkit.Doc) (*Result, error) {
	// insert document
	res, err := namespace.Insert(doc)
	if err != nil {
		return nil, err
	}

	// append oplog
	e.append(oplog, handle, "insert", doc, nil)

	return &Result{
		Modified: res.Modified,
	}, nil
}

// Replace will replace the first matching document with the specified
// replacement document. If upsert is enabled, it will insert the replacement
// document if it is missing. The returned result will contain the matched
// and modified or upserted document.
func (e *Engine) Replace(handle Handle, query, sort, repl bsonkit.Doc, upsert bool) (*Result, error) {
	// acquire write lock
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check if closed
	if e.closed {
		return nil, ErrEngineClosed
	}

	// check handle
	if handle[0] == "" || handle[1] == "" {
		return nil, ErrInvalidHandle
	}

	// check namespace
	if e.dataset.Namespaces[handle] == nil && !upsert {
		return &Result{}, nil
	}

	// clone replacement
	repl = bsonkit.Clone(repl)

	// clone dataset
	clone := e.dataset.Clone()

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
	res, err := e.replace(handle, oplog, namespace, query, repl, sort, upsert)
	if err != nil {
		return nil, err
	}

	// check if modified
	if len(res.Modified) > 0 || res.Upserted != nil {
		// write dataset
		err = e.store.Store(clone)
		if err != nil {
			return nil, err
		}

		// set new dataset
		e.dataset = clone

		// broadcast change
		e.broadcast()
	}

	return res, nil
}

func (e *Engine) replace(handle Handle, oplog, namespace *mongokit.Collection, query, repl, sort bsonkit.Doc, upsert bool) (*Result, error) {
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
		e.append(oplog, handle, "insert", res.Upserted, nil)

		return &Result{
			Upserted: res.Upserted,
		}, nil
	}

	// append oplog
	if len(res.Modified) > 0 {
		e.append(oplog, handle, "replace", res.Modified[0], nil)
	}

	return &Result{
		Matched:  res.Matched,
		Modified: res.Modified,
	}, nil
}

// Update will apply the update to all matching document. Sort, skip and limit
// may be supplied to modify the result. If upsert is enabled, it will extract
// constant parts of the query and apply the update and insert the document if
// it is missing. The returned result will contain the matched and modified or
// upserted document.
func (e *Engine) Update(handle Handle, query, sort, update bsonkit.Doc, limit int, upsert bool) (*Result, error) {
	// acquire write lock
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check if closed
	if e.closed {
		return nil, ErrEngineClosed
	}

	// check handle
	if handle[0] == "" || handle[1] == "" {
		return nil, ErrInvalidHandle
	}

	// check namespace
	if e.dataset.Namespaces[handle] == nil && !upsert {
		return &Result{}, nil
	}

	// clone dataset
	clone := e.dataset.Clone()

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
	res, err := e.update(handle, oplog, namespace, query, update, sort, upsert, limit)
	if err != nil {
		return nil, err
	}

	// check if modified
	if len(res.Modified) > 0 || res.Upserted != nil {
		// write dataset
		err = e.store.Store(clone)
		if err != nil {
			return nil, err
		}

		// set new dataset
		e.dataset = clone

		// broadcast change
		e.broadcast()
	}

	return res, nil
}

func (e *Engine) update(handle Handle, oplog, namespace *mongokit.Collection, query, update, sort bsonkit.Doc, upsert bool, limit int) (*Result, error) {
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
		e.append(oplog, handle, "insert", res.Upserted, nil)

		return &Result{
			Upserted: res.Upserted,
		}, nil
	}

	// append oplog
	for i, doc := range res.Modified {
		e.append(oplog, handle, "update", doc, res.Changes[i])
	}

	return &Result{
		Matched:  res.Matched,
		Modified: res.Modified,
	}, nil
}

// Delete will remove all matching documents from the namespace. Sort, skip and
// limit may be supplied to modify the result. The returned result will contain
// the matched documents.
func (e *Engine) Delete(handle Handle, query, sort bsonkit.Doc, limit int) (*Result, error) {
	// acquire write lock
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check if closed
	if e.closed {
		return nil, ErrEngineClosed
	}

	// check handle
	if handle[0] == "" || handle[1] == "" {
		return nil, ErrInvalidHandle
	}

	// check namespace
	if e.dataset.Namespaces[handle] == nil {
		return &Result{}, nil
	}

	// clone dataset
	clone := e.dataset.Clone()

	// clone namespace
	namespace := clone.Namespaces[handle].Clone()
	clone.Namespaces[handle] = namespace

	// clone oplog
	oplog := clone.Namespaces[Oplog].Clone()
	clone.Namespaces[Oplog] = oplog

	// perform delete
	res, err := e.delete(handle, oplog, namespace, query, sort, limit)
	if err != nil {
		return nil, err
	}

	// check if matched
	if len(res.Matched) > 0 {
		// write dataset
		err = e.store.Store(clone)
		if err != nil {
			return nil, err
		}

		// set new dataset
		e.dataset = clone

		// broadcast change
		e.broadcast()
	}

	return res, nil
}

func (e *Engine) delete(handle Handle, oplog, namespace *mongokit.Collection, query, sort bsonkit.Doc, limit int) (*Result, error) {
	// perform delete
	res, err := namespace.Delete(query, sort, limit)
	if err != nil {
		return nil, err
	}

	// append oplog
	for _, doc := range res.Matched {
		e.append(oplog, handle, "delete", doc, nil)
	}

	return &Result{
		Matched: res.Matched,
	}, nil
}

// Drop will return the namespace with the specified handle from the dataset.
// If the second part of the handle is empty, it will drop all namespaces
// matching the first part.
func (e *Engine) Drop(handle Handle) error {
	// acquire write lock
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check if closed
	if e.closed {
		return ErrEngineClosed
	}

	// check handle
	if handle[0] == "" {
		return ErrInvalidHandle
	}

	// clone dataset
	clone := e.dataset.Clone()

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
			e.append(oplog, ns, "drop", nil, nil)
		}
	}

	// append oplog if database has been dropped
	if handle[1] == "" && dropped > 0 {
		e.append(oplog, handle, "dropDatabase", nil, nil)
	}

	// check if dropped
	if dropped > 0 {
		// write dataset
		err := e.store.Store(clone)
		if err != nil {
			return err
		}

		// set new dataset
		e.dataset = clone

		// broadcast change
		e.broadcast()
	}

	return nil
}

func (e *Engine) append(oplog *mongokit.Collection, handle Handle, op string, doc bsonkit.Doc, changes *mongokit.Changes) {
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

// ListDatabases will return a list of all databases in the dataset.
func (e *Engine) ListDatabases(query bsonkit.Doc) (bsonkit.List, error) {
	// acquire read lock
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	// check if closed
	if e.closed {
		return nil, ErrEngineClosed
	}

	// sort namespaces
	sort := map[string][]*mongokit.Collection{}
	for ns, namespace := range e.dataset.Namespaces {
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

// ListCollections will return a list of all collections in the specified db.
func (e *Engine) ListCollections(db string, query bsonkit.Doc) (bsonkit.List, error) {
	// acquire read lock
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	// check if closed
	if e.closed {
		return nil, ErrEngineClosed
	}

	// prepare list
	list := make(bsonkit.List, 0, len(e.dataset.Namespaces))

	// add documents
	for ns := range e.dataset.Namespaces {
		if ns[0] == db {
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
	list, err := mongokit.Filter(list, query, 0)
	if err != nil {
		return nil, err
	}

	return list, nil
}

// NumDocuments will return the number of documents in the specified namespace.
func (e *Engine) NumDocuments(handle Handle) (int, error) {
	// acquire read lock
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	// check if closed
	if e.closed {
		return 0, ErrEngineClosed
	}

	// check handle
	if handle[0] == "" || handle[1] == "" {
		return 0, ErrInvalidHandle
	}

	// check namespace
	namespace, ok := e.dataset.Namespaces[handle]
	if !ok {
		return 0, nil
	}

	return len(namespace.Documents.List), nil
}

// ListIndexes will return a list of indexes in the specified namespace.
func (e *Engine) ListIndexes(handle Handle) (bsonkit.List, error) {
	// acquire read lock
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	// check if closed
	if e.closed {
		return nil, ErrEngineClosed
	}

	// check handle
	if handle[0] == "" || handle[1] == "" {
		return nil, ErrInvalidHandle
	}

	// check namespace
	if e.dataset.Namespaces[handle] == nil {
		return nil, fmt.Errorf("missing namespace %q", handle.String())
	}

	// get namespace
	namespace := e.dataset.Namespaces[handle]

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

// CreateIndex will create the specified index in the specified namespace.
func (e *Engine) CreateIndex(handle Handle, key bsonkit.Doc, name string, unique bool, partial bsonkit.Doc) (string, error) {
	// acquire write lock
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check if closed
	if e.closed {
		return "", ErrEngineClosed
	}

	// check handle
	if handle[0] == "" || handle[1] == "" {
		return "", ErrInvalidHandle
	}

	// clone dataset
	clone := e.dataset.Clone()

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
	name, err := namespace.CreateIndex(name, mongokit.IndexConfig{
		Key:     key,
		Unique:  unique,
		Partial: partial,
	})
	if err != nil {
		return "", err
	}

	// write dataset
	err = e.store.Store(clone)
	if err != nil {
		return "", err
	}

	// set new dataset
	e.dataset = clone

	return name, nil
}

// DropIndex will drop the specified index in the specified namespace.
func (e *Engine) DropIndex(handle Handle, name string) error {
	// acquire write lock
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check if closed
	if e.closed {
		return ErrEngineClosed
	}

	// check handle
	if handle[0] == "" || handle[1] == "" {
		return ErrInvalidHandle
	}

	// check namespace
	if e.dataset.Namespaces[handle] == nil {
		return fmt.Errorf("missing namespace %q", handle.String())
	}

	// clone dataset
	clone := e.dataset.Clone()

	// clone namespace
	namespace := clone.Namespaces[handle].Clone()
	clone.Namespaces[handle] = namespace

	// drop index
	dropped, err := namespace.DropIndex(name)
	if err != nil {
		return err
	}

	// check if dropped
	if len(dropped) > 0 {
		// write dataset
		err := e.store.Store(clone)
		if err != nil {
			return err
		}

		// set new dataset
		e.dataset = clone
	}

	return nil
}

func (e *Engine) broadcast() {
	for stream := range e.streams {
		select {
		case stream.signal <- struct{}{}:
		default:
			// stream already got earlier signal
		}
	}
}

// Watch will return a stream that is able to consume events from the oplog.
func (e *Engine) Watch(handle Handle, filter bsonkit.List, resumeAfter, startAfter bsonkit.Doc, startAt *primitive.Timestamp) (*Stream, error) {
	// acquire write lock
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check if closed
	if e.closed {
		return nil, ErrEngineClosed
	}

	// get oplog
	oplog := e.dataset.Namespaces[Oplog].Documents.List

	// get index
	index := len(oplog) - 1

	// resume after
	if resumeAfter != nil {
		resumed := false
		for i, event := range oplog {
			res := bsonkit.Compare(*resumeAfter, bsonkit.Get(event, "_id"))
			if res == 0 {
				index = i
				resumed = true
				break
			}
		}
		if !resumed {
			return nil, fmt.Errorf("unable to resume change stream")
		}
	}

	// start after
	if startAfter != nil {
		resumed := false
		for i, event := range oplog {
			res := bsonkit.Compare(*startAfter, bsonkit.Get(event, "_id"))
			if res == 0 {
				index = i
				resumed = true
				break
			}
		}
		if !resumed {
			return nil, fmt.Errorf("unable to resume change stream")
		}
	}

	// start at
	if startAt != nil {
		resumed := false
		for i, event := range oplog {
			res := bsonkit.Compare(*startAt, bsonkit.Get(event, "clusterTime"))
			if res == 0 {
				index = i - 1
				resumed = true
				break
			}
		}
		if !resumed {
			return nil, fmt.Errorf("unable to resume change stream")
		}
	}

	// create stream
	stream := &Stream{
		handle: handle,
		index:  index,
		filter: filter,
		signal: make(chan struct{}, 1),
	}

	// set oplog method
	stream.oplog = func() *bsonkit.Set {
		e.mutex.Lock()
		defer e.mutex.Unlock()
		return e.dataset.Namespaces[Oplog].Documents
	}

	// set cancel method
	stream.cancel = func() {
		e.mutex.Lock()
		defer e.mutex.Unlock()
		delete(e.streams, stream)
	}

	// register stream
	e.streams[stream] = struct{}{}

	return stream, nil
}

// Close will close the engine.
func (e *Engine) Close() {
	// acquire write lock
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check if closed
	if e.closed {
		return
	}

	// close streams
	for stream := range e.streams {
		close(stream.signal)
	}

	// set flag
	e.closed = true
}
