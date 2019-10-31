package lungo

import (
	"errors"
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/256dpi/lungo/bsonkit"
)

// ErrEngineClosed is returned if the engine has been closed.
var ErrEngineClosed = errors.New("engine closed")

// Options is used to configure an engine.
type Options struct {
	// The store used by the engine to load and store the catalog.
	Store Store
}

// Engine manages the catalog loaded from a store and provides the various
// MongoDB style CRUD operations.
type Engine struct {
	store   Store
	catalog *Catalog
	streams map[*Stream]struct{}
	closed  bool
	mutex   sync.RWMutex
}

// CreateEngine will create and return an engine with a loaded catalog from the
// store.
func CreateEngine(opts Options) (*Engine, error) {
	// create engine
	e := &Engine{
		store:   opts.Store,
		streams: map[*Stream]struct{}{},
	}

	// load catalog
	data, err := e.store.Load()
	if err != nil {
		return nil, err
	}

	// set catalog
	e.catalog = data

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

	// prepare transaction
	txn := NewTransaction(e.catalog)

	// perform find
	res, err := txn.Find(handle, query, sort, skip, limit)
	if err != nil {
		return nil, err
	}

	return res, nil
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

	// prepare transaction
	txn := NewTransaction(e.catalog)

	// perform bulk
	res, err := txn.Bulk(handle, ops, ordered)
	if err != nil {
		return nil, err
	}

	// check if dirty
	if txn.Dirty() {
		// write catalog
		err = e.store.Store(txn.Catalog())
		if err != nil {
			return nil, err
		}

		// set new catalog
		e.catalog = txn.Catalog()

		// broadcast change
		e.broadcast()
	}

	return res, nil
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

	// prepare transaction
	txn := NewTransaction(e.catalog)

	// perform insert
	res, err := txn.Insert(handle, list, ordered)
	if err != nil {
		return nil, err
	}

	// check if dirty
	if txn.Dirty() {
		// write catalog
		err = e.store.Store(txn.Catalog())
		if err != nil {
			return nil, err
		}

		// set new catalog
		e.catalog = txn.Catalog()

		// broadcast change
		e.broadcast()
	}

	return res, nil
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

	// prepare transaction
	txn := NewTransaction(e.catalog)

	// perform replace
	res, err := txn.Replace(handle, query, sort, repl, upsert)
	if err != nil {
		return nil, err
	}

	// check if dirty
	if txn.Dirty() {
		// write catalog
		err = e.store.Store(txn.Catalog())
		if err != nil {
			return nil, err
		}

		// set new catalog
		e.catalog = txn.Catalog()

		// broadcast change
		e.broadcast()
	}

	return res, nil
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

	// prepare transaction
	txn := NewTransaction(e.catalog)

	// perform update
	res, err := txn.Update(handle, query, sort, update, limit, upsert)
	if err != nil {
		return nil, err
	}

	// check if dirty
	if txn.Dirty() {
		// write catalog
		err = e.store.Store(txn.Catalog())
		if err != nil {
			return nil, err
		}

		// set new catalog
		e.catalog = txn.Catalog()

		// broadcast change
		e.broadcast()
	}

	return res, nil
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

	// prepare transaction
	txn := NewTransaction(e.catalog)

	// perform delete
	res, err := txn.Delete(handle, query, sort, limit)
	if err != nil {
		return nil, err
	}

	// check if dirty
	if txn.Dirty() {
		// write catalog
		err = e.store.Store(txn.Catalog())
		if err != nil {
			return nil, err
		}

		// set new catalog
		e.catalog = txn.Catalog()

		// broadcast change
		e.broadcast()
	}

	return res, nil
}

// Drop will return the namespace with the specified handle from the catalog.
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

	// prepare transaction
	txn := NewTransaction(e.catalog)

	// perform drop
	err := txn.Drop(handle)
	if err != nil {
		return err
	}

	// check if dirty
	if txn.Dirty() {
		// write catalog
		err = e.store.Store(txn.Catalog())
		if err != nil {
			return err
		}

		// set new catalog
		e.catalog = txn.Catalog()

		// broadcast change
		e.broadcast()
	}

	return nil
}

// ListDatabases will return a list of all databases in the catalog.
func (e *Engine) ListDatabases(query bsonkit.Doc) (bsonkit.List, error) {
	// acquire read lock
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	// check if closed
	if e.closed {
		return nil, ErrEngineClosed
	}

	// prepare transaction
	txn := NewTransaction(e.catalog)

	// list databases
	res, err := txn.ListDatabases(query)
	if err != nil {
		return nil, err
	}

	return res, nil
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

	// prepare transaction
	txn := NewTransaction(e.catalog)

	// list collections
	res, err := txn.ListCollections(db, query)
	if err != nil {
		return nil, err
	}

	return res, nil
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

	// prepare transaction
	txn := NewTransaction(e.catalog)

	// num documents
	res, err := txn.NumDocuments(handle)
	if err != nil {
		return 0, err
	}

	return res, nil
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

	// prepare transaction
	txn := NewTransaction(e.catalog)

	// list indexes
	res, err := txn.ListIndexes(handle)
	if err != nil {
		return nil, err
	}

	return res, nil
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

	// prepare transaction
	txn := NewTransaction(e.catalog)

	// create index
	res, err := txn.CreateIndex(handle, key, name, unique, partial)
	if err != nil {
		return "", err
	}

	// check if dirty
	if txn.Dirty() {
		// write catalog
		err = e.store.Store(txn.Catalog())
		if err != nil {
			return "", err
		}

		// set new catalog
		e.catalog = txn.Catalog()
	}

	return res, nil
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

	// prepare transaction
	txn := NewTransaction(e.catalog)

	// drop index
	err := txn.DropIndex(handle, name)
	if err != nil {
		return err
	}

	// check if dirty
	if txn.Dirty() {
		// write catalog
		err = e.store.Store(txn.Catalog())
		if err != nil {
			return err
		}

		// set new catalog
		e.catalog = txn.Catalog()
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
	oplog := e.catalog.Namespaces[Oplog].Documents

	// get last event
	var last bsonkit.Doc
	if len(oplog.List) > 0 {
		last = oplog.List[len(oplog.List)-1]
	}

	// resume after
	if resumeAfter != nil {
		resumed := false
		for _, event := range oplog.List {
			res := bsonkit.Compare(*resumeAfter, bsonkit.Get(event, "_id"))
			if res == 0 {
				last = event
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
		for _, event := range oplog.List {
			res := bsonkit.Compare(*startAfter, bsonkit.Get(event, "_id"))
			if res == 0 {
				last = event
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
		for i, event := range oplog.List {
			res := bsonkit.Compare(*startAt, bsonkit.Get(event, "clusterTime"))
			if res == 0 {
				if i > 0 {
					last = oplog.List[i-1]
					resumed = true
				}
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
		last:   last,
		filter: filter,
		signal: make(chan struct{}, 1),
	}

	// set oplog method
	stream.oplog = func() *bsonkit.Set {
		e.mutex.Lock()
		defer e.mutex.Unlock()
		return e.catalog.Namespaces[Oplog].Documents
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
