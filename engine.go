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
	mutex   sync.Mutex
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

// Transaction will create a new transaction from the current catalog.
func (e *Engine) Transaction() *Transaction {
	// acquire lock
	e.mutex.Lock()
	defer e.mutex.Unlock()

	return NewTransaction(e.catalog)
}

// Commit will attempt to store the modified catalog and on success replace the
// current catalog.
func (e *Engine) Commit(txn *Transaction) error {
	// acquire lock
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check if closed
	if e.closed {
		return ErrEngineClosed
	}

	// check if dirty
	if !txn.Dirty() {
		return nil
	}

	// write catalog
	err := e.store.Store(txn.Catalog())
	if err != nil {
		return err
	}

	// set new catalog
	e.catalog = txn.Catalog()

	// broadcast change
	for stream := range e.streams {
		select {
		case stream.signal <- struct{}{}:
		default:
			// stream already got earlier signal
		}
	}

	return nil
}

// Watch will return a stream that is able to consume events from the oplog.
func (e *Engine) Watch(handle Handle, filter bsonkit.List, resumeAfter, startAfter bsonkit.Doc, startAt *primitive.Timestamp) (*Stream, error) {
	// acquire lock
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
	// acquire lock
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
