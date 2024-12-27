package lungo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"gopkg.in/tomb.v2"

	"github.com/256dpi/lungo/bsonkit"
	"github.com/256dpi/lungo/dbkit"
)

// ErrEngineClosed is returned if the engine has been closed.
var ErrEngineClosed = errors.New("engine closed")

// Options is used to configure an engine.
type Options struct {
	// The store used by the engine to load and store the catalog.
	Store Store

	// The interval at which expired documents are removed.
	//
	// Default: 60s.
	ExpireInterval time.Duration

	// The function that is called with errors from the expiry goroutine.
	ExpireErrors func(error)

	// The minimum and maximum size of the oplog.
	//
	// Default: 100, 1000.
	MinOplogSize int
	MaxOplogSize int

	// The minimum and maximum age of oplog entries.
	//
	// Default: 5m, 1h.
	MinOplogAge time.Duration
	MaxOplogAge time.Duration
}

// Engine manages the catalog loaded from a store and provides access to it
// through transactions. Additionally, it also manages streams that subscribe
// to catalog changes.
type Engine struct {
	opts    Options
	store   Store
	catalog *Catalog
	streams map[*Stream]struct{}
	token   *dbkit.Semaphore
	txn     *Transaction
	tomb    tomb.Tomb
	mutex   sync.Mutex
}

// CreateEngine will create and return an engine with a loaded catalog from the
// store.
func CreateEngine(opts Options) (*Engine, error) {
	// set default interval
	if opts.ExpireInterval == 0 {
		opts.ExpireInterval = 60 * time.Second
	}

	// set default min and max oplog size
	if opts.MinOplogSize == 0 {
		opts.MinOplogSize = 100
	}
	if opts.MaxOplogSize == 0 {
		opts.MaxOplogSize = 1000
	}

	// set default min and max oplog age
	if opts.MinOplogAge == 0 {
		opts.MinOplogAge = 5 * time.Minute
	}
	if opts.MaxOplogAge == 0 {
		opts.MaxOplogAge = time.Hour
	}

	// create engine
	e := &Engine{
		opts:    opts,
		store:   opts.Store,
		streams: map[*Stream]struct{}{},
		token:   dbkit.NewSemaphore(1),
	}

	// load catalog
	data, err := e.store.Load()
	if err != nil {
		return nil, err
	}

	// set catalog
	e.catalog = data

	// run expiry
	e.tomb.Go(func() error {
		e.expire(opts.ExpireInterval, opts.ExpireErrors)
		return nil
	})

	return e, nil
}

// Catalog will return the currently used catalog. Any modifications to the
// returned catalog while using the engine results in undefined behaviour.
func (e *Engine) Catalog() *Catalog {
	// acquire lock
	e.mutex.Lock()
	defer e.mutex.Unlock()

	return e.catalog
}

// Begin will create a new transaction from the current catalog. A locked
// transaction must be committed or aborted before another transaction can be
// started. Unlocked transactions serve as a point in time snapshots and can be
// just be discarded when not being used further.
func (e *Engine) Begin(ctx context.Context, lock bool) (*Transaction, error) {
	// acquire lock
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check if closed
	if !e.tomb.Alive() {
		return nil, ErrEngineClosed
	}

	// non lock transactions do not need to be managed
	if !lock {
		return NewTransaction(e.catalog), nil
	}

	// ensure context
	ctx = ensureContext(ctx)

	// check for transaction
	sess, ok := ctx.Value(sessionKey{}).(*Session)
	if ok {
		txn := sess.Transaction()
		if txn != nil {
			return nil, fmt.Errorf("detected nested transaction")
		}
	}

	// acquire token (without lock)
	e.mutex.Unlock()
	ok = e.token.Acquire(ctx.Done(), time.Minute)
	e.mutex.Lock()
	if !ok {
		return nil, fmt.Errorf("token acquisition timeout")
	}

	// assert transaction
	if e.txn != nil {
		e.token.Release()
		return nil, fmt.Errorf("existing transaction")
	}

	// create transaction
	e.txn = NewTransaction(e.catalog)

	return e.txn, nil
}

// Commit will attempt to store the modified catalog and on success replace the
// current catalog. If an error is returned the transaction has been aborted
// and become invalid.
func (e *Engine) Commit(txn *Transaction) error {
	// acquire lock
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check if closed
	if !e.tomb.Alive() {
		return ErrEngineClosed
	}

	// check transaction
	if e.txn != txn {
		return fmt.Errorf("existing transaction")
	}

	// ensure token is released
	defer e.token.Release()

	// unset transaction
	e.txn = nil

	// check if dirty
	if !txn.Dirty() {
		return nil
	}

	// clean oplog
	txn.Clean(e.opts.MinOplogSize, e.opts.MaxOplogSize, e.opts.MinOplogAge, e.opts.MaxOplogAge)

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

// Abort will abort the specified transaction. To ensure a transaction is
// always released, Abort should be called after finishing any transaction.
func (e *Engine) Abort(txn *Transaction) {
	// acquire lock
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check if closed
	if !e.tomb.Alive() {
		return
	}

	// check transaction
	if e.txn != txn {
		return
	}

	// unset transaction
	e.txn = nil

	// release token
	e.token.Release()
}

// Watch will return a stream that is able to consume events from the oplog.
func (e *Engine) Watch(handle Handle, pipeline bsonkit.List, resumeAfter, startAfter bsonkit.Doc, startAt *primitive.Timestamp) (*Stream, error) {
	// acquire lock
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check if closed
	if !e.tomb.Alive() {
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
			res := bsonkit.Compare(*resumeAfter, bsonkit.Get(event, "_id"), nil)
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
			res := bsonkit.Compare(*startAfter, bsonkit.Get(event, "_id"), nil)
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
			res := bsonkit.Compare(*startAt, bsonkit.Get(event, "clusterTime"), nil)
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
		handle:   handle,
		last:     last,
		pipeline: pipeline,
		signal:   make(chan struct{}, 1),
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
	if !e.tomb.Alive() {
		return
	}

	// close streams
	for stream := range e.streams {
		close(stream.signal)
	}

	// kill tomb
	e.tomb.Kill(nil)
	_ = e.tomb.Wait()
}

func (e *Engine) expire(interval time.Duration, reporter func(error)) {
	// prepare ticker
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		// await next interval
		select {
		case <-e.tomb.Dying():
			return
		case <-ticker.C:
		}

		// get transaction
		txn, err := e.Begin(nil, true)
		if err != nil {
			if reporter != nil {
				reporter(err)
			}
			continue
		}

		// expire documents
		err = txn.Expire()
		if err != nil {
			e.Abort(txn)
			if reporter != nil {
				reporter(err)
			}
			continue
		}

		// commit transaction
		err = e.Commit(txn)
		if err != nil {
			if reporter != nil {
				reporter(err)
			}
			continue
		}
	}
}
