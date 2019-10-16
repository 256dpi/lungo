package lungo

import (
	"context"
	"errors"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
)

var errCursorClosed = errors.New("cursor closed")
var errCursorExhausted = errors.New("cursor exhausted")

type Backend interface {
	Setup() error
	Find(db, coll string, qry bson.M) (Cursor, error)
	InsertOne(db, coll string, doc bson.M) error
}

type MemoryBackend struct {
	store Store
	data  *Data
	mutex sync.Mutex
}

func NewMemoryBackend(store Store) *MemoryBackend {
	return &MemoryBackend{
		store: store,
	}
}

func (b *MemoryBackend) Setup() error {
	// load data
	data, err := b.store.Load()
	if err != nil {
		return err
	}

	// set data
	b.data = data

	return nil
}

func (b *MemoryBackend) Find(db, coll string, qry bson.M) (Cursor, error) {
	// acquire mutex
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// check database
	if b.data.Databases[db] == nil {
		// TODO: What does mongo do?
		return &MemoryCursor{}, nil
	}

	// check collection
	if b.data.Databases[db].Collections[coll] == nil {
		// TODO: What does mongo do?
		return &MemoryCursor{}, nil
	}

	// TODO: Apply query.

	return &MemoryCursor{
		list: b.data.Databases[db].Collections[coll].Documents,
		pos:  0,
	}, nil
}

func (b *MemoryBackend) InsertOne(db, coll string, doc bson.M) error {
	// acquire mutex
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// ensure database
	if b.data.Databases[db] == nil {
		b.data.Databases[db] = NewDatabaseData(db)
	}

	// ensure collection
	if b.data.Databases[db].Collections[coll] == nil {
		b.data.Databases[db].Collections[coll] = NewCollectionData(coll)
	}

	// TODO: Check indexes (unique id).

	// add document
	b.data.Databases[db].Collections[coll].Documents = append(b.data.Databases[db].Collections[coll].Documents, doc)

	return nil
}

type MemoryCursor struct {
	list   []bson.M
	pos    int
	closed bool
	mutex  sync.Mutex
}

func (c *MemoryCursor) All(ctx context.Context, out interface{}) error {
	// acquire mutex
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// check if closed
	if c.closed {
		return errCursorClosed
	}

	// decode items
	err := DecodeList(c.list, out)
	if err != nil {
		return err
	}

	// close cursor
	c.closed = true

	return nil
}

func (c *MemoryCursor) Close(ctx context.Context) error {
	// acquire mutex
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// close cursor
	c.closed = true

	return nil
}

func (c *MemoryCursor) Decode(out interface{}) error {
	// acquire mutex
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// check if closed
	if c.closed {
		return errCursorClosed
	}

	// check if exhausted
	if c.pos > len(c.list) {
		return errCursorExhausted
	}

	// decode item
	err := DecodeItem(c.list[c.pos-1], out)
	if err != nil {
		return err
	}

	return nil
}

func (c *MemoryCursor) Err() error {
	return nil
}

func (c *MemoryCursor) ID() int64 {
	return 0
}

func (c *MemoryCursor) Next(context.Context) bool {
	// acquire mutex
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// check if closed
	if c.closed {
		return false
	}

	// increment position
	c.pos++

	// return whether the are items
	return c.pos <= len(c.list)
}
