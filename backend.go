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
	Find(db, coll string, qry bson.M) (Cursor, error)
	InsertOne(db, coll string, doc bson.M) error
}

type MemoryBackend struct {
	store map[string]map[string][]bson.M
	mutex sync.Mutex
}

func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		store: make(map[string]map[string][]bson.M),
	}
}

func (b *MemoryBackend) Find(db, coll string, qry bson.M) (Cursor, error) {
	// acquire mutex
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// check db
	if b.store[db] == nil {
		// TODO: What does mongo do?
		return &MemoryCursor{}, nil
	}

	// check coll
	if b.store[db][coll] == nil {
		// TODO: What does mongo do?
		return &MemoryCursor{}, nil
	}

	// TODO: Apply query.

	return &MemoryCursor{
		list: b.store[db][coll],
		pos:  0,
	}, nil
}

func (b *MemoryBackend) InsertOne(db, coll string, doc bson.M) error {
	// acquire mutex
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// ensure db
	if b.store[db] == nil {
		b.store[db] = make(map[string][]bson.M)
	}

	// ensure coll
	if b.store[db][coll] == nil {
		b.store[db][coll] = make([]bson.M, 0)
	}

	// add document
	b.store[db][coll] = append(b.store[db][coll], doc)

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
