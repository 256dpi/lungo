package lungo

import (
	"context"
	"errors"
	"sync"

	"github.com/256dpi/lungo/bsonkit"
)

var ErrCursorClosed = errors.New("cursor closed")
var ErrCursorExhausted = errors.New("cursor exhausted")

type Cursor struct {
	list   bsonkit.List
	pos    int
	closed bool
	mutex  sync.Mutex
}

func (c *Cursor) All(ctx context.Context, out interface{}) error {
	// acquire mutex
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// check if closed
	if c.closed {
		return ErrCursorClosed
	}

	// decode items
	err := bsonkit.DecodeList(c.list, out)
	if err != nil {
		return err
	}

	// close cursor
	c.closed = true

	return nil
}

func (c *Cursor) Close(ctx context.Context) error {
	// acquire mutex
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// close cursor
	c.closed = true

	return nil
}

func (c *Cursor) Decode(out interface{}) error {
	// acquire mutex
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// check if closed
	if c.closed {
		return ErrCursorClosed
	}

	// check if exhausted
	if c.pos > len(c.list) {
		return ErrCursorExhausted
	}

	// decode item
	err := bsonkit.Decode(c.list[c.pos-1], out)
	if err != nil {
		return err
	}

	return nil
}

func (c *Cursor) Err() error {
	return nil
}

func (c *Cursor) ID() int64 {
	return 0
}

func (c *Cursor) Next(context.Context) bool {
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
