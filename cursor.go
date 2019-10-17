package lungo

import (
	"context"
	"errors"
	"sync"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

var errCursorClosed = errors.New("cursor closed")
var errCursorExhausted = errors.New("cursor exhausted")

type staticCursor struct {
	list   []bson.D
	pos    int
	closed bool
	mutex  sync.Mutex
}

func (c *staticCursor) All(ctx context.Context, out interface{}) error {
	// acquire mutex
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// check if closed
	if c.closed {
		return errCursorClosed
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

func (c *staticCursor) Close(ctx context.Context) error {
	// acquire mutex
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// close cursor
	c.closed = true

	return nil
}

func (c *staticCursor) Decode(out interface{}) error {
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
	err := bsonkit.Decode(c.list[c.pos-1], out)
	if err != nil {
		return err
	}

	return nil
}

func (c *staticCursor) Err() error {
	return nil
}

func (c *staticCursor) ID() int64 {
	return 0
}

func (c *staticCursor) Next(context.Context) bool {
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
