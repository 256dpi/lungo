package lungo

import (
	"context"
	"errors"
	"io"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/256dpi/lungo/bsonkit"
)

// ErrLostOplogPosition may be returned by a stream when the oplog position has
// been lost. This can happen if a consumer is slower than the expiration of
// oplog entries.
var ErrLostOplogPosition = errors.New("lost oplog position")

// Stream provides a mongo compatible way to read oplog events.
type Stream struct {
	handle   Handle
	last     bsonkit.Doc
	pipeline bsonkit.List
	signal   chan struct{}
	oplog    func() *bsonkit.Set
	cancel   func()
	event    bsonkit.Doc
	token    interface{}
	dropped  bool
	closed   bool
	error    error
	mutex    sync.Mutex
}

// Close implements the IChangeStream.Close method.
func (s *Stream) Close(context.Context) error {
	// acquire mutex
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check state
	if s.closed {
		return nil
	}

	// close stream
	s.cancel()
	s.event = nil
	s.closed = true
	s.error = nil

	// wake up any goroutine blocked in next() (non-blocking send so we don't
	// stall when the buffered signal slot is already full)
	select {
	case s.signal <- struct{}{}:
	default:
	}

	return nil
}

// Decode implements the IChangeStream.Decode method.
func (s *Stream) Decode(out interface{}) error {
	// acquire mutex
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check validity
	if s.event == nil {
		if s.closed {
			return mongo.ErrNilCursor
		}
		return io.EOF
	}

	// decode event
	err := bsonkit.Decode(s.event, out)
	if err != nil {
		return err
	}

	return nil
}

// Err implements the IChangeStream.Err method.
func (s *Stream) Err() error {
	// acquire mutex
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.error
}

// ID implements the IChangeStream.ID method.
func (s *Stream) ID() int64 {
	// acquire mutex
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return 0
}

// Next implements the IChangeStream.Next method.
func (s *Stream) Next(ctx context.Context) bool {
	return s.next(ctx, true)
}

// RemainingBatchLength implements the IChangeStream.RemainingBatchLength
// method.
func (s *Stream) RemainingBatchLength() int {
	return 0
}

// ResumeToken implements the IChangeStream.ResumeToken method.
func (s *Stream) ResumeToken() bson.Raw {
	// acquire mutex
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check token
	if s.token == nil {
		return nil
	}

	// encode token
	bytes, _ := bson.Marshal(s.token)

	return bytes
}

// SetBatchSize implements the IChangeStream.SetBatchSize method.
func (s *Stream) SetBatchSize(int32) {}

// TryNext implements the IChangeStream.TryNext method.
func (s *Stream) TryNext(ctx context.Context) bool {
	return s.next(ctx, false)
}

func (s *Stream) next(ctx context.Context, block bool) bool {
	// ensure context
	ctx = ensureContext(ctx)

	for {
		// acquire mutex
		s.mutex.Lock()

		// check validity
		if s.error != nil || s.closed {
			s.mutex.Unlock()
			return false
		}

		// check if dropped
		if s.dropped {
			s.event = bsonkit.MustConvert(bson.M{
				"_id":           bson.M{"ts": "drop"},
				"operationType": "invalidate",
				"clusterTime":   bsonkit.Now(),
			})
			s.token = bsonkit.Get(s.event, "_id")
			s.cancel()
			s.closed = true
			s.mutex.Unlock()
			return true
		}

		// get oplog
		oplog := s.oplog()

		// get index
		index := -1
		if s.last != nil {
			i, ok := oplog.Index[s.last]
			if !ok {
				s.cancel()
				s.closed = true
				s.error = ErrLostOplogPosition
				s.mutex.Unlock()
				return false
			}
			index = i
		}

		// get next event
		if len(oplog.List) > index+1 {
			// get event
			event := oplog.List[index+1]

			// get details
			token := bsonkit.Get(event, "_id")
			nsDB := bsonkit.Get(event, "ns.db")
			nsColl := bsonkit.Get(event, "ns.coll")
			opType := bsonkit.Get(event, "operationType")

			// match database and collection
			if s.handle[0] != "" && s.handle[0] != nsDB {
				s.last = event
				s.mutex.Unlock()
				continue
			} else if s.handle[1] != "" && s.handle[1] != nsColl && opType != "dropDatabase" {
				// dropDatabase events carry only ns.db; let them through so a
				// collection-scoped stream watching a database that's being
				// dropped still gets its invalidation
				s.last = event
				s.mutex.Unlock()
				continue
			}

			// check drop and drop database
			if s.handle[0] != "" && s.handle[1] != "" && opType == "drop" {
				s.dropped = true
			} else if s.handle[0] != "" && opType == "dropDatabase" {
				s.dropped = true
			}

			// TODO: Filter with pipeline.

			// set event and token
			s.last = event
			s.event = event
			s.token = token
			s.mutex.Unlock()
			return true
		}

		// handle non blocking
		if !block {
			if err := ctx.Err(); err != nil {
				s.error = err
			}
			s.mutex.Unlock()
			return false
		}

		// release the mutex while blocking so Close and other accessors can
		// run concurrently with the wait
		signal := s.signal
		s.mutex.Unlock()

		// await next event
		select {
		case _, ok := <-signal:
			if !ok {
				// close stream
				s.mutex.Lock()
				s.cancel()
				s.closed = true
				s.mutex.Unlock()
				return false
			}
		case <-ctx.Done():
			// set error
			s.mutex.Lock()
			if s.error == nil {
				s.error = ctx.Err()
			}
			s.mutex.Unlock()
			return false
		}
	}
}

func (s *Stream) RemainingBatchLength() int {
	panic("lungo: unimplemented")
}
