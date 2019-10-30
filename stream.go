package lungo

import (
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/256dpi/lungo/bsonkit"
)

// Stream provides a mongo compatible way to read oplog events.
type Stream struct {
	handle Handle
	filter bsonkit.List
	signal chan struct{}
	oplog  func() *bsonkit.Set
	cancel func()
	event  bsonkit.Doc
	token  interface{}
	closed bool
	mutex  sync.Mutex
}

// Close implements the IChangeStream.Close method.
func (s *Stream) Close(context.Context) error {
	// acquire mutex
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// close stream
	s.cancel()
	s.closed = true

	return nil
}

// Decode implements the IChangeStream.Decode method.
func (s *Stream) Decode(out interface{}) error {
	// acquire mutex
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check event
	if s.event == nil {
		return mongo.ErrNilCursor
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

	return nil
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
	// acquire mutex
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if closed
	if s.closed {
		return false
	}

	// ensure context
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		// get current oplog set
		oplog := s.oplog()

		// get next index
		var index int
		if s.event != nil {
			index = oplog.Index[s.event] + 1
		}

		// get next event
		if len(oplog.List) > index {
			// get event and token
			event := oplog.List[index]
			token := bsonkit.Get(event, "_id")

			// TODO: Match handle.
			// TODO: Match with filter.
			// TODO: Generate invalidate after drop and close.

			// set event and token
			s.event = event
			s.token = token

			return true
		}

		// await next event
		select {
		case _, ok := <-s.signal:
			if ok {
				continue
			}
		case <-ctx.Done():
			return false
		}

		// close stream
		s.cancel()
		s.closed = true

		return false
	}
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
