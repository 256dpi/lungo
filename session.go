package lungo

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type sessionKey struct{}

// ErrSessionEnded is returned if the session has been ended.
var ErrSessionEnded = errors.New("session ended")

// SessionContext provides a mongo compatible session context.
type SessionContext struct {
	context.Context
	*Session
}

// Session provides a mongo compatible way to handle transactions.
type Session struct {
	engine *Engine
	txn    *Transaction
	ended  bool
	mutex  sync.Mutex
}

// AbortTransaction implements the ISession.AbortTransaction method.
func (s *Session) AbortTransaction(context.Context) error {
	// acquire lock
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if ended
	if s.ended {
		return ErrSessionEnded
	}

	// abort and unset transaction
	if s.txn != nil {
		s.engine.Abort(s.txn)
		s.txn = nil
	}

	return nil
}

// AdvanceClusterTime implements the ISession.AdvanceClusterTime method.
func (s *Session) AdvanceClusterTime(bson.Raw) error {
	panic("lungo: not implemented")
}

// AdvanceOperationTime implements the ISession.AdvanceOperationTime method.
func (s *Session) AdvanceOperationTime(*primitive.Timestamp) error {
	panic("lungo: not implemented")
}

// Client implements the ISession.Client method.
func (s *Session) Client() IClient {
	return &Client{
		engine: s.engine,
	}
}

// ClusterTime implements the ISession.ClusterTime method.
func (s *Session) ClusterTime() bson.Raw {
	panic("lungo: not implemented")
}

// CommitTransaction implements the ISession.CommitTransaction method.
func (s *Session) CommitTransaction(context.Context) error {
	// acquire lock
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if ended
	if s.ended {
		return ErrSessionEnded
	}

	// check transaction
	if s.txn == nil {
		return fmt.Errorf("missing transaction")
	}

	// get and unset transaction
	txn := s.txn
	s.txn = nil

	// commit transaction
	err := s.engine.Commit(txn)
	if err != nil {
		return err
	}

	return nil
}

// EndSession implements the ISession.EndSession method.
func (s *Session) EndSession(context.Context) {
	// acquire lock
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if ended
	if s.ended {
		return
	}

	// abort and unset transaction
	if s.txn != nil {
		s.engine.Abort(s.txn)
		s.txn = nil
	}

	// set flag
	s.ended = true
}

// OperationTime implements the ISession.OperationTime method.
func (s *Session) OperationTime() *primitive.Timestamp {
	panic("lungo: not implemented")
}

// StartTransaction implements the ISession.StartTransaction method.
func (s *Session) StartTransaction(opts ...*options.TransactionOptions) error {
	// acquire lock
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if ended
	if s.ended {
		return ErrSessionEnded
	}

	// merge options
	opt := options.MergeTransactionOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"ReadConcern":    ignored,
		"ReadPreference": ignored,
		"WriteConcern":   ignored,
		"MaxCommitTime":  ignored,
	})

	// check transaction
	if s.txn != nil {
		return fmt.Errorf("existing transaction")
	}

	// create transaction
	s.txn = s.engine.Begin(nil, true)

	return nil
}

// WithTransaction implements the ISession.WithTransaction method.
func (s *Session) WithTransaction(ctx context.Context, fn func(ISessionContext) (interface{}, error), opts ...*options.TransactionOptions) (interface{}, error) {
	// do not take locks as we only use safe functions

	// merge options
	opt := options.MergeTransactionOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"ReadConcern":    ignored,
		"ReadPreference": ignored,
		"WriteConcern":   ignored,
		"MaxCommitTime":  ignored,
	})

	// start transaction
	err := s.StartTransaction(opt)
	if err != nil {
		return nil, err
	}

	// ensure abort
	defer func() {
		_ = s.AbortTransaction(ctx)
	}()

	// yield transaction
	res, err := fn(&SessionContext{
		Context: context.WithValue(ctx, sessionKey{}, s),
		Session: s,
	})
	if err != nil {
		return nil, err
	}

	// commit transaction
	err = s.CommitTransaction(ctx)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Transaction will return the active transaction or nil if no transaction has
// been started.
func (s *Session) Transaction() *Transaction {
	// acquire lock
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.txn
}
