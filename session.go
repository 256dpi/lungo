package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Session provides a mongo compatible way to handle transactions.
type Session struct {
	client *Client
}

// AbortTransaction implements the ISession.AbortTransaction method.
func (s *Session) AbortTransaction(context.Context) error {
	panic("lungo: not implemented")
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
	return s.client
}

// ClusterTime implements the ISession.ClusterTime method.
func (s *Session) ClusterTime() bson.Raw {
	panic("lungo: not implemented")
}

// CommitTransaction implements the ISession.CommitTransaction method.
func (s *Session) CommitTransaction(context.Context) error {
	panic("lungo: not implemented")
}

// EndSession implements the ISession.EndSession method.
func (s *Session) EndSession(context.Context) {
	panic("lungo: not implemented")
}

// OperationTime implements the ISession.OperationTime method.
func (s *Session) OperationTime() *primitive.Timestamp {
	panic("lungo: not implemented")
}

// StartTransaction implements the ISession.StartTransaction method.
func (s *Session) StartTransaction(opts ...*options.TransactionOptions) error {
	// merge options
	opt := options.MergeTransactionOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"ReadConcern":    ignored,
		"ReadPreference": ignored,
		"WriteConcern":   ignored,
		"MaxCommitTime":  ignored,
	})

	panic("lungo: not implemented")
}

// WithTransaction implements the ISession.WithTransaction method.
func (s *Session) WithTransaction(ctx context.Context, fn func(sessCtx ISessionContext) (interface{}, error), opts ...*options.TransactionOptions) (interface{}, error) {
	panic("lungo: not implemented")
}