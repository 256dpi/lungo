package lungo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var _ IClient = &AltClient{}

type AltClientOptions struct {
	Store          Store
	AssertCallback func(string)
}

type AltClient struct {
	backend *Backend
	opts    AltClientOptions
}

func Open(ctx context.Context, opts AltClientOptions) (IClient, error) {
	// create backend
	backend := newBackend(opts.Store)

	// setup backend
	err := backend.setup()
	if err != nil {
		return nil, err
	}

	return &AltClient{
		backend: backend,
		opts:    opts,
	}, nil
}

func (c *AltClient) Connect(context.Context) error {
	panic("not implemented")
}

func (c *AltClient) Database(name string, opts ...*options.DatabaseOptions) IDatabase {
	// merge options
	opt := options.MergeDatabaseOptions(opts...)

	// assert unsupported options
	c.assertUnsupported(opt.ReadConcern == nil, "DatabaseOptions.ReadConcern")
	c.assertUnsupported(opt.WriteConcern == nil, "DatabaseOptions.WriteConcern")
	c.assertUnsupported(opt.ReadPreference == nil, "DatabaseOptions.ReadPreference")
	c.assertUnsupported(opt.Registry == nil, "DatabaseOptions.Registry")

	return &AltDatabase{
		name:   name,
		client: c,
	}
}

func (c *AltClient) Disconnect(context.Context) error {
	panic("not implemented")
}

func (c *AltClient) ListDatabaseNames(context.Context, interface{}, ...*options.ListDatabasesOptions) ([]string, error) {
	panic("not implemented")
}

func (c *AltClient) ListDatabases(context.Context, interface{}, ...*options.ListDatabasesOptions) (mongo.ListDatabasesResult, error) {
	panic("not implemented")
}

func (c *AltClient) Ping(context.Context, *readpref.ReadPref) error {
	return nil
}

func (c *AltClient) StartSession(...*options.SessionOptions) (mongo.Session, error) {
	panic("not implemented")
}

func (c *AltClient) UseSession(context.Context, func(mongo.SessionContext) error) error {
	panic("not implemented")
}

func (c *AltClient) UseSessionWithOptions(context.Context, *options.SessionOptions, func(mongo.SessionContext) error) error {
	panic("not implemented")
}

func (c *AltClient) Watch(context.Context, interface{}, ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error) {
	panic("not implemented")
}

func (c *AltClient) assertUnsupported(ok bool, typ string) {
	// check condition
	if ok {
		return
	}

	// create message
	msg := fmt.Sprintf("unsupported: %s", typ)

	// call callback or panic
	if c.opts.AssertCallback != nil {
		c.opts.AssertCallback(msg)
	} else {
		panic(msg)
	}
}
