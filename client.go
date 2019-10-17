package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var _ IClient = &Client{}

type ClientOptions struct {
	Store Store
}

type Client struct {
	backend *Backend
	opts    ClientOptions
}

func Open(ctx context.Context, opts ClientOptions) (IClient, error) {
	// create backend
	backend := newBackend(opts.Store)

	// setup backend
	err := backend.setup()
	if err != nil {
		return nil, err
	}

	return &Client{
		backend: backend,
		opts:    opts,
	}, nil
}

func (c *Client) Connect(context.Context) error {
	panic("not implemented")
}

func (c *Client) Database(name string, opts ...*options.DatabaseOptions) IDatabase {
	// merge options
	opt := options.MergeDatabaseOptions(opts...)

	// assert unsupported options
	err := assertUnsupported(map[string]bool{
		"DatabaseOptions.ReadConcern":    opt.ReadConcern != nil,
		"DatabaseOptions.WriteConcern":   opt.WriteConcern != nil,
		"DatabaseOptions.ReadPreference": opt.ReadPreference != nil,
		"DatabaseOptions.Registry":       opt.Registry != nil,
	})
	if err != nil {
		panic(err)
	}

	return &Database{
		name:   name,
		client: c,
	}
}

func (c *Client) Disconnect(context.Context) error {
	panic("not implemented")
}

func (c *Client) ListDatabaseNames(context.Context, interface{}, ...*options.ListDatabasesOptions) ([]string, error) {
	panic("not implemented")
}

func (c *Client) ListDatabases(context.Context, interface{}, ...*options.ListDatabasesOptions) (mongo.ListDatabasesResult, error) {
	panic("not implemented")
}

func (c *Client) Ping(context.Context, *readpref.ReadPref) error {
	return nil
}

func (c *Client) StartSession(...*options.SessionOptions) (mongo.Session, error) {
	panic("not implemented")
}

func (c *Client) UseSession(context.Context, func(mongo.SessionContext) error) error {
	panic("not implemented")
}

func (c *Client) UseSessionWithOptions(context.Context, *options.SessionOptions, func(mongo.SessionContext) error) error {
	panic("not implemented")
}

func (c *Client) Watch(context.Context, interface{}, ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error) {
	panic("not implemented")
}
