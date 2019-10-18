package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var _ IClient = &Client{}

type Client struct {
	engine *engine
}

func Open(ctx context.Context, store Store) (IClient, error) {
	// create engine
	engine, err := createEngine(store)
	if err != nil {
		return nil, err
	}

	return &Client{
		engine: engine,
	}, nil
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

func (c *Client) ListDatabaseNames(context.Context, interface{}, ...*options.ListDatabasesOptions) ([]string, error) {
	panic("not implemented")
}

func (c *Client) ListDatabases(context.Context, interface{}, ...*options.ListDatabasesOptions) (mongo.ListDatabasesResult, error) {
	panic("not implemented")
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
