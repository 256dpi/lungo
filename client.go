package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/256dpi/lungo/bsonkit"
)

var _ IClient = &Client{}

type Client struct {
	engine *Engine
}

func Open(ctx context.Context, store Store) (IClient, error) {
	// create engine
	engine, err := CreateEngine(store)
	if err != nil {
		return nil, err
	}

	return &Client{
		engine: engine,
	}, nil
}

func (c *Client) Connect(context.Context) error {
	return nil
}

func (c *Client) Database(name string, opts ...*options.DatabaseOptions) IDatabase {
	// merge options
	opt := options.MergeDatabaseOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{})

	return &Database{
		name:   name,
		client: c,
	}
}

func (c *Client) Disconnect(context.Context) error {
	return nil
}

func (c *Client) ListDatabaseNames(ctx context.Context, filter interface{}, opts ...*options.ListDatabasesOptions) ([]string, error) {
	// list databases
	res, err := c.ListDatabases(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}

	// collect names
	names := make([]string, 0, len(res.Databases))
	for _, db := range res.Databases {
		names = append(names, db.Name)
	}

	return names, nil
}

func (c *Client) ListDatabases(ctx context.Context, filter interface{}, opts ...*options.ListDatabasesOptions) (mongo.ListDatabasesResult, error) {
	// merge options
	opt := options.MergeListDatabasesOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{})

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return mongo.ListDatabasesResult{}, err
	}

	// list collections
	list, err := c.engine.ListDatabases(query)
	if err != nil {
		return mongo.ListDatabasesResult{}, err
	}

	// decode documents
	specs := make([]mongo.DatabaseSpecification, 0, len(list))
	err = bsonkit.DecodeList(list, &specs)
	if err != nil {
		return mongo.ListDatabasesResult{}, err
	}

	// sum size
	var totalSize int64
	for _, spec := range specs {
		totalSize += spec.SizeOnDisk
	}

	// prepare result
	result := mongo.ListDatabasesResult{
		Databases: specs,
		TotalSize: totalSize,
	}

	return result, nil
}

func (c *Client) Ping(context.Context, *readpref.ReadPref) error {
	return nil
}

func (c *Client) StartSession(...*options.SessionOptions) (mongo.Session, error) {
	panic("lungo: not implemented")
}

func (c *Client) UseSession(context.Context, func(mongo.SessionContext) error) error {
	panic("lungo: not implemented")
}

func (c *Client) UseSessionWithOptions(context.Context, *options.SessionOptions, func(mongo.SessionContext) error) error {
	panic("lungo: not implemented")
}

func (c *Client) Watch(context.Context, interface{}, ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error) {
	panic("lungo: not implemented")
}
