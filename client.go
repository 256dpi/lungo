package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/256dpi/lungo/bsonkit"
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
	assertUnsupported(map[string]bool{
		"DatabaseOptions.ReadConcern":    opt.ReadConcern != nil,
		"DatabaseOptions.WriteConcern":   opt.WriteConcern != nil,
		"DatabaseOptions.ReadPreference": opt.ReadPreference != nil,
		"DatabaseOptions.Registry":       opt.Registry != nil,
	})

	return &Database{
		name:   name,
		client: c,
	}
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

	// assert unsupported options
	assertUnsupported(map[string]bool{
		"ListDatabasesOptions.NameOnly": opt.NameOnly != nil,
	})

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return mongo.ListDatabasesResult{}, err
	}

	// list collections
	list, err := c.engine.listDatabases(query)
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
