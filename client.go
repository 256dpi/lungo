package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/256dpi/lungo/bsonkit"
)

var _ IClient = &Client{}

// Client wraps an Engine to be mongo compatible.
type Client struct {
	engine *Engine
}

// Open will open a lungo database using the provided store.
func Open(ctx context.Context, opts Options) (IClient, *Engine, error) {
	// create engine
	engine, err := CreateEngine(opts)
	if err != nil {
		return nil, nil, err
	}

	return NewClient(engine), engine, nil
}

// NewClient will create and return a new client.
func NewClient(engine *Engine) IClient {
	return &Client{
		engine: engine,
	}
}

// Connect implements the IClient.Connect method.
func (c *Client) Connect(context.Context) error {
	return nil
}

// Database implements the IClient.Database method.
func (c *Client) Database(name string, opts ...*options.DatabaseOptions) IDatabase {
	// merge options
	opt := options.MergeDatabaseOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"ReadConcern":    ignored,
		"WriteConcern":   ignored,
		"ReadPreference": ignored,
	})

	return &Database{
		name:   name,
		engine: c.engine,
	}
}

// Disconnect implements the IClient.Disconnect method.
func (c *Client) Disconnect(context.Context) error {
	return nil
}

// ListDatabaseNames implements the IClient.ListDatabaseNames method.
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

// ListDatabases implements the IClient.ListDatabases method.
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

// Ping implements the IClient.Ping method.
func (c *Client) Ping(context.Context, *readpref.ReadPref) error {
	return nil
}

// StartSession implements the IClient.StartSession method.
func (c *Client) StartSession(...*options.SessionOptions) (mongo.Session, error) {
	panic("lungo: not implemented")
}

// UseSession implements the IClient.UseSession method.
func (c *Client) UseSession(context.Context, func(mongo.SessionContext) error) error {
	panic("lungo: not implemented")
}

// UseSessionWithOptions implements the IClient.UseSessionWithOptions method.
func (c *Client) UseSessionWithOptions(context.Context, *options.SessionOptions, func(mongo.SessionContext) error) error {
	panic("lungo: not implemented")
}

// Watch implements the IClient.Watch method.
func (c *Client) Watch(_ context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (IChangeStream, error) {
	// merge options
	opt := options.MergeChangeStreamOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"BatchSize":            ignored,
		"FullDocument":         ignored,
		"MaxAwaitTime":         ignored,
		"ResumeAfter":          supported,
		"StartAtOperationTime": supported,
		"StartAfter":           supported,
	})

	// transform pipeline
	filter, err := bsonkit.TransformList(pipeline)
	if err != nil {
		return nil, err
	}

	// get resume after
	var resumeAfter bsonkit.Doc
	if opt.ResumeAfter != nil {
		resumeAfter, err = bsonkit.Transform(opt.ResumeAfter)
		if err != nil {
			return nil, err
		}
	}

	// get start after
	var startAfter bsonkit.Doc
	if opt.StartAfter != nil {
		startAfter, err = bsonkit.Transform(opt.StartAfter)
		if err != nil {
			return nil, err
		}
	}

	// open stream
	stream, err := c.engine.Watch(Handle{}, filter, resumeAfter, startAfter, opt.StartAtOperationTime)
	if err != nil {
		return nil, err
	}

	return stream, nil
}
