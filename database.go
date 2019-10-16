package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

var _ IDatabase = &Database{}

type Database struct {
	name   string
	client *Client
}

func (d *Database) Aggregate(context.Context, interface{}, ...*options.AggregateOptions) (ICursor, error) {
	panic("not implemented")
}

func (d *Database) Client() IClient {
	return d.client
}

func (d *Database) Collection(name string, opts ...*options.CollectionOptions) ICollection {
	// merge options
	opt := options.MergeCollectionOptions(opts...)

	// assert unsupported options
	d.client.assertUnsupported(opt.ReadConcern == nil, "CollectionOptions.ReadConcern")
	d.client.assertUnsupported(opt.WriteConcern == nil, "CollectionOptions.WriteConcern")
	d.client.assertUnsupported(opt.ReadPreference == nil, "CollectionOptions.ReadPreference")
	d.client.assertUnsupported(opt.Registry == nil, "CollectionOptions.Registry")

	return &Collection{
		ns:     d.name + "." + name,
		name:   name,
		db:     d,
		client: d.client,
	}
}

func (d *Database) Drop(context.Context) error {
	panic("not implemented")
}

func (d *Database) ListCollectionNames(context.Context, interface{}, ...*options.ListCollectionsOptions) ([]string, error) {
	panic("not implemented")
}

func (d *Database) ListCollections(context.Context, interface{}, ...*options.ListCollectionsOptions) (ICursor, error) {
	panic("not implemented")
}

func (d *Database) Name() string {
	panic("not implemented")
}

func (d *Database) ReadConcern() *readconcern.ReadConcern {
	panic("not implemented")
}

func (d *Database) ReadPreference() *readpref.ReadPref {
	panic("not implemented")
}

func (d *Database) RunCommand(context.Context, interface{}, ...*options.RunCmdOptions) *mongo.SingleResult {
	panic("not implemented")
}

func (d *Database) RunCommandCursor(context.Context, interface{}, ...*options.RunCmdOptions) (ICursor, error) {
	panic("not implemented")
}

func (d *Database) Watch(context.Context, interface{}, ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error) {
	panic("not implemented")
}

func (d *Database) WriteConcern() *writeconcern.WriteConcern {
	panic("not implemented")
}
