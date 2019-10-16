package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

var _ Database = &AltDatabase{}

type AltDatabase struct {
	name   string
	client *AltClient
}

func (d *AltDatabase) Aggregate(context.Context, interface{}, ...*options.AggregateOptions) (Cursor, error) {
	panic("not implemented")
}

func (d *AltDatabase) Client() Client {
	return d.client
}

func (d *AltDatabase) Collection(name string, opts ...*options.CollectionOptions) Collection {
	// merge options
	opt := options.MergeCollectionOptions(opts...)

	// assert unsupported options
	d.client.assertUnsupported(opt.ReadConcern == nil, "CollectionOptions.ReadConcern")
	d.client.assertUnsupported(opt.WriteConcern == nil, "CollectionOptions.WriteConcern")
	d.client.assertUnsupported(opt.ReadPreference == nil, "CollectionOptions.ReadPreference")
	d.client.assertUnsupported(opt.Registry == nil, "CollectionOptions.Registry")

	return &AltCollection{
		name:   name,
		db:     d,
		client: d.client,
	}
}

func (d *AltDatabase) Drop(context.Context) error {
	panic("not implemented")
}

func (d *AltDatabase) ListCollectionNames(context.Context, interface{}, ...*options.ListCollectionsOptions) ([]string, error) {
	panic("not implemented")
}

func (d *AltDatabase) ListCollections(context.Context, interface{}, ...*options.ListCollectionsOptions) (Cursor, error) {
	panic("not implemented")
}

func (d *AltDatabase) Name() string {
	panic("not implemented")
}

func (d *AltDatabase) ReadConcern() *readconcern.ReadConcern {
	panic("not implemented")
}

func (d *AltDatabase) ReadPreference() *readpref.ReadPref {
	panic("not implemented")
}

func (d *AltDatabase) RunCommand(context.Context, interface{}, ...*options.RunCmdOptions) *mongo.SingleResult {
	panic("not implemented")
}

func (d *AltDatabase) RunCommandCursor(context.Context, interface{}, ...*options.RunCmdOptions) (Cursor, error) {
	panic("not implemented")
}

func (d *AltDatabase) Watch(context.Context, interface{}, ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error) {
	panic("not implemented")
}

func (d *AltDatabase) WriteConcern() *writeconcern.WriteConcern {
	panic("not implemented")
}
