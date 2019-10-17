package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/256dpi/lungo/bsonkit"
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
	err := assertUnsupported(map[string]bool{
		"CollectionOptions.ReadConcern":    opt.ReadConcern != nil,
		"CollectionOptions.WriteConcern":   opt.WriteConcern != nil,
		"CollectionOptions.ReadPreference": opt.ReadPreference != nil,
		"CollectionOptions.Registry":       opt.Registry != nil,
	})
	if err != nil {
		panic(err)
	}

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

func (d *Database) ListCollectionNames(ctx context.Context, filter interface{}, opts ...*options.ListCollectionsOptions) ([]string, error) {
	// merge options
	opt := options.MergeListCollectionsOptions(opts...)

	// assert unsupported options
	err := assertUnsupported(map[string]bool{
		"ListCollectionsOptions.NameOnly": opt.NameOnly != nil,
	})
	if err != nil {
		return nil, err
	}

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return nil, err
	}

	// list collections
	list, err := d.client.backend.listCollections(d.name, query)
	if err != nil {
		return nil, err
	}

	// collect names
	names := make([]string, 0)
	for _, doc := range list {
		names = append(names, bsonkit.Get(doc, "name").(string))
	}

	return names, nil
}

func (d *Database) ListCollections(ctx context.Context, filter interface{}, opts ...*options.ListCollectionsOptions) (ICursor, error) {
	// merge options
	opt := options.MergeListCollectionsOptions(opts...)

	// assert unsupported options
	err := assertUnsupported(map[string]bool{
		"ListCollectionsOptions.NameOnly": opt.NameOnly != nil,
	})
	if err != nil {
		return nil, err
	}

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return nil, err
	}

	// list collections
	list, err := d.client.backend.listCollections(d.name, query)
	if err != nil {
		return nil, err
	}

	return &staticCursor{list: list}, nil
}

func (d *Database) Name() string {
	return d.name
}

func (d *Database) ReadConcern() *readconcern.ReadConcern {
	return readconcern.New()
}

func (d *Database) ReadPreference() *readpref.ReadPref {
	return readpref.Primary()
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
	return nil
}
