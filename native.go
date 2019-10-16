package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var _ IClient = &NativeClient{}

type NativeClient struct {
	*mongo.Client
}

func Connect(ctx context.Context, opts ...*options.ClientOptions) (IClient, error) {
	client, err := mongo.Connect(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return &NativeClient{Client: client}, nil
}

func (c *NativeClient) Database(name string, opts ...*options.DatabaseOptions) IDatabase {
	return &NativeDatabase{Database: c.Client.Database(name, opts...), client: c}
}

var _ IDatabase = &NativeDatabase{}

type NativeDatabase struct {
	*mongo.Database

	client *NativeClient
}

func (m *NativeDatabase) Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (ICursor, error) {
	return m.Database.Aggregate(ctx, pipeline, opts...)
}

func (m *NativeDatabase) Client() IClient {
	return m.client
}

func (m *NativeDatabase) Collection(name string, opts ...*options.CollectionOptions) ICollection {
	return &NativeCollection{Collection: m.Database.Collection(name, opts...)}
}

func (m *NativeDatabase) ListCollections(ctx context.Context, filter interface{}, opts ...*options.ListCollectionsOptions) (ICursor, error) {
	return m.Database.ListCollections(ctx, filter, opts...)
}

func (m *NativeDatabase) RunCommandCursor(ctx context.Context, filter interface{}, opts ...*options.RunCmdOptions) (ICursor, error) {
	return m.Database.RunCommandCursor(ctx, filter, opts...)
}

var _ ICollection = &NativeCollection{}

type NativeCollection struct {
	*mongo.Collection

	db *NativeDatabase
}

func (c *NativeCollection) Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (ICursor, error) {
	return c.Collection.Aggregate(ctx, pipeline, opts...)
}

func (c *NativeCollection) Clone(opts ...*options.CollectionOptions) (ICollection, error) {
	coll, err := c.Collection.Clone(opts...)
	if err != nil {
		return nil, err
	}

	return &NativeCollection{Collection: coll, db: c.db}, nil
}

func (c *NativeCollection) Database() IDatabase {
	return c.db
}

func (c *NativeCollection) Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (ICursor, error) {
	return c.Collection.Find(ctx, filter, opts...)
}
