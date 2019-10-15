package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var _ Client = &MongoClient{}

type MongoClient struct {
	*mongo.Client
}

func Connect(ctx context.Context, opts ...*options.ClientOptions) (Client, error) {
	client, err := mongo.Connect(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return &MongoClient{Client: client}, nil
}

func (c *MongoClient) Database(name string, opts ...*options.DatabaseOptions) Database {
	return &MongoDatabase{Database: c.Client.Database(name, opts...), client: c}
}

var _ Database = &MongoDatabase{}

type MongoDatabase struct {
	*mongo.Database

	client *MongoClient
}

func (m *MongoDatabase) Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (Cursor, error) {
	return m.Database.Aggregate(ctx, pipeline, opts...)
}

func (m *MongoDatabase) Client() Client {
	return m.client
}

func (m *MongoDatabase) Collection(name string, opts ...*options.CollectionOptions) Collection {
	return &MongoCollection{Collection: m.Database.Collection(name, opts...)}
}

func (m *MongoDatabase) ListCollections(ctx context.Context, filter interface{}, opts ...*options.ListCollectionsOptions) (Cursor, error) {
	return m.Database.ListCollections(ctx, filter, opts...)
}

func (m *MongoDatabase) RunCommandCursor(ctx context.Context, filter interface{}, opts ...*options.RunCmdOptions) (Cursor, error) {
	return m.Database.RunCommandCursor(ctx, filter, opts...)
}

type MongoCollection struct {
	*mongo.Collection

	db *MongoDatabase
}

func (c *MongoCollection) Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (Cursor, error) {
	return c.Collection.Aggregate(ctx, pipeline, opts...)
}

func (c *MongoCollection) Clone(opts ...*options.CollectionOptions) (Collection, error) {
	coll, err := c.Collection.Clone(opts...)
	if err != nil {
		return nil, err
	}

	return &MongoCollection{Collection: coll, db: c.db}, nil
}

func (c *MongoCollection) Database() Database {
	return c.db
}

func (c *MongoCollection) Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (Cursor, error) {
	return c.Collection.Find(ctx, filter, opts...)
}
