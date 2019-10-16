package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var _ Client = &AltClient{}

type AltClientOptions struct {
}

type AltClient struct {
}

func Open(ctx context.Context, opts AltClientOptions) (Client, error) {
	return &AltClient{}, nil
}

func (c *AltClient) Connect(context.Context) error {
	panic("not implemented")
}

func (c *AltClient) Database(string, ...*options.DatabaseOptions) Database {
	panic("not implemented")
}

func (c *AltClient) Disconnect(context.Context) error {
	panic("not implemented")
}

func (c *AltClient) ListDatabaseNames(context.Context, interface{}, ...*options.ListDatabasesOptions) ([]string, error) {
	panic("not implemented")
}

func (c *AltClient) ListDatabases(context.Context, interface{}, ...*options.ListDatabasesOptions) (mongo.ListDatabasesResult, error) {
	panic("not implemented")
}

func (c *AltClient) Ping(context.Context, *readpref.ReadPref) error {
	return nil
}

func (c *AltClient) StartSession(...*options.SessionOptions) (mongo.Session, error) {
	panic("not implemented")
}

func (c *AltClient) UseSession(context.Context, func(mongo.SessionContext) error) error {
	panic("not implemented")
}

func (c *AltClient) UseSessionWithOptions(context.Context, *options.SessionOptions, func(mongo.SessionContext) error) error {
	panic("not implemented")
}

func (c *AltClient) Watch(context.Context, interface{}, ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error) {
	panic("not implemented")
}
