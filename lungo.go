package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

// IClient defines a generic client.
type IClient interface {
	Connect(context.Context) error
	Database(string, ...*options.DatabaseOptions) IDatabase
	Disconnect(context.Context) error
	ListDatabaseNames(context.Context, interface{}, ...*options.ListDatabasesOptions) ([]string, error)
	ListDatabases(context.Context, interface{}, ...*options.ListDatabasesOptions) (mongo.ListDatabasesResult, error)
	Ping(context.Context, *readpref.ReadPref) error
	StartSession(...*options.SessionOptions) (ISession, error)
	UseSession(context.Context, func(ISessionContext) error) error
	UseSessionWithOptions(context.Context, *options.SessionOptions, func(ISessionContext) error) error
	Watch(context.Context, interface{}, ...*options.ChangeStreamOptions) (IChangeStream, error)
}

// IDatabase defines a generic database.
type IDatabase interface {
	Aggregate(context.Context, interface{}, ...*options.AggregateOptions) (ICursor, error)
	Client() IClient
	Collection(string, ...*options.CollectionOptions) ICollection
	Drop(context.Context) error
	ListCollectionNames(context.Context, interface{}, ...*options.ListCollectionsOptions) ([]string, error)
	ListCollections(context.Context, interface{}, ...*options.ListCollectionsOptions) (ICursor, error)
	Name() string
	ReadConcern() *readconcern.ReadConcern
	ReadPreference() *readpref.ReadPref
	RunCommand(context.Context, interface{}, ...*options.RunCmdOptions) ISingleResult
	RunCommandCursor(context.Context, interface{}, ...*options.RunCmdOptions) (ICursor, error)
	Watch(context.Context, interface{}, ...*options.ChangeStreamOptions) (IChangeStream, error)
	WriteConcern() *writeconcern.WriteConcern
}

// ICollection defines a generic collection.
type ICollection interface {
	Aggregate(context.Context, interface{}, ...*options.AggregateOptions) (ICursor, error)
	BulkWrite(context.Context, []mongo.WriteModel, ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error)
	Clone(...*options.CollectionOptions) (ICollection, error)
	CountDocuments(context.Context, interface{}, ...*options.CountOptions) (int64, error)
	Database() IDatabase
	DeleteMany(context.Context, interface{}, ...*options.DeleteOptions) (*mongo.DeleteResult, error)
	DeleteOne(context.Context, interface{}, ...*options.DeleteOptions) (*mongo.DeleteResult, error)
	Distinct(context.Context, string, interface{}, ...*options.DistinctOptions) ([]interface{}, error)
	Drop(context.Context) error
	EstimatedDocumentCount(context.Context, ...*options.EstimatedDocumentCountOptions) (int64, error)
	Find(context.Context, interface{}, ...*options.FindOptions) (ICursor, error)
	FindOne(context.Context, interface{}, ...*options.FindOneOptions) ISingleResult
	FindOneAndDelete(context.Context, interface{}, ...*options.FindOneAndDeleteOptions) ISingleResult
	FindOneAndReplace(context.Context, interface{}, interface{}, ...*options.FindOneAndReplaceOptions) ISingleResult
	FindOneAndUpdate(context.Context, interface{}, interface{}, ...*options.FindOneAndUpdateOptions) ISingleResult
	Indexes() IIndexView
	InsertMany(context.Context, []interface{}, ...*options.InsertManyOptions) (*mongo.InsertManyResult, error)
	InsertOne(context.Context, interface{}, ...*options.InsertOneOptions) (*mongo.InsertOneResult, error)
	Name() string
	ReplaceOne(context.Context, interface{}, interface{}, ...*options.ReplaceOptions) (*mongo.UpdateResult, error)
	UpdateMany(context.Context, interface{}, interface{}, ...*options.UpdateOptions) (*mongo.UpdateResult, error)
	UpdateOne(context.Context, interface{}, interface{}, ...*options.UpdateOptions) (*mongo.UpdateResult, error)
	Watch(context.Context, interface{}, ...*options.ChangeStreamOptions) (IChangeStream, error)
}

// ICursor defines a generic cursor.
type ICursor interface {
	All(context.Context, interface{}) error
	Close(context.Context) error
	Decode(interface{}) error
	Err() error
	ID() int64
	Next(context.Context) bool
}

// ISingleResult defines a generic single result
type ISingleResult interface {
	Decode(interface{}) error
	DecodeBytes() (bson.Raw, error)
	Err() error
}

// IIndexView defines a generic index view.
type IIndexView interface {
	CreateMany(context.Context, []mongo.IndexModel, ...*options.CreateIndexesOptions) ([]string, error)
	CreateOne(context.Context, mongo.IndexModel, ...*options.CreateIndexesOptions) (string, error)
	DropAll(context.Context, ...*options.DropIndexesOptions) (bson.Raw, error)
	DropOne(context.Context, string, ...*options.DropIndexesOptions) (bson.Raw, error)
	List(context.Context, ...*options.ListIndexesOptions) (ICursor, error)
}

// IChangeStream defines a generic change stream.
type IChangeStream interface {
	Close(context.Context) error
	Decode(interface{}) error
	Err() error
	ID() int64
	Next(context.Context) bool
	ResumeToken() bson.Raw
}

// ISession defines a generic session.
type ISession interface {
	AbortTransaction(context.Context) error
	AdvanceClusterTime(bson.Raw) error
	AdvanceOperationTime(*primitive.Timestamp) error
	Client() IClient
	ClusterTime() bson.Raw
	CommitTransaction(context.Context) error
	EndSession(context.Context)
	OperationTime() *primitive.Timestamp
	StartTransaction(...*options.TransactionOptions) error
	WithTransaction(ctx context.Context, fn func(sessCtx ISessionContext) (interface{}, error), opts ...*options.TransactionOptions) (interface{}, error)
}

// ISessionContext defines a generic session context.
type ISessionContext interface {
	context.Context
	ISession
}
