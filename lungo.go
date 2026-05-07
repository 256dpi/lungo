package lungo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// IClient defines a generic client.
type IClient interface {
	Database(string, ...options.Lister[options.DatabaseOptions]) IDatabase
	Disconnect(context.Context) error
	ListDatabaseNames(context.Context, interface{}, ...options.Lister[options.ListDatabasesOptions]) ([]string, error)
	ListDatabases(context.Context, interface{}, ...options.Lister[options.ListDatabasesOptions]) (mongo.ListDatabasesResult, error)
	NumberSessionsInProgress() int
	Ping(context.Context, *readpref.ReadPref) error
	StartSession(...options.Lister[options.SessionOptions]) (ISession, error)
	UseSession(context.Context, func(ISessionContext) error) error
	UseSessionWithOptions(context.Context, options.Lister[options.SessionOptions], func(ISessionContext) error) error
	Watch(context.Context, interface{}, ...options.Lister[options.ChangeStreamOptions]) (IChangeStream, error)
}

// IDatabase defines a generic database.
type IDatabase interface {
	Aggregate(context.Context, interface{}, ...options.Lister[options.AggregateOptions]) (ICursor, error)
	Client() IClient
	Collection(string, ...options.Lister[options.CollectionOptions]) ICollection
	CreateCollection(context.Context, string, ...options.Lister[options.CreateCollectionOptions]) error
	CreateView(context.Context, string, string, interface{}, ...options.Lister[options.CreateViewOptions]) error
	Drop(context.Context) error
	ListCollectionNames(context.Context, interface{}, ...options.Lister[options.ListCollectionsOptions]) ([]string, error)
	ListCollectionSpecifications(context.Context, interface{}, ...options.Lister[options.ListCollectionsOptions]) ([]mongo.CollectionSpecification, error)
	ListCollections(context.Context, interface{}, ...options.Lister[options.ListCollectionsOptions]) (ICursor, error)
	Name() string
	RunCommand(context.Context, interface{}, ...options.Lister[options.RunCmdOptions]) ISingleResult
	RunCommandCursor(context.Context, interface{}, ...options.Lister[options.RunCmdOptions]) (ICursor, error)
	Watch(context.Context, interface{}, ...options.Lister[options.ChangeStreamOptions]) (IChangeStream, error)
}

// ICollection defines a generic collection.
type ICollection interface {
	Aggregate(context.Context, interface{}, ...options.Lister[options.AggregateOptions]) (ICursor, error)
	BulkWrite(context.Context, []mongo.WriteModel, ...options.Lister[options.BulkWriteOptions]) (*mongo.BulkWriteResult, error)
	Clone(...options.Lister[options.CollectionOptions]) ICollection
	CountDocuments(context.Context, interface{}, ...options.Lister[options.CountOptions]) (int64, error)
	Database() IDatabase
	DeleteMany(context.Context, interface{}, ...options.Lister[options.DeleteManyOptions]) (*mongo.DeleteResult, error)
	DeleteOne(context.Context, interface{}, ...options.Lister[options.DeleteOneOptions]) (*mongo.DeleteResult, error)
	Distinct(context.Context, string, interface{}, ...options.Lister[options.DistinctOptions]) ([]interface{}, error)
	Drop(context.Context, ...options.Lister[options.DropCollectionOptions]) error
	EstimatedDocumentCount(context.Context, ...options.Lister[options.EstimatedDocumentCountOptions]) (int64, error)
	Find(context.Context, interface{}, ...options.Lister[options.FindOptions]) (ICursor, error)
	FindOne(context.Context, interface{}, ...options.Lister[options.FindOneOptions]) ISingleResult
	FindOneAndDelete(context.Context, interface{}, ...options.Lister[options.FindOneAndDeleteOptions]) ISingleResult
	FindOneAndReplace(context.Context, interface{}, interface{}, ...options.Lister[options.FindOneAndReplaceOptions]) ISingleResult
	FindOneAndUpdate(context.Context, interface{}, interface{}, ...options.Lister[options.FindOneAndUpdateOptions]) ISingleResult
	Indexes() IIndexView
	InsertMany(context.Context, interface{}, ...options.Lister[options.InsertManyOptions]) (*mongo.InsertManyResult, error)
	InsertOne(context.Context, interface{}, ...options.Lister[options.InsertOneOptions]) (*mongo.InsertOneResult, error)
	Name() string
	ReplaceOne(context.Context, interface{}, interface{}, ...options.Lister[options.ReplaceOptions]) (*mongo.UpdateResult, error)
	SearchIndexes() mongo.SearchIndexView
	UpdateByID(context.Context, interface{}, interface{}, ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, error)
	UpdateMany(context.Context, interface{}, interface{}, ...options.Lister[options.UpdateManyOptions]) (*mongo.UpdateResult, error)
	UpdateOne(context.Context, interface{}, interface{}, ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, error)
	Watch(context.Context, interface{}, ...options.Lister[options.ChangeStreamOptions]) (IChangeStream, error)
}

// ICursor defines a generic cursor.
type ICursor interface {
	All(context.Context, interface{}) error
	Close(context.Context) error
	Decode(interface{}) error
	Err() error
	ID() int64
	Next(context.Context) bool
	RemainingBatchLength() int
	SetBatchSize(batchSize int32)
	SetComment(interface{})
	SetMaxAwaitTime(time.Duration)
	TryNext(context.Context) bool
}

// ISingleResult defines a generic single result
type ISingleResult interface {
	Decode(interface{}) error
	Err() error
	Raw() (bson.Raw, error)
}

// IIndexView defines a generic index view.
type IIndexView interface {
	CreateMany(context.Context, []mongo.IndexModel, ...options.Lister[options.CreateIndexesOptions]) ([]string, error)
	CreateOne(context.Context, mongo.IndexModel, ...options.Lister[options.CreateIndexesOptions]) (string, error)
	DropAll(context.Context, ...options.Lister[options.DropIndexesOptions]) error
	DropOne(context.Context, string, ...options.Lister[options.DropIndexesOptions]) error
	DropWithKey(context.Context, interface{}, ...options.Lister[options.DropIndexesOptions]) error
	List(context.Context, ...options.Lister[options.ListIndexesOptions]) (ICursor, error)
	ListSpecifications(context.Context, ...options.Lister[options.ListIndexesOptions]) ([]mongo.IndexSpecification, error)
}

// IChangeStream defines a generic change stream.
type IChangeStream interface {
	Close(context.Context) error
	Decode(interface{}) error
	Err() error
	ID() int64
	Next(context.Context) bool
	RemainingBatchLength() int
	ResumeToken() bson.Raw
	SetBatchSize(int32)
	TryNext(context.Context) bool
}

// ISession defines a generic session.
type ISession interface {
	ID() bson.Raw
	AbortTransaction(context.Context) error
	AdvanceClusterTime(bson.Raw) error
	AdvanceOperationTime(*bson.Timestamp) error
	Client() IClient
	ClusterTime() bson.Raw
	CommitTransaction(context.Context) error
	EndSession(context.Context)
	OperationTime() *bson.Timestamp
	StartTransaction(...options.Lister[options.TransactionOptions]) error
	WithTransaction(context.Context, func(ISessionContext) (interface{}, error), ...options.Lister[options.TransactionOptions]) (interface{}, error)
}

// ISessionContext defines a generic session context.
type ISessionContext interface {
	context.Context
	ISession
}

// WithSession will yield a session context to the provided callback that uses
// the specified session.
func WithSession(ctx context.Context, session ISession, fn func(ISessionContext) error) error {
	switch ses := session.(type) {
	case *MongoSession:
		return mongo.WithSession(ensureContext(ctx), ses.Session, func(c context.Context) error {
			return fn(&MongoSessionContext{
				Context:      c,
				MongoSession: ses,
			})
		})
	case *Session:
		return fn(&SessionContext{
			Context: context.WithValue(ensureContext(ctx), sessionKey{}, ses),
			Session: ses,
		})
	default:
		return fmt.Errorf("unknown session %T", session)
	}
}
