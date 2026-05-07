package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var _ IClient = &MongoClient{}

// MongoClient wraps a mongo.Client to be lungo compatible.
type MongoClient struct {
	*mongo.Client
}

// Connect will connect to a MongoDB database and return a lungo compatible client.
func Connect(_ context.Context, opts ...*options.ClientOptions) (IClient, error) {
	client, err := mongo.Connect(opts...)
	if err != nil {
		return nil, err
	}

	return &MongoClient{Client: client}, nil
}

// Database implements the IClient.Database method.
func (c *MongoClient) Database(name string, opts ...options.Lister[options.DatabaseOptions]) IDatabase {
	return &MongoDatabase{Database: c.Client.Database(name, opts...), client: c}
}

// ListDatabaseNames implements the IClient.ListDatabaseNames method.
func (c *MongoClient) ListDatabaseNames(ctx context.Context, filter interface{}, opts ...options.Lister[options.ListDatabasesOptions]) ([]string, error) {
	return c.Client.ListDatabaseNames(ctx, filter, opts...)
}

// ListDatabases implements the IClient.ListDatabases method.
func (c *MongoClient) ListDatabases(ctx context.Context, filter interface{}, opts ...options.Lister[options.ListDatabasesOptions]) (mongo.ListDatabasesResult, error) {
	return c.Client.ListDatabases(ctx, filter, opts...)
}

// StartSession implements the IClient.StartSession method.
func (c *MongoClient) StartSession(opts ...options.Lister[options.SessionOptions]) (ISession, error) {
	session, err := c.Client.StartSession(opts...)
	if err != nil {
		return nil, err
	}

	return &MongoSession{Session: session, client: c}, nil
}

// UseSession implements the IClient.UseSession method.
func (c *MongoClient) UseSession(ctx context.Context, fn func(ISessionContext) error) error {
	return c.UseSessionWithOptions(ctx, options.Session(), fn)
}

// UseSessionWithOptions implements the IClient.UseSessionWithOptions method.
func (c *MongoClient) UseSessionWithOptions(ctx context.Context, opt options.Lister[options.SessionOptions], fn func(ISessionContext) error) error {
	sess, err := c.StartSession(opt)
	if err != nil {
		return err
	}
	defer sess.EndSession(ctx)

	return WithSession(ctx, sess, fn)
}

// Watch implements the IClient.Watch method.
func (c *MongoClient) Watch(ctx context.Context, pipeline interface{}, opts ...options.Lister[options.ChangeStreamOptions]) (IChangeStream, error) {
	return c.Client.Watch(ctx, pipeline, opts...)
}

var _ IDatabase = &MongoDatabase{}

// MongoDatabase wraps a mongo.Database to be lungo compatible.
type MongoDatabase struct {
	*mongo.Database

	client *MongoClient
}

// Aggregate implements the IDatabase.Aggregate method.
func (d *MongoDatabase) Aggregate(ctx context.Context, pipeline interface{}, opts ...options.Lister[options.AggregateOptions]) (ICursor, error) {
	return d.Database.Aggregate(ctx, pipeline, opts...)
}

// Client implements the IDatabase.Client method.
func (d *MongoDatabase) Client() IClient {
	return d.client
}

// Collection implements the IDatabase.Collection method.
func (d *MongoDatabase) Collection(name string, opts ...options.Lister[options.CollectionOptions]) ICollection {
	return &MongoCollection{Collection: d.Database.Collection(name, opts...), db: d}
}

// CreateCollection implements the IDatabase.CreateCollection method.
func (d *MongoDatabase) CreateCollection(ctx context.Context, name string, opts ...options.Lister[options.CreateCollectionOptions]) error {
	return d.Database.CreateCollection(ensureContext(ctx), name, opts...)
}

// CreateView implements the IDatabase.CreateView method.
func (d *MongoDatabase) CreateView(ctx context.Context, viewName, viewOn string, pipeline interface{}, opts ...options.Lister[options.CreateViewOptions]) error {
	return d.Database.CreateView(ensureContext(ctx), viewName, viewOn, pipeline, opts...)
}

// ListCollectionNames implements the IDatabase.ListCollectionNames method.
func (d *MongoDatabase) ListCollectionNames(ctx context.Context, filter interface{}, opts ...options.Lister[options.ListCollectionsOptions]) ([]string, error) {
	return d.Database.ListCollectionNames(ctx, filter, opts...)
}

// ListCollectionSpecifications implements the IDatabase.ListCollectionSpecifications method.
func (d *MongoDatabase) ListCollectionSpecifications(ctx context.Context, filter interface{}, opts ...options.Lister[options.ListCollectionsOptions]) ([]mongo.CollectionSpecification, error) {
	return d.Database.ListCollectionSpecifications(ctx, filter, opts...)
}

// ListCollections implements the IDatabase.ListCollections method.
func (d *MongoDatabase) ListCollections(ctx context.Context, filter interface{}, opts ...options.Lister[options.ListCollectionsOptions]) (ICursor, error) {
	return d.Database.ListCollections(ctx, filter, opts...)
}

// RunCommand implements the IDatabase.RunCommand method.
func (d *MongoDatabase) RunCommand(ctx context.Context, runCommand interface{}, opts ...options.Lister[options.RunCmdOptions]) ISingleResult {
	return d.Database.RunCommand(ctx, runCommand, opts...)
}

// RunCommandCursor implements the IDatabase.RunCommandCursor method.
func (d *MongoDatabase) RunCommandCursor(ctx context.Context, filter interface{}, opts ...options.Lister[options.RunCmdOptions]) (ICursor, error) {
	return d.Database.RunCommandCursor(ctx, filter, opts...)
}

// Watch implements the IDatabase.Watch method.
func (d *MongoDatabase) Watch(ctx context.Context, pipeline interface{}, opts ...options.Lister[options.ChangeStreamOptions]) (IChangeStream, error) {
	return d.Database.Watch(ctx, pipeline, opts...)
}

var _ ICollection = &MongoCollection{}

// MongoCollection wraps a mongo.Collection to be lungo compatible.
type MongoCollection struct {
	*mongo.Collection

	db *MongoDatabase
}

// Aggregate implements the ICollection.Aggregate method.
func (c *MongoCollection) Aggregate(ctx context.Context, pipeline interface{}, opts ...options.Lister[options.AggregateOptions]) (ICursor, error) {
	return c.Collection.Aggregate(ctx, pipeline, opts...)
}

// BulkWrite implements the ICollection.BulkWrite method.
func (c *MongoCollection) BulkWrite(ctx context.Context, models []mongo.WriteModel, opts ...options.Lister[options.BulkWriteOptions]) (*mongo.BulkWriteResult, error) {
	return c.Collection.BulkWrite(ctx, models, opts...)
}

// Clone implements the ICollection.Clone method.
func (c *MongoCollection) Clone(opts ...options.Lister[options.CollectionOptions]) ICollection {
	coll := c.Collection.Clone(opts...)
	return &MongoCollection{Collection: coll, db: c.db}
}

// CountDocuments implements the ICollection.CountDocuments method.
func (c *MongoCollection) CountDocuments(ctx context.Context, filter interface{}, opts ...options.Lister[options.CountOptions]) (int64, error) {
	return c.Collection.CountDocuments(ctx, filter, opts...)
}

// Database implements the ICollection.Database method.
func (c *MongoCollection) Database() IDatabase {
	return c.db
}

// DeleteMany implements the ICollection.DeleteMany method.
func (c *MongoCollection) DeleteMany(ctx context.Context, filter interface{}, opts ...options.Lister[options.DeleteManyOptions]) (*mongo.DeleteResult, error) {
	return c.Collection.DeleteMany(ctx, filter, opts...)
}

// DeleteOne implements the ICollection.DeleteOne method.
func (c *MongoCollection) DeleteOne(ctx context.Context, filter interface{}, opts ...options.Lister[options.DeleteOneOptions]) (*mongo.DeleteResult, error) {
	return c.Collection.DeleteOne(ctx, filter, opts...)
}

// Distinct implements the ICollection.Distinct method.
func (c *MongoCollection) Distinct(ctx context.Context, fieldName string, filter interface{}, opts ...options.Lister[options.DistinctOptions]) ([]interface{}, error) {
	res := c.Collection.Distinct(ctx, fieldName, filter, opts...)
	if err := res.Err(); err != nil {
		return nil, err
	}
	var values []interface{}
	if err := res.Decode(&values); err != nil {
		return nil, err
	}
	return values, nil
}

// EstimatedDocumentCount implements the ICollection.EstimatedDocumentCount method.
func (c *MongoCollection) EstimatedDocumentCount(ctx context.Context, opts ...options.Lister[options.EstimatedDocumentCountOptions]) (int64, error) {
	return c.Collection.EstimatedDocumentCount(ctx, opts...)
}

// Find implements the ICollection.Find method.
func (c *MongoCollection) Find(ctx context.Context, filter interface{}, opts ...options.Lister[options.FindOptions]) (ICursor, error) {
	return c.Collection.Find(ctx, filter, opts...)
}

// FindOne implements the ICollection.FindOne method.
func (c *MongoCollection) FindOne(ctx context.Context, filter interface{}, opts ...options.Lister[options.FindOneOptions]) ISingleResult {
	return c.Collection.FindOne(ctx, filter, opts...)
}

// FindOneAndDelete implements the ICollection.FindOneAndDelete method.
func (c *MongoCollection) FindOneAndDelete(ctx context.Context, filter interface{}, opts ...options.Lister[options.FindOneAndDeleteOptions]) ISingleResult {
	return c.Collection.FindOneAndDelete(ctx, filter, opts...)
}

// FindOneAndReplace implements the ICollection.FindOneAndReplace method.
func (c *MongoCollection) FindOneAndReplace(ctx context.Context, filter, replacement interface{}, opts ...options.Lister[options.FindOneAndReplaceOptions]) ISingleResult {
	return c.Collection.FindOneAndReplace(ctx, filter, replacement, opts...)
}

// FindOneAndUpdate implements the ICollection.FindOneAndUpdate method.
func (c *MongoCollection) FindOneAndUpdate(ctx context.Context, filter, update interface{}, opts ...options.Lister[options.FindOneAndUpdateOptions]) ISingleResult {
	return c.Collection.FindOneAndUpdate(ctx, filter, update, opts...)
}

// Indexes implements the ICollection.Indexes method.
func (c *MongoCollection) Indexes() IIndexView {
	return &MongoIndexView{
		IndexView: c.Collection.Indexes(),
	}
}

// InsertMany implements the ICollection.InsertMany method.
func (c *MongoCollection) InsertMany(ctx context.Context, documents interface{}, opts ...options.Lister[options.InsertManyOptions]) (*mongo.InsertManyResult, error) {
	return c.Collection.InsertMany(ctx, documents, opts...)
}

// InsertOne implements the ICollection.InsertOne method.
func (c *MongoCollection) InsertOne(ctx context.Context, document interface{}, opts ...options.Lister[options.InsertOneOptions]) (*mongo.InsertOneResult, error) {
	return c.Collection.InsertOne(ctx, document, opts...)
}

// ReplaceOne implements the ICollection.ReplaceOne method.
func (c *MongoCollection) ReplaceOne(ctx context.Context, filter, replacement interface{}, opts ...options.Lister[options.ReplaceOptions]) (*mongo.UpdateResult, error) {
	return c.Collection.ReplaceOne(ctx, filter, replacement, opts...)
}

// UpdateByID implements the ICollection.UpdateByID method.
func (c *MongoCollection) UpdateByID(ctx context.Context, id, update interface{}, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, error) {
	return c.Collection.UpdateByID(ctx, id, update, opts...)
}

// UpdateMany implements the ICollection.UpdateMany method.
func (c *MongoCollection) UpdateMany(ctx context.Context, filter, update interface{}, opts ...options.Lister[options.UpdateManyOptions]) (*mongo.UpdateResult, error) {
	return c.Collection.UpdateMany(ctx, filter, update, opts...)
}

// UpdateOne implements the ICollection.UpdateOne method.
func (c *MongoCollection) UpdateOne(ctx context.Context, filter, update interface{}, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, error) {
	return c.Collection.UpdateOne(ctx, filter, update, opts...)
}

// Watch implements the ICollection.Watch method.
func (c *MongoCollection) Watch(ctx context.Context, pipeline interface{}, opts ...options.Lister[options.ChangeStreamOptions]) (IChangeStream, error) {
	return c.Collection.Watch(ctx, pipeline, opts...)
}

// Drop implements the ICollection.Drop method.
func (c *MongoCollection) Drop(ctx context.Context, opts ...options.Lister[options.DropCollectionOptions]) error {
	return c.Collection.Drop(ctx, opts...)
}

var _ IIndexView = &MongoIndexView{}

// MongoIndexView wraps a mongo.IndexView to be lungo compatible.
type MongoIndexView struct {
	mongo.IndexView
}

// CreateMany implements the IIndexView.CreateMany method.
func (m *MongoIndexView) CreateMany(ctx context.Context, models []mongo.IndexModel, opts ...options.Lister[options.CreateIndexesOptions]) ([]string, error) {
	return m.IndexView.CreateMany(ensureContext(ctx), models, opts...)
}

// CreateOne implements the IIndexView.CreateOne method.
func (m *MongoIndexView) CreateOne(ctx context.Context, model mongo.IndexModel, opts ...options.Lister[options.CreateIndexesOptions]) (string, error) {
	return m.IndexView.CreateOne(ensureContext(ctx), model, opts...)
}

// DropAll implements the IIndexView.DropAll method.
func (m *MongoIndexView) DropAll(ctx context.Context, opts ...options.Lister[options.DropIndexesOptions]) error {
	return m.IndexView.DropAll(ensureContext(ctx), opts...)
}

// DropOne implements the IIndexView.DropOne method.
func (m *MongoIndexView) DropOne(ctx context.Context, name string, opts ...options.Lister[options.DropIndexesOptions]) error {
	return m.IndexView.DropOne(ensureContext(ctx), name, opts...)
}

// DropWithKey implements the IIndexView.DropWithKey method.
func (m *MongoIndexView) DropWithKey(ctx context.Context, keySpec interface{}, opts ...options.Lister[options.DropIndexesOptions]) error {
	return m.IndexView.DropWithKey(ensureContext(ctx), keySpec, opts...)
}

// List implements the IIndexView.List method.
func (m *MongoIndexView) List(ctx context.Context, opts ...options.Lister[options.ListIndexesOptions]) (ICursor, error) {
	return m.IndexView.List(ctx, opts...)
}

// ListSpecifications implements the IIndexView.ListSpecifications method.
func (m *MongoIndexView) ListSpecifications(ctx context.Context, opts ...options.Lister[options.ListIndexesOptions]) ([]mongo.IndexSpecification, error) {
	return m.IndexView.ListSpecifications(ctx, opts...)
}

var _ ISession = &MongoSession{}

// MongoSession wraps a mongo.Session to be lungo compatible.
type MongoSession struct {
	*mongo.Session

	client *MongoClient
}

// AbortTransaction implements the ISession.AbortTransaction method.
func (s *MongoSession) AbortTransaction(ctx context.Context) error {
	return s.Session.AbortTransaction(ensureContext(ctx))
}

// Client implements the ISession.Client method.
func (s *MongoSession) Client() IClient {
	return s.client
}

// CommitTransaction implements the ISession.CommitTransaction method.
func (s *MongoSession) CommitTransaction(ctx context.Context) error {
	return s.Session.CommitTransaction(ensureContext(ctx))
}

// EndSession implements the ISession.EndSession method.
func (s *MongoSession) EndSession(ctx context.Context) {
	s.Session.EndSession(ensureContext(ctx))
}

// StartTransaction implements the ISession.StartTransaction method.
func (s *MongoSession) StartTransaction(opts ...options.Lister[options.TransactionOptions]) error {
	return s.Session.StartTransaction(opts...)
}

// WithTransaction implements the ISession.WithTransaction method.
func (s *MongoSession) WithTransaction(ctx context.Context, fn func(ISessionContext) (interface{}, error), opts ...options.Lister[options.TransactionOptions]) (interface{}, error) {
	return s.Session.WithTransaction(ensureContext(ctx), func(sc context.Context) (interface{}, error) {
		return fn(&MongoSessionContext{
			Context: sc,
			MongoSession: &MongoSession{
				Session: mongo.SessionFromContext(sc),
				client:  s.client,
			},
		})
	}, opts...)
}

var _ ISessionContext = &MongoSessionContext{}

// MongoSessionContext wraps a context that carries a mongo.Session to be lungo
// compatible.
type MongoSessionContext struct {
	context.Context
	*MongoSession
}
