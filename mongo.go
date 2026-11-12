package lungo

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

var _ IClient = &MongoClient{}

// MongoClient wraps a mongo.Client to be lungo compatible.
type MongoClient struct {
	client *mongo.Client
}

// Connect will connect to a MongoDB database and return a lungo compatible client.
func Connect(ctx context.Context, opts ...*options.ClientOptions) (IClient, error) {
	_ = ctx
	client, err := mongo.Connect(opts...)
	if err != nil {
		return nil, err
	}
	return &MongoClient{client: client}, nil
}

func (c *MongoClient) Connect(ctx context.Context) error {
	return c.client.Ping(ensureContext(ctx), readpref.Primary())
}

func (c *MongoClient) Database(name string, opts ...*options.DatabaseOptions) IDatabase {
	return &MongoDatabase{db: c.client.Database(name, optionListers(opts...)...), client: c}
}

func (c *MongoClient) Disconnect(ctx context.Context) error {
	return c.client.Disconnect(ensureContext(ctx))
}

func (c *MongoClient) ListDatabaseNames(ctx context.Context, filter interface{}, opts ...*options.ListDatabasesOptions) ([]string, error) {
	return c.client.ListDatabaseNames(ensureContext(ctx), filter, optionListers(opts...)...)
}

func (c *MongoClient) ListDatabases(ctx context.Context, filter interface{}, opts ...*options.ListDatabasesOptions) (mongo.ListDatabasesResult, error) {
	return c.client.ListDatabases(ensureContext(ctx), filter, optionListers(opts...)...)
}

func (c *MongoClient) NumberSessionsInProgress() int {
	return c.client.NumberSessionsInProgress()
}

func (c *MongoClient) Ping(ctx context.Context, rp *readpref.ReadPref) error {
	return c.client.Ping(ensureContext(ctx), rp)
}

func (c *MongoClient) StartSession(opts ...*options.SessionOptions) (ISession, error) {
	list := optionListers(opts...)
	if len(list) == 0 {
		list = []options.Lister[options.SessionOptions]{options.Session()}
	}
	session, err := c.client.StartSession(list...)
	if err != nil {
		return nil, err
	}
	return &MongoSession{sess: session, client: c}, nil
}

func (c *MongoClient) Timeout() *time.Duration {
	return nil
}

func (c *MongoClient) UseSession(ctx context.Context, fn func(ISessionContext) error) error {
	return c.UseSessionWithOptions(ctx, nil, fn)
}

func (c *MongoClient) UseSessionWithOptions(ctx context.Context, opt *options.SessionOptions, fn func(ISessionContext) error) error {
	list := optionListers(opt)
	if len(list) == 0 {
		list = []options.Lister[options.SessionOptions]{options.Session()}
	}
	sess, err := c.client.StartSession(list...)
	if err != nil {
		return err
	}
	defer sess.EndSession(ensureContext(ctx))
	return fn(&MongoSessionContext{
		Context: mongo.NewSessionContext(ensureContext(ctx), sess),
		MongoSession: &MongoSession{
			sess:   sess,
			client: c,
		},
	})
}

func (c *MongoClient) Watch(ctx context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (IChangeStream, error) {
	return c.client.Watch(ensureContext(ctx), pipeline, optionListers(opts...)...)
}

var _ IDatabase = &MongoDatabase{}

type MongoDatabase struct {
	db     *mongo.Database
	client *MongoClient
}

func (d *MongoDatabase) Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (ICursor, error) {
	c, err := d.db.Aggregate(ensureContext(ctx), pipeline, optionListers(opts...)...)
	return wrapCursor(c), err
}

func (d *MongoDatabase) Client() IClient { return d.client }

func (d *MongoDatabase) Collection(name string, opts ...*options.CollectionOptions) ICollection {
	return &MongoCollection{coll: d.db.Collection(name, optionListers(opts...)...), db: d}
}

func (d *MongoDatabase) CreateCollection(ctx context.Context, name string, opts ...*options.CreateCollectionOptions) error {
	return d.db.CreateCollection(ensureContext(ctx), name, optionListers(opts...)...)
}

func (d *MongoDatabase) CreateView(ctx context.Context, view, source string, pipeline interface{}, opts ...*options.CreateViewOptions) error {
	return d.db.CreateView(ensureContext(ctx), view, source, pipeline, optionListers(opts...)...)
}

func (d *MongoDatabase) Drop(ctx context.Context) error {
	return d.db.Drop(ensureContext(ctx))
}

func (d *MongoDatabase) ListCollectionNames(ctx context.Context, filter interface{}, opts ...*options.ListCollectionsOptions) ([]string, error) {
	return d.db.ListCollectionNames(ensureContext(ctx), filter, optionListers(opts...)...)
}

func (d *MongoDatabase) ListCollectionSpecifications(ctx context.Context, filter interface{}, opts ...*options.ListCollectionsOptions) ([]*mongo.CollectionSpecification, error) {
	specs, err := d.db.ListCollectionSpecifications(ensureContext(ctx), filter, optionListers(opts...)...)
	if err != nil {
		return nil, err
	}
	out := make([]*mongo.CollectionSpecification, len(specs))
	for i := range specs {
		spec := specs[i]
		out[i] = &spec
	}
	return out, nil
}

func (d *MongoDatabase) ListCollections(ctx context.Context, filter interface{}, opts ...*options.ListCollectionsOptions) (ICursor, error) {
	c, err := d.db.ListCollections(ensureContext(ctx), filter, optionListers(opts...)...)
	return wrapCursor(c), err
}

func (d *MongoDatabase) Name() string { return d.db.Name() }

func (d *MongoDatabase) ReadConcern() *readconcern.ReadConcern { return readconcern.Local() }

func (d *MongoDatabase) ReadPreference() *readpref.ReadPref { return readpref.Primary() }

func (d *MongoDatabase) RunCommand(ctx context.Context, runCommand interface{}, opts ...*options.RunCmdOptions) ISingleResult {
	return wrapSingleResult(d.db.RunCommand(ensureContext(ctx), runCommand, optionListers(opts...)...))
}

func (d *MongoDatabase) RunCommandCursor(ctx context.Context, runCommand interface{}, opts ...*options.RunCmdOptions) (ICursor, error) {
	c, err := d.db.RunCommandCursor(ensureContext(ctx), runCommand, optionListers(opts...)...)
	return wrapCursor(c), err
}

func (d *MongoDatabase) Watch(ctx context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (IChangeStream, error) {
	return d.db.Watch(ensureContext(ctx), pipeline, optionListers(opts...)...)
}

func (d *MongoDatabase) WriteConcern() *writeconcern.WriteConcern { return writeconcern.Majority() }

var _ ICollection = &MongoCollection{}

type MongoCollection struct {
	coll *mongo.Collection
	db   *MongoDatabase
}

func (c *MongoCollection) Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (ICursor, error) {
	cur, err := c.coll.Aggregate(ensureContext(ctx), pipeline, optionListers(opts...)...)
	return wrapCursor(cur), err
}

func (c *MongoCollection) BulkWrite(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
	return c.coll.BulkWrite(ensureContext(ctx), models, optionListers(opts...)...)
}

func (c *MongoCollection) Clone(opts ...*options.CollectionOptions) (ICollection, error) {
	cloned := c.coll.Clone(optionListers(opts...)...)
	return &MongoCollection{coll: cloned, db: c.db}, nil
}

func (c *MongoCollection) CountDocuments(ctx context.Context, filter interface{}, opts ...*options.CountOptions) (int64, error) {
	return c.coll.CountDocuments(ensureContext(ctx), filter, optionListers(opts...)...)
}

func (c *MongoCollection) Database() IDatabase { return c.db }

func (c *MongoCollection) DeleteMany(ctx context.Context, filter interface{}, opts ...*DeleteOptions) (*mongo.DeleteResult, error) {
	return c.coll.DeleteMany(ensureContext(ctx), filter, deleteManyListers(opts...)...)
}

func (c *MongoCollection) DeleteOne(ctx context.Context, filter interface{}, opts ...*DeleteOptions) (*mongo.DeleteResult, error) {
	return c.coll.DeleteOne(ensureContext(ctx), filter, optionListers(opts...)...)
}

func (c *MongoCollection) Distinct(ctx context.Context, field string, filter interface{}, opts ...*options.DistinctOptions) ([]interface{}, error) {
	res := c.coll.Distinct(ensureContext(ctx), field, filter, optionListers(opts...)...)
	if err := res.Err(); err != nil {
		return nil, err
	}
	var values []interface{}
	if err := res.Decode(&values); err != nil {
		return nil, err
	}
	return values, nil
}

func (c *MongoCollection) Drop(ctx context.Context) error {
	return c.coll.Drop(ensureContext(ctx))
}

func (c *MongoCollection) EstimatedDocumentCount(ctx context.Context, opts ...*options.EstimatedDocumentCountOptions) (int64, error) {
	return c.coll.EstimatedDocumentCount(ensureContext(ctx), optionListers(opts...)...)
}

func (c *MongoCollection) Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (ICursor, error) {
	cur, err := c.coll.Find(ensureContext(ctx), filter, optionListers(opts...)...)
	return wrapCursor(cur), err
}

func (c *MongoCollection) FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) ISingleResult {
	return wrapSingleResult(c.coll.FindOne(ensureContext(ctx), filter, optionListers(opts...)...))
}

func (c *MongoCollection) FindOneAndDelete(ctx context.Context, filter interface{}, opts ...*options.FindOneAndDeleteOptions) ISingleResult {
	return wrapSingleResult(c.coll.FindOneAndDelete(ensureContext(ctx), filter, optionListers(opts...)...))
}

func (c *MongoCollection) FindOneAndReplace(ctx context.Context, filter, replacement interface{}, opts ...*options.FindOneAndReplaceOptions) ISingleResult {
	return wrapSingleResult(c.coll.FindOneAndReplace(ensureContext(ctx), filter, replacement, optionListers(opts...)...))
}

func (c *MongoCollection) FindOneAndUpdate(ctx context.Context, filter, update interface{}, opts ...*options.FindOneAndUpdateOptions) ISingleResult {
	return wrapSingleResult(c.coll.FindOneAndUpdate(ensureContext(ctx), filter, update, optionListers(opts...)...))
}

func (c *MongoCollection) Indexes() IIndexView {
	v := c.coll.Indexes()
	return &MongoIndexView{view: v}
}

func (c *MongoCollection) InsertMany(ctx context.Context, documents []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	return c.coll.InsertMany(ensureContext(ctx), documents, optionListers(opts...)...)
}

func (c *MongoCollection) InsertOne(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	return c.coll.InsertOne(ensureContext(ctx), document, optionListers(opts...)...)
}

func (c *MongoCollection) Name() string { return c.coll.Name() }

func (c *MongoCollection) ReplaceOne(ctx context.Context, filter, replacement interface{}, opts ...*options.ReplaceOptions) (*mongo.UpdateResult, error) {
	return c.coll.ReplaceOne(ensureContext(ctx), filter, replacement, optionListers(opts...)...)
}

func (c *MongoCollection) SearchIndexes() mongo.SearchIndexView {
	return c.coll.SearchIndexes()
}

func (c *MongoCollection) UpdateByID(ctx context.Context, id interface{}, update interface{}, opts ...*UpdateOptions) (*mongo.UpdateResult, error) {
	return c.coll.UpdateByID(ensureContext(ctx), id, update, optionListers(opts...)...)
}

func (c *MongoCollection) UpdateMany(ctx context.Context, filter, update interface{}, opts ...*UpdateOptions) (*mongo.UpdateResult, error) {
	return c.coll.UpdateMany(ensureContext(ctx), filter, update, updateManyListers(opts...)...)
}

func (c *MongoCollection) UpdateOne(ctx context.Context, filter, update interface{}, opts ...*UpdateOptions) (*mongo.UpdateResult, error) {
	return c.coll.UpdateOne(ensureContext(ctx), filter, update, optionListers(opts...)...)
}

func (c *MongoCollection) Watch(ctx context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (IChangeStream, error) {
	return c.coll.Watch(ensureContext(ctx), pipeline, optionListers(opts...)...)
}

var _ IIndexView = &MongoIndexView{}

type MongoIndexView struct {
	view mongo.IndexView
}

func (m *MongoIndexView) CreateMany(ctx context.Context, models []mongo.IndexModel, opts ...*options.CreateIndexesOptions) ([]string, error) {
	return m.view.CreateMany(ensureContext(ctx), models, optionListers(opts...)...)
}

func (m *MongoIndexView) CreateOne(ctx context.Context, model mongo.IndexModel, opts ...*options.CreateIndexesOptions) (string, error) {
	return m.view.CreateOne(ensureContext(ctx), model, optionListers(opts...)...)
}

func (m *MongoIndexView) DropAll(ctx context.Context, opts ...*options.DropIndexesOptions) (bson.Raw, error) {
	err := m.view.DropAll(ensureContext(ctx), optionListers(opts...)...)
	return nil, err
}

func (m *MongoIndexView) DropOne(ctx context.Context, name string, opts ...*options.DropIndexesOptions) (bson.Raw, error) {
	err := m.view.DropOne(ensureContext(ctx), name, optionListers(opts...)...)
	return nil, err
}

func (m *MongoIndexView) DropOneWithKey(ctx context.Context, keySpec interface{}, opts ...*options.DropIndexesOptions) (bson.Raw, error) {
	err := m.view.DropWithKey(ensureContext(ctx), keySpec, optionListers(opts...)...)
	return nil, err
}

func (m *MongoIndexView) List(ctx context.Context, opts ...*options.ListIndexesOptions) (ICursor, error) {
	cur, err := m.view.List(ensureContext(ctx), optionListers(opts...)...)
	return wrapCursor(cur), err
}

func (m *MongoIndexView) ListSpecifications(ctx context.Context, opts ...*options.ListIndexesOptions) ([]*mongo.IndexSpecification, error) {
	specs, err := m.view.ListSpecifications(ensureContext(ctx), optionListers(opts...)...)
	if err != nil {
		return nil, err
	}
	out := make([]*mongo.IndexSpecification, len(specs))
	for i := range specs {
		spec := specs[i]
		out[i] = &spec
	}
	return out, nil
}

var _ ISession = &MongoSession{}

type MongoSession struct {
	sess   *mongo.Session
	client *MongoClient
}

func (s *MongoSession) ID() bson.Raw { return s.sess.ID() }

func (s *MongoSession) AbortTransaction(ctx context.Context) error {
	return s.sess.AbortTransaction(ensureContext(ctx))
}

func (s *MongoSession) AdvanceClusterTime(ct bson.Raw) error { return s.sess.AdvanceClusterTime(ct) }

func (s *MongoSession) AdvanceOperationTime(ts *bson.Timestamp) error {
	return s.sess.AdvanceOperationTime(ts)
}

func (s *MongoSession) Client() IClient { return s.client }

func (s *MongoSession) ClusterTime() bson.Raw { return s.sess.ClusterTime() }

func (s *MongoSession) CommitTransaction(ctx context.Context) error {
	return s.sess.CommitTransaction(ensureContext(ctx))
}

func (s *MongoSession) EndSession(ctx context.Context) { s.sess.EndSession(ensureContext(ctx)) }

func (s *MongoSession) OperationTime() *bson.Timestamp { return s.sess.OperationTime() }

func (s *MongoSession) StartTransaction(opts ...*options.TransactionOptions) error {
	return s.sess.StartTransaction(optionListers(opts...)...)
}

func (s *MongoSession) WithTransaction(ctx context.Context, fn func(ISessionContext) (interface{}, error), opts ...*options.TransactionOptions) (interface{}, error) {
	return s.sess.WithTransaction(ensureContext(ctx), func(sc context.Context) (interface{}, error) {
		sess := mongo.SessionFromContext(sc)
		return fn(&MongoSessionContext{
			Context: sc,
			MongoSession: &MongoSession{
				sess:   sess,
				client: s.client,
			},
		})
	}, optionListers(opts...)...)
}

var _ ISessionContext = &MongoSessionContext{}

type MongoSessionContext struct {
	context.Context
	*MongoSession
}
