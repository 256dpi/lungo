package lungo

import (
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

var mongoReplacements = map[string]string{
	"*mongo.Client":        "lungo.IClient",
	"*mongo.Database":      "lungo.IDatabase",
	"*mongo.Collection":    "lungo.ICollection",
	"*mongo.Cursor":        "lungo.ICursor",
	"*mongo.SingleResult":  "lungo.ISingleResult",
	"mongo.IndexView":      "lungo.IIndexView",
	"*mongo.ChangeStream":  "lungo.IChangeStream",
	"mongo.Session":        "lungo.ISession",
	"mongo.SessionContext": "lungo.ISessionContext",
}

func TestClientInterface(t *testing.T) {
	assertDriverInterfaceParity(t,
		reflect.TypeOf((*IClient)(nil)).Elem(),
		reflect.TypeOf(&mongo.Client{}),
		mongoReplacements,
		[]string{"AppendDriverInfo", "BulkWrite"},
		[]string{"Connect(context.Context) error", "Timeout() *time.Duration"},
	)
}

func TestDatabaseInterface(t *testing.T) {
	assertDriverInterfaceParity(t,
		reflect.TypeOf((*IDatabase)(nil)).Elem(),
		reflect.TypeOf(&mongo.Database{}),
		mongoReplacements,
		[]string{"AggregateRaw", "GridFSBucket", "ListCollectionSpecifications", "WithReadOnlyIntegrationMode"},
		[]string{
			"ListCollectionSpecifications(context.Context, any, ...*options.ListCollectionsOptions) ([]*mongo.CollectionSpecification, error)",
			"ReadConcern() *readconcern.ReadConcern",
			"ReadPreference() *readpref.ReadPref",
			"WriteConcern() *writeconcern.WriteConcern",
		},
	)
}

func TestCollectionInterface(t *testing.T) {
	assertDriverInterfaceParity(t,
		reflect.TypeOf((*ICollection)(nil)).Elem(),
		reflect.TypeOf(&mongo.Collection{}),
		mongoReplacements,
		[]string{"BulkWrite", "Clone", "FindRaw"},
		[]string{
			"BulkWrite(context.Context, []mongo.WriteModel, ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error)",
			"Clone(...*options.CollectionOptions) (ICollection, error)",
		},
	)
}

func TestCursorInterface(t *testing.T) {
	assertDriverInterfaceParity(t,
		reflect.TypeOf((*ICursor)(nil)).Elem(),
		reflect.TypeOf(&mongo.Cursor{}),
		mongoReplacements,
		[]string{"SetComment", "SetCommentThreadingSafe", "SetMaxAwaitTime", "SetSession"},
		[]string{"SetComment(any)", "SetMaxTime(time.Duration)", "Stream(context.Context) (*mongo.Cursor, error)"},
	)
}

func TestSingleResultInterface(t *testing.T) {
	assertDriverInterfaceParity(t,
		reflect.TypeOf((*ISingleResult)(nil)).Elem(),
		reflect.TypeOf(&mongo.SingleResult{}),
		mongoReplacements,
		nil,
		[]string{"DecodeBytes() (bson.Raw, error)"},
	)
}

func TestIndexViewInterface(t *testing.T) {
	assertDriverInterfaceParity(t,
		reflect.TypeOf((*IIndexView)(nil)).Elem(),
		reflect.TypeOf(&mongo.IndexView{}),
		mongoReplacements,
		[]string{"DropAll", "DropOne", "DropWithKey", "ListSpecifications"},
		[]string{
			"DropAll(context.Context, ...*options.DropIndexesOptions) (bson.Raw, error)",
			"DropOne(context.Context, string, ...*options.DropIndexesOptions) (bson.Raw, error)",
			"DropOneWithKey(context.Context, any, ...*options.DropIndexesOptions) (bson.Raw, error)",
			"ListSpecifications(context.Context, ...*options.ListIndexesOptions) ([]*mongo.IndexSpecification, error)",
		},
	)
}

func TestChangeStreamInterface(t *testing.T) {
	assertDriverInterfaceParity(t,
		reflect.TypeOf((*IChangeStream)(nil)).Elem(),
		reflect.TypeOf(&mongo.ChangeStream{}),
		mongoReplacements,
		[]string{"SetComment", "SetCommentThreadingSafe", "SetSession"},
		nil,
	)
}

func TestSessionInterface(t *testing.T) {
	assertDriverInterfaceParity(t,
		reflect.TypeOf((*ISession)(nil)).Elem(),
		reflect.TypeOf(&mongo.Session{}),
		mongoReplacements,
		[]string{"SnapshotTime", "TransactionRunning", "WithTransactionContext"},
		nil,
	)
}
