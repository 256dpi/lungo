package lungo

import (
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

var mongoReplacements = map[string]string{
	"*mongo.Client":                                  "lungo.IClient",
	"*mongo.Database":                                "lungo.IDatabase",
	"*mongo.Collection":                              "lungo.ICollection",
	"*mongo.Cursor":                                  "lungo.ICursor",
	"*mongo.SingleResult":                            "lungo.ISingleResult",
	"mongo.IndexView":                                "lungo.IIndexView",
	"*mongo.ChangeStream":                            "lungo.IChangeStream",
	"*mongo.Session":                                 "lungo.ISession",
	"func(context.Context) error":                    "func(lungo.ISessionContext) error",
	"func(context.Context) (interface {}, error)":    "func(lungo.ISessionContext) (interface {}, error)",
	"*options.SessionOptionsBuilder":                 "options.Lister[go.mongodb.org/mongo-driver/v2/mongo/options.SessionOptions]",
}

func TestClientInterface(t *testing.T) {
	a := reflect.TypeOf((*IClient)(nil)).Elem()
	b := reflect.TypeOf(&mongo.Client{})
	// the v2 driver added AppendDriverInfo/BulkWrite that lungo does not expose
	skip := []string{"AppendDriverInfo", "BulkWrite"}
	assert.Equal(t, methods(a, nil), methods(b, mongoReplacements, skip...))
}

func TestDatabaseInterface(t *testing.T) {
	a := reflect.TypeOf((*IDatabase)(nil)).Elem()
	b := reflect.TypeOf(&mongo.Database{})
	// GridFSBucket is exposed via lungo.NewBucket instead of a method
	skip := []string{"GridFSBucket"}
	assert.Equal(t, methods(a, nil), methods(b, mongoReplacements, skip...))
}

func TestCollectionInterface(t *testing.T) {
	a := reflect.TypeOf((*ICollection)(nil)).Elem()
	b := reflect.TypeOf(&mongo.Collection{})
	// Distinct returns ([]interface{}, error) instead of *mongo.DistinctResult
	skip := []string{"Distinct"}
	expected := methods(b, mongoReplacements, skip...)
	expected = append(expected,
		"Distinct(context.Context, string, interface {}, ...options.Lister[go.mongodb.org/mongo-driver/v2/mongo/options.DistinctOptions]) ([]interface {}, error)",
	)
	sort.Strings(expected)
	assert.Equal(t, methods(a, nil), expected)
}

func TestCursorInterface(t *testing.T) {
	a := reflect.TypeOf((*ICursor)(nil)).Elem()
	b := reflect.TypeOf(&mongo.Cursor{})
	assert.Equal(t, methods(a, nil), methods(b, mongoReplacements))
}

func TestSingleResultInterface(t *testing.T) {
	a := reflect.TypeOf((*ISingleResult)(nil)).Elem()
	b := reflect.TypeOf(&mongo.SingleResult{})
	assert.Equal(t, methods(a, nil), methods(b, mongoReplacements))
}

func TestIndexViewInterface(t *testing.T) {
	a := reflect.TypeOf((*IIndexView)(nil)).Elem()
	b := reflect.TypeOf(&mongo.IndexView{})
	assert.Equal(t, methods(a, nil), methods(b, mongoReplacements))
}

func TestChangeStreamInterface(t *testing.T) {
	a := reflect.TypeOf((*IChangeStream)(nil)).Elem()
	b := reflect.TypeOf(&mongo.ChangeStream{})
	assert.Equal(t, methods(a, nil), methods(b, mongoReplacements))
}

func TestSessionInterface(t *testing.T) {
	a := reflect.TypeOf((*ISession)(nil)).Elem()
	b := reflect.TypeOf(&mongo.Session{})
	// lungo's session keeps the ISessionContext callback shape; the v2 driver
	// added SnapshotTime and TransactionRunning that lungo does not expose.
	skip := []string{"SnapshotTime", "TransactionRunning"}
	assert.Equal(t, methods(a, nil), methods(b, mongoReplacements, skip...))
}
