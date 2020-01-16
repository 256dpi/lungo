package lungo

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
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
	a := reflect.TypeOf((*IClient)(nil)).Elem()
	b := reflect.TypeOf(&mongo.Client{})
	assert.Equal(t, methods(a, nil), methods(b, mongoReplacements))
}

func TestDatabaseInterface(t *testing.T) {
	a := reflect.TypeOf((*IDatabase)(nil)).Elem()
	b := reflect.TypeOf(&mongo.Database{})
	assert.Equal(t, methods(a, nil), methods(b, mongoReplacements))
}

func TestCollectionInterface(t *testing.T) {
	a := reflect.TypeOf((*ICollection)(nil)).Elem()
	b := reflect.TypeOf(&mongo.Collection{})
	assert.Equal(t, methods(a, nil), methods(b, mongoReplacements))
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
	b := reflect.TypeOf((*mongo.Session)(nil)).Elem()
	assert.Equal(t, methods(a, nil), methods(b, mongoReplacements))
}
