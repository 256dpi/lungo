package lungo

import (
	"reflect"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
)

var interfaceReplacements = map[string]string{
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
	assert.Equal(t, listMethods(a, false, false), listMethods(b, true, false))
}

func TestDatabaseInterface(t *testing.T) {
	a := reflect.TypeOf((*IDatabase)(nil)).Elem()
	b := reflect.TypeOf(&mongo.Database{})
	assert.Equal(t, listMethods(a, false, false), listMethods(b, true, false))
}

func TestCollectionInterface(t *testing.T) {
	a := reflect.TypeOf((*ICollection)(nil)).Elem()
	b := reflect.TypeOf(&mongo.Collection{})
	assert.Equal(t, listMethods(a, false, false), listMethods(b, true, false))
}

func TestCursorInterface(t *testing.T) {
	a := reflect.TypeOf((*ICursor)(nil)).Elem()
	b := reflect.TypeOf(&mongo.Cursor{})
	assert.Equal(t, listMethods(a, false, false), listMethods(b, true, false))
}

func TestSingleResultInterface(t *testing.T) {
	a := reflect.TypeOf((*ISingleResult)(nil)).Elem()
	b := reflect.TypeOf(&mongo.SingleResult{})
	assert.Equal(t, listMethods(a, false, false), listMethods(b, true, false))
}

func TestIndexViewInterface(t *testing.T) {
	a := reflect.TypeOf((*IIndexView)(nil)).Elem()
	b := reflect.TypeOf(&mongo.IndexView{})
	assert.Equal(t, listMethods(a, false, false), listMethods(b, true, false))
}

func TestChangeStreamInterface(t *testing.T) {
	a := reflect.TypeOf((*IChangeStream)(nil)).Elem()
	b := reflect.TypeOf(&mongo.ChangeStream{})
	assert.Equal(t, listMethods(a, false, false), listMethods(b, true, false))
}

func TestSessionInterface(t *testing.T) {
	a := reflect.TypeOf((*ISession)(nil)).Elem()
	b := reflect.TypeOf((*mongo.Session)(nil)).Elem()
	assert.Equal(t, listMethods(a, false, true), listMethods(b, true, true))
}

func listMethods(t reflect.Type, original, iface bool) string {
	var list []string
	for i := 0; i < t.NumMethod(); i++ {
		m := t.Method(i)
		f := m.Type.String()[4:]

		if original {
			if !iface {
				c := strings.Index(f, ",")
				if c >= 0 && c < strings.Index(f, ")") {
					f = "(" + f[c+2:]
				} else {
					c = strings.Index(f, ")")
					f = "(" + f[c:]
				}
			}

			for a, b := range interfaceReplacements {
				f = strings.ReplaceAll(f, a, b)
			}
		}

		if unicode.IsUpper(rune(m.Name[0])) {
			list = append(list, m.Name+f)
		}
	}

	return strings.Join(list, "\n")
}
