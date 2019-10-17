package lungo

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
)

var interfaceReplacements = map[string]string{
	"*mongo.Client":       "lungo.IClient",
	"*mongo.Database":     "lungo.IDatabase",
	"*mongo.Collection":   "lungo.ICollection",
	"*mongo.Cursor":       "lungo.ICursor",
	"*mongo.SingleResult": "lungo.ISingleResult",
}

func TestClientInterface(t *testing.T) {
	a := reflect.TypeOf((*IClient)(nil)).Elem()
	b := reflect.TypeOf(&mongo.Client{})
	assert.Equal(t, listMethods(a, false), listMethods(b, true, "Connect", "Disconnect", "Ping"))
}

func TestDatabaseInterface(t *testing.T) {
	a := reflect.TypeOf((*IDatabase)(nil)).Elem()
	b := reflect.TypeOf(&mongo.Database{})
	assert.Equal(t, listMethods(a, false), listMethods(b, true))
}

func TestCollectionInterface(t *testing.T) {
	a := reflect.TypeOf((*ICollection)(nil)).Elem()
	b := reflect.TypeOf(&mongo.Collection{})
	assert.Equal(t, listMethods(a, false), listMethods(b, true))
}

func TestCursorInterface(t *testing.T) {
	a := reflect.TypeOf((*ICursor)(nil)).Elem()
	b := reflect.TypeOf(&mongo.Cursor{})
	assert.Equal(t, listMethods(a, false), listMethods(b, true))
}

func TestSingleResultInterface(t *testing.T) {
	a := reflect.TypeOf((*ISingleResult)(nil)).Elem()
	b := reflect.TypeOf(&mongo.SingleResult{})
	assert.Equal(t, listMethods(a, false), listMethods(b, true))
}

func listMethods(t reflect.Type, original bool, skip ...string) string {
	blacklist := map[string]bool{}
	for _, name := range skip {
		blacklist[name] = true
	}

	var list []string
	for i := 0; i < t.NumMethod(); i++ {
		m := t.Method(i)
		f := m.Type.String()[4:]

		if blacklist[m.Name] {
			continue
		}

		if original {
			c := strings.Index(f, ",")
			if c >= 0 && c < strings.Index(f, ")") {
				f = "(" + f[c+2:]
			} else {
				c = strings.Index(f, ")")
				f = "(" + f[c:]
			}

			for a, b := range interfaceReplacements {
				f = strings.ReplaceAll(f, a, b)
			}
		}

		list = append(list, m.Name+f)
	}

	return strings.Join(list, "\n")
}
