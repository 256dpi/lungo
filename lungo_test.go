package lungo

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
)

var alternativeTypes = map[string]string{
	"*mongo.Client":     "lungo.Client",
	"*mongo.Database":   "lungo.Database",
	"*mongo.Collection": "lungo.Collection",
	"*mongo.Cursor":     "lungo.Cursor",
}

func TestClientInterface(t *testing.T) {
	a := reflect.TypeOf((*Client)(nil)).Elem()
	b := reflect.TypeOf(&mongo.Client{})
	assert.Equal(t, listMethods(a, false), listMethods(b, true))
}

func TestDatabaseInterface(t *testing.T) {
	a := reflect.TypeOf((*Database)(nil)).Elem()
	b := reflect.TypeOf(&mongo.Database{})
	assert.Equal(t, listMethods(a, false), listMethods(b, true))
}

func TestCollectionInterface(t *testing.T) {
	a := reflect.TypeOf((*Collection)(nil)).Elem()
	b := reflect.TypeOf(&mongo.Collection{})
	assert.Equal(t, listMethods(a, false), listMethods(b, true))
}

func TestCursorInterface(t *testing.T) {
	a := reflect.TypeOf((*Cursor)(nil)).Elem()
	b := reflect.TypeOf(&mongo.Cursor{})
	assert.Equal(t, listMethods(a, false), listMethods(b, true))
}

func listMethods(t reflect.Type, original bool) string {
	var list []string
	for i := 0; i < t.NumMethod(); i++ {
		m := t.Method(i)
		f := m.Type.String()[4:]

		if original {
			c := strings.Index(f, ",")
			if c >= 0 {
				f = "(" + f[c+2:]
			} else {
				c = strings.Index(f, ")")
				f = "(" + f[c:]
			}

			for a, b := range alternativeTypes {
				f = strings.ReplaceAll(f, a, b)
			}
		}

		list = append(list, m.Name+f)
	}

	return strings.Join(list, "\n")
}
