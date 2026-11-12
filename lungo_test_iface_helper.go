package lungo

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// assertDriverInterfaceParity checks that lungo's interface surface matches the
// official mongo driver's exported methods, modulo v2 API drift and lungo extras.
func assertDriverInterfaceParity(
	t *testing.T,
	iface reflect.Type,
	concrete reflect.Type,
	replacements map[string]string,
	skipMongo []string,
	extraLungo []string,
) {
	t.Helper()

	a := normalizeMongoDriverMethods(methods(iface, replacements))
	b := normalizeMongoDriverMethods(methods(concrete, replacements, skipMongo...))

	a = omitMethodSignatures(a, extraLungo)
	sort.Strings(a)
	sort.Strings(b)

	assert.Equal(t, a, b)
}

func omitMethodSignatures(list []string, drops []string) []string {
	if len(drops) == 0 {
		return list
	}
	drop := make(map[string]struct{}, len(drops))
	for _, s := range drops {
		drop[s] = struct{}{}
	}
	out := make([]string, 0, len(list))
	for _, s := range list {
		if _, ok := drop[s]; ok {
			continue
		}
		out = append(out, s)
	}
	return out
}

func normalizeMongoDriverMethods(list []string) []string {
	out := make([]string, len(list))
	for i, s := range list {
		s = strings.ReplaceAll(s, "*options.DeleteOneOptions", "*DeleteOptions")
		s = strings.ReplaceAll(s, "*options.DeleteManyOptions", "*DeleteOptions")
		s = strings.ReplaceAll(s, "*options.UpdateOneOptions", "*UpdateOptions")
		s = strings.ReplaceAll(s, "*options.UpdateManyOptions", "*UpdateOptions")
		s = strings.ReplaceAll(s, "[]interface{}", "[]any")
		s = strings.ReplaceAll(s, "Drop(context.Context) error", "Drop(context.Context) error")
		s = strings.ReplaceAll(s, "Drop(context.Context, ...*options.DropCollectionOptions) error", "Drop(context.Context) error")
		if strings.HasPrefix(s, "Clone(") && !strings.Contains(s, "error)") {
			s = strings.Replace(s, ") ICollection", ") (ICollection, error)", 1)
		}
		if strings.HasPrefix(s, "Distinct(") {
			s = strings.Replace(s, ") *mongo.DistinctResult", ") ([]any, error)", 1)
		}
		s = strings.ReplaceAll(s, "*mongo.Session", "ISession")
		s = strings.ReplaceAll(s, "(mongo.Session,", "(ISession,")
		s = strings.ReplaceAll(s, " (mongo.Session)", " ISession")
		s = strings.ReplaceAll(s, "*options.SessionOptionsBuilder", "*options.SessionOptions")
		if strings.HasPrefix(s, "UseSession") {
			s = strings.ReplaceAll(s, "func(context.Context) error", "func(ISessionContext) error")
		}
		s = strings.ReplaceAll(s, "interface {}", "any")
		s = strings.ReplaceAll(s, "InsertMany(context.Context, any,", "InsertMany(context.Context, []any,")
		if strings.HasPrefix(s, "WithTransaction") {
			s = strings.ReplaceAll(s, "func(context.Context) (any, error)", "func(ISessionContext) (any, error)")
		}
		s = strings.ReplaceAll(s, "(interface{}, error)", "(any, error)")
		s = strings.ReplaceAll(s, "(interface{},", "(any,")
		s = strings.ReplaceAll(s, "lungo.IClient", "IClient")
		s = strings.ReplaceAll(s, "lungo.IDatabase", "IDatabase")
		s = strings.ReplaceAll(s, "lungo.ICollection", "ICollection")
		s = strings.ReplaceAll(s, "lungo.ICursor", "ICursor")
		s = strings.ReplaceAll(s, "lungo.ISingleResult", "ISingleResult")
		s = strings.ReplaceAll(s, "lungo.IIndexView", "IIndexView")
		s = strings.ReplaceAll(s, "lungo.IChangeStream", "IChangeStream")
		s = strings.ReplaceAll(s, "lungo.ISession", "ISession")
		s = strings.ReplaceAll(s, "lungo.ISessionContext", "ISessionContext")
		out[i] = s
	}
	return out
}
