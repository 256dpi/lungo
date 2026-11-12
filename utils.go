package lungo

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/SaveliiYam/lungo/bsonkit"
)

const (
	supported = "supported"
	ignored   = "ignored"
)

func optionFieldUnset(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
		return v.IsNil()
	default:
		return v.IsZero()
	}
}

func ensureContext(ctx context.Context) context.Context {
	// check context
	if ctx != nil {
		return ctx
	}

	return context.Background()
}

func assertOptions(opts interface{}, fields map[string]string) {
	if opts == nil {
		return
	}
	val := reflect.ValueOf(opts)
	if val.Kind() == reflect.Ptr && val.IsNil() {
		return
	}

	// get value
	value := val.Elem()

	// check fields
	for i := 0; i < value.NumField(); i++ {
		// get name
		name := value.Type().Field(i).Name

		// check if field is supported
		support := fields[name]
		if support == supported || support == ignored {
			continue
		}

		// otherwise, assert field is unset (v2 options may use struct fields, not only pointers)
		if !optionFieldUnset(value.Field(i)) {
			panic(fmt.Sprintf("lungo: unsupported option: %s", name))
		}
	}
}

// validateReplacement rejects replacement documents whose first key begins
// with '$'. The official mongo-driver enforces this client-side because such
// documents look like update operators and would otherwise be silently stored
// as data.
func validateReplacement(doc bsonkit.Doc) error {
	if doc == nil || len(*doc) == 0 {
		return nil
	}
	if strings.HasPrefix((*doc)[0].Key, "$") {
		return fmt.Errorf("replacement document cannot contain keys beginning with '$'")
	}
	return nil
}

func useTransaction(ctx context.Context, engine *Engine, lock bool, fn func(*Transaction) (interface{}, error)) (interface{}, error) {
	// ensure context
	ctx = ensureContext(ctx)

	// use active transaction from session in context
	sess, ok := ctx.Value(sessionKey{}).(*Session)
	if ok {
		txn := sess.Transaction()
		if txn != nil {
			return fn(txn)
		}
	}

	// create transaction
	txn, err := engine.Begin(ctx, lock)
	if err != nil {
		return nil, err
	}

	// handle unlocked transactions immediately
	if !lock {
		return fn(txn)
	}

	// ensure abortion
	defer engine.Abort(txn)

	// yield callback
	res, err := fn(txn)
	if err != nil {
		return nil, err
	}

	// commit transaction
	err = engine.Commit(txn)
	if err != nil {
		return nil, err
	}

	return res, nil
}
