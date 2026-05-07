package lungo

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/256dpi/lungo/bsonkit"
)

// mergeOptions merges a slice of option listers into a single options struct
// in a "last-one-wins" manner, ignoring nil entries.
func mergeOptions[T any](opts ...options.Lister[T]) *T {
	args := new(T)
	for _, opt := range opts {
		if opt == nil || reflect.ValueOf(opt).IsNil() {
			continue
		}
		for _, set := range opt.List() {
			if set == nil {
				continue
			}
			if err := set(args); err != nil {
				panic(fmt.Sprintf("lungo: option error: %s", err))
			}
		}
	}
	return args
}

const (
	supported = "supported"
	ignored   = "ignored"
)

func ensureContext(ctx context.Context) context.Context {
	// check context
	if ctx != nil {
		return ctx
	}

	return context.Background()
}

func assertOptions(opts interface{}, fields map[string]string) {
	// get value
	value := reflect.ValueOf(opts).Elem()

	// check fields
	for i := 0; i < value.NumField(); i++ {
		// get name
		name := value.Type().Field(i).Name

		// check if field is supported
		support := fields[name]
		if support == supported || support == ignored {
			continue
		}

		// skip non-nilable fields (e.g. internal struct values added by the
		// v2 driver that cannot represent "unset")
		field := value.Field(i)
		switch field.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface,
			reflect.Map, reflect.Ptr, reflect.Slice:
			// nilable — fall through to IsNil check
		default:
			continue
		}

		// otherwise, assert field is nil
		if !field.IsNil() {
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
