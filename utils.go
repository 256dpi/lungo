package lungo

import (
	"context"
	"fmt"
	"reflect"

	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

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

		// deprecated
		if name == "Internal" {
			continue
		}

		// check if field is supported
		support := fields[name]
		if support == supported || support == ignored {
			continue
		}

		// otherwise, assert field is nil
		if !value.Field(i).IsNil() {
			panic(fmt.Sprintf("lungo: unsupported option: %s", name))
		}
	}
}

func useTransaction[T any](ctx context.Context, engine *Engine, lock bool, fn func(*Transaction) (T, error)) (T, error) {
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
		return *new(T), err
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
		return *new(T), err
	}

	// commit transaction
	err = engine.Commit(txn)
	if err != nil {
		return *new(T), err
	}

	return res, nil
}

// NewOptions will functionally merge a slice of mongo.Options in a
// "last-one-wins" manner, where nil options are ignored.
func NewOptions[T any](opts ...options.Lister[T]) (*T, error) {
	args := new(T)
	for _, opt := range opts {
		if opt == nil || reflect.ValueOf(opt).IsNil() {
			// Do nothing if the option is nil or if opt is nil but implicitly cast as
			// an Options interface by the NewArgsFromOptions function. The latter
			// case would look something like this:
			continue
		}
		for _, setArgs := range opt.List() {
			if setArgs == nil {
				continue
			}

			if err := setArgs(args); err != nil {
				return nil, err
			}
		}
	}
	return args, nil
}
