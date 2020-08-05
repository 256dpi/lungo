package lungo

import (
	"context"
	"fmt"
	"reflect"
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

		// check if field is supported
		support := fields[name]
		if support == supported || support == ignored {
			continue
		}

		// otherwise assert field is nil
		if !value.Field(i).IsNil() {
			panic(fmt.Sprintf("lungo: unsupported option: %s", name))
		}
	}
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
