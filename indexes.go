package lungo

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/256dpi/lungo/bsonkit"
	"github.com/256dpi/lungo/mongokit"
)

var _ IIndexView = &IndexView{}

// IndexView wraps an Engine to be mongo compatible.
type IndexView struct {
	engine *Engine
	handle Handle
}

// CreateMany implements the IIndexView.CreateMany method.
func (v *IndexView) CreateMany(ctx context.Context, indexes []mongo.IndexModel, opts ...options.Lister[options.CreateIndexesOptions]) ([]string, error) {
	// merge options
	opt := mergeOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"MaxTime": ignored,
	})

	// check filer
	if len(indexes) == 0 {
		panic("lungo: missing indexes")
	}

	// created indexes separately
	var names []string
	for _, index := range indexes {
		name, err := v.CreateOne(ctx, index, opts...)
		if err != nil {
			return names, err
		}
		names = append(names, name)
	}

	return names, nil
}

// CreateOne implements the IIndexView.CreateOne method.
func (v *IndexView) CreateOne(ctx context.Context, index mongo.IndexModel, opts ...options.Lister[options.CreateIndexesOptions]) (string, error) {
	// merge options
	opt := mergeOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"MaxTime": ignored,
	})

	// resolve index options
	idxOpt := mergeOptions[options.IndexOptions](index.Options)

	// assert supported index options
	assertOptions(idxOpt, map[string]string{
		"ExpireAfterSeconds":      supported,
		"Name":                    supported,
		"Unique":                  supported,
		"Version":                 ignored,
		"PartialFilterExpression": supported,
	})

	// transform key
	key, err := bsonkit.Transform(index.Keys)
	if err != nil {
		return "", err
	}

	// get expiry
	var expiry time.Duration
	if idxOpt.ExpireAfterSeconds != nil {
		if *idxOpt.ExpireAfterSeconds == 0 {
			expiry = time.Nanosecond
		} else {
			expiry = time.Duration(*idxOpt.ExpireAfterSeconds) * time.Second
		}
	}

	// get name
	var name string
	if idxOpt.Name != nil {
		name = *idxOpt.Name
	}

	// get unique
	var unique bool
	if idxOpt.Unique != nil {
		unique = *idxOpt.Unique
	}

	// get partial
	var partial bsonkit.Doc
	if idxOpt.PartialFilterExpression != nil {
		partial, err = bsonkit.Transform(idxOpt.PartialFilterExpression)
		if err != nil {
			return "", err
		}
	}

	// begin transaction
	txn, err := v.engine.Begin(ctx, true)
	if err != nil {
		return "", err
	}

	// ensure abortion
	defer v.engine.Abort(txn)

	// create index
	name, err = txn.CreateIndex(v.handle, name, mongokit.IndexConfig{
		Key:     key,
		Unique:  unique,
		Partial: partial,
		Expiry:  expiry,
	})
	if err != nil {
		return "", err
	}

	// commit transaction
	err = v.engine.Commit(txn)
	if err != nil {
		return "", err
	}

	return name, nil
}

// DropAll implements the IIndexView.DropAll method.
func (v *IndexView) DropAll(ctx context.Context, opts ...options.Lister[options.DropIndexesOptions]) error {
	// merge options
	opt := mergeOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"MaxTime": ignored,
	})

	// begin transaction
	txn, err := v.engine.Begin(ctx, true)
	if err != nil {
		return err
	}

	// ensure abortion
	defer v.engine.Abort(txn)

	// drop all indexes
	err = txn.DropIndex(v.handle, "")
	if err != nil {
		return err
	}

	// commit transaction
	err = v.engine.Commit(txn)
	if err != nil {
		return err
	}

	return nil
}

// DropOne implements the IIndexView.DropOne method.
func (v *IndexView) DropOne(ctx context.Context, name string, opts ...options.Lister[options.DropIndexesOptions]) error {
	// merge options
	opt := mergeOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"MaxTime": ignored,
	})

	// check name
	if name == "" || name == "*" {
		panic("lungo: invalid index name")
	}

	// begin transaction
	txn, err := v.engine.Begin(ctx, true)
	if err != nil {
		return err
	}

	// ensure abortion
	defer v.engine.Abort(txn)

	// drop all indexes
	err = txn.DropIndex(v.handle, name)
	if err != nil {
		return err
	}

	// commit transaction
	err = v.engine.Commit(txn)
	if err != nil {
		return err
	}

	return nil
}

// DropWithKey implements the IIndexView.DropWithKey method.
func (v *IndexView) DropWithKey(ctx context.Context, keySpec interface{}, opts ...options.Lister[options.DropIndexesOptions]) error {
	// merge options
	opt := mergeOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"MaxTime": ignored,
	})

	// transform key
	key, err := bsonkit.Transform(keySpec)
	if err != nil {
		return err
	}

	// begin transaction
	txn, err := v.engine.Begin(ctx, true)
	if err != nil {
		return err
	}

	// ensure abortion
	defer v.engine.Abort(txn)

	// drop index by key
	err = txn.DropIndexByKey(v.handle, key)
	if err != nil {
		return err
	}

	// commit transaction
	err = v.engine.Commit(txn)
	if err != nil {
		return err
	}

	return nil
}

// List implements the IIndexView.List method.
func (v *IndexView) List(ctx context.Context, opts ...options.Lister[options.ListIndexesOptions]) (ICursor, error) {
	// merge options
	opt := mergeOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"BatchSize": ignored,
		"MaxTime":   ignored,
	})

	// list indexes (route through useTransaction so session-bound
	// transactions are honored)
	res, err := useTransaction(ctx, v.engine, false, func(txn *Transaction) (interface{}, error) {
		return txn.ListIndexes(v.handle)
	})
	if err != nil {
		return nil, err
	}

	return &Cursor{list: res.(bsonkit.List)}, nil
}

// ListSpecifications implements the IIndexView.ListSpecifications method.
func (v *IndexView) ListSpecifications(context.Context, ...options.Lister[options.ListIndexesOptions]) ([]mongo.IndexSpecification, error) {
	panic("lungo: not implemented")
}
