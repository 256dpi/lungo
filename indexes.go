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
func (v *IndexView) CreateMany(
	ctx context.Context,
	models []mongo.IndexModel,
	opts ...options.Lister[options.CreateIndexesOptions],
) ([]string, error) {
	// merge options
	args, err := NewOptions[options.CreateIndexesOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{
		"MaxTime": ignored,
	})

	// check filer
	if len(models) == 0 {
		panic("lungo: missing indexes")
	}

	// created indexes separately
	var names []string
	for _, index := range models {
		name, err := v.CreateOne(ctx, index, opts...)
		if err != nil {
			return names, err
		}
		names = append(names, name)
	}

	return names, nil
}

// CreateOne implements the IIndexView.CreateOne method.
func (v *IndexView) CreateOne(
	ctx context.Context,
	model mongo.IndexModel,
	opts ...options.Lister[options.CreateIndexesOptions],
) (string, error) {
	// merge options
	args, err := NewOptions[options.CreateIndexesOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{
		"MaxTime": ignored,
	})

	mOpts, err := NewOptions[options.IndexOptions](model.Options)
	if err != nil {
		panic(err)
	}
	// assert supported index options
	if mOpts != nil {
		assertOptions(mOpts, map[string]string{
			"Background":              ignored,
			"ExpireAfterSeconds":      supported,
			"Name":                    supported,
			"Unique":                  supported,
			"Version":                 ignored,
			"PartialFilterExpression": supported,
		})
	}

	// transform key
	key, err := bsonkit.Transform(model.Keys)
	if err != nil {
		return "", err
	}

	// get expiry
	var expiry time.Duration
	if mOpts != nil && mOpts.ExpireAfterSeconds != nil {
		if *mOpts.ExpireAfterSeconds == 0 {
			expiry = time.Nanosecond
		} else {
			expiry = time.Duration(*mOpts.ExpireAfterSeconds) * time.Second
		}
	}

	// get name
	var name string
	if mOpts != nil && mOpts.Name != nil {
		name = *mOpts.Name
	}

	// get unique
	var unique bool
	if mOpts != nil && mOpts.Unique != nil {
		unique = *mOpts.Unique
	}

	// get partial
	var partial bsonkit.Doc
	if mOpts != nil && mOpts.PartialFilterExpression != nil {
		partial, err = bsonkit.Transform(mOpts.PartialFilterExpression)
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
func (v *IndexView) DropAll(
	ctx context.Context,
	opts ...options.Lister[options.DropIndexesOptions],
) error {
	// merge options
	args, err := NewOptions[options.DropIndexesOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{
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
func (v *IndexView) DropOne(
	ctx context.Context,
	name string,
	opts ...options.Lister[options.DropIndexesOptions],
) error {
	// merge options
	args, err := NewOptions[options.DropIndexesOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{
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

func (v *IndexView) DropWithKey(ctx context.Context, keySpecDocument any, opts ...options.Lister[options.DropIndexesOptions]) error {
	panic("lungo: not implemented")
}

// DropOneWithKey implements the IIndexView.DropOneWithKey method.
func (v *IndexView) DropOneWithKey(ctx context.Context, keySpec interface{}, opts ...*options.DropIndexesOptions) (bson.Raw, error) {
	// merge options
	opt := options.MergeDropIndexesOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"MaxTime": ignored,
	})

	// transform key
	key, err := bsonkit.Transform(keySpec)
	if err != nil {
		return nil, err
	}

	// begin transaction
	txn, err := v.engine.Begin(ctx, true)
	if err != nil {
		return nil, err
	}

	// ensure abortion
	defer v.engine.Abort(txn)

	// drop index by key
	err = txn.DropIndexByKey(v.handle, key)
	if err != nil {
		return nil, err
	}

	// commit transaction
	err = v.engine.Commit(txn)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// List implements the IIndexView.List method.
func (v *IndexView) List(ctx context.Context, opts ...options.Lister[options.ListIndexesOptions]) (ICursor, error) {
	// merge options
	args, err := NewOptions[options.ListIndexesOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{
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
func (v *IndexView) ListSpecifications(
	ctx context.Context,
	opts ...options.Lister[options.ListIndexesOptions],
) ([]mongo.IndexSpecification, error) {
	panic("lungo: not implemented")
}
