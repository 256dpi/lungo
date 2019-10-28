package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/256dpi/lungo/bsonkit"
)

var _ IIndexView = &IndexView{}

// IndexView wraps an Engine to be mongo compatible.
type IndexView struct {
	ns     string
	engine *Engine
}

// CreateMany implements the IIndexView.CreateMany method.
func (v *IndexView) CreateMany(ctx context.Context, indexes []mongo.IndexModel, opts ...*options.CreateIndexesOptions) ([]string, error) {
	// merge options
	opt := options.MergeCreateIndexesOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"MaxTime": ignored,
	})

	// check filer
	if len(indexes) == 0 {
		panic("lungo: missing indexes")
	}

	// TODO: Should this be atomic?

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
func (v *IndexView) CreateOne(ctx context.Context, index mongo.IndexModel, opts ...*options.CreateIndexesOptions) (string, error) {
	// merge options
	opt := options.MergeCreateIndexesOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"MaxTime": ignored,
	})

	// assert supported index options
	if index.Options != nil {
		assertOptions(index.Options, map[string]string{
			"Background": ignored,
			"Name":       supported,
			"Unique":     supported,
			"Version":    ignored,
		})

		// TODO: Support ExpireAfterSeconds.
		// TODO: Support PartialFilterExpression.
	}

	// transform keys
	keys, err := bsonkit.Transform(index.Keys)
	if err != nil {
		return "", err
	}

	// get name
	var name string
	if index.Options != nil && index.Options.Name != nil {
		name = *index.Options.Name
	}

	// get unique
	var unique bool
	if index.Options != nil && index.Options.Unique != nil {
		unique = *index.Options.Unique
	}

	// create index
	name, err = v.engine.CreateIndex(v.ns, keys, name, unique)
	if err != nil {
		return "", err
	}

	return name, nil
}

// DropAll implements the IIndexView.DropAll method.
func (v *IndexView) DropAll(ctx context.Context, opts ...*options.DropIndexesOptions) (bson.Raw, error) {
	// merge options
	opt := options.MergeDropIndexesOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"MaxTime": ignored,
	})

	// drop all indexes
	err := v.engine.DropIndex(v.ns, "*")
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// DropOne implements the IIndexView.DropOne method.
func (v *IndexView) DropOne(ctx context.Context, name string, opts ...*options.DropIndexesOptions) (bson.Raw, error) {
	// merge options
	opt := options.MergeDropIndexesOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"MaxTime": ignored,
	})

	// check name
	if name == "" || name == "*" {
		panic("lungo: invalid index name")
	}

	// drop all indexes
	err := v.engine.DropIndex(v.ns, name)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// List implements the IIndexView.List method.
func (v *IndexView) List(ctx context.Context, opts ...*options.ListIndexesOptions) (ICursor, error) {
	// merge options
	opt := options.MergeListIndexesOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"BatchSIze": ignored,
		"MaxTime":   ignored,
	})

	// list indexes
	list, err := v.engine.ListIndexes(v.ns)
	if err != nil {
		return nil, err
	}

	return &Cursor{list: list}, nil
}
