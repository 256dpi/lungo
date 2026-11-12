package lungo

import (
	"reflect"

	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// mergeOptions merges driver option structs the way mongo-driver v1 did (last non-nil field wins).
func mergeOptions[T any](opts ...*T) *T {
	out := new(T)
	outVal := reflect.ValueOf(out).Elem()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		inVal := reflect.ValueOf(opt).Elem()
		for i := 0; i < inVal.NumField(); i++ {
			field := inVal.Field(i)
			if isZeroish(field) {
				continue
			}
			outVal.Field(i).Set(field)
		}
	}
	return out
}

func isZeroish(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return v.IsNil()
	default:
		return v.IsZero()
	}
}

type ptrLister[T any] struct {
	opt *T
}

func (p *ptrLister[T]) List() []func(*T) error {
	return []func(*T) error{
		func(args *T) error {
			if p.opt != nil {
				*args = *p.opt
			}
			return nil
		},
	}
}

// optionListers adapts lungo's variadic *Options pointers to mongo-driver v2 Listers.
// materialize applies a v2 options builder into a concrete options struct.
func materialize[T any](b options.Lister[T]) (*T, error) {
	out := new(T)
	if b == nil {
		return out, nil
	}
	for _, set := range b.List() {
		if set == nil {
			continue
		}
		if err := set(out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func deleteManyListers(opts ...*DeleteOptions) []options.Lister[options.DeleteManyOptions] {
	out := make([]options.Lister[options.DeleteManyOptions], 0, len(opts))
	for _, o := range opts {
		if o == nil {
			continue
		}
		dup := &options.DeleteManyOptions{
			Comment:   o.Comment,
			Hint:      o.Hint,
			Collation: o.Collation,
			Let:       o.Let,
		}
		out = append(out, &ptrLister[options.DeleteManyOptions]{opt: dup})
	}
	return out
}

func updateManyListers(opts ...*UpdateOptions) []options.Lister[options.UpdateManyOptions] {
	out := make([]options.Lister[options.UpdateManyOptions], 0, len(opts))
	for _, o := range opts {
		if o == nil {
			continue
		}
		dup := &options.UpdateManyOptions{
			ArrayFilters:             o.ArrayFilters,
			BypassDocumentValidation: o.BypassDocumentValidation,
			Collation:                o.Collation,
			Comment:                  o.Comment,
			Hint:                     o.Hint,
			Upsert:                   o.Upsert,
			Let:                      o.Let,
		}
		out = append(out, &ptrLister[options.UpdateManyOptions]{opt: dup})
	}
	return out
}

func optionListers[T any](opts ...*T) []options.Lister[T] {
	if len(opts) == 0 {
		return nil
	}
	out := make([]options.Lister[T], 0, len(opts))
	for _, o := range opts {
		out = append(out, &ptrLister[T]{opt: o})
	}
	return out
}
