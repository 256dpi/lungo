package mongokit

import (
	"errors"
	"fmt"
	"math"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/256dpi/lungo/bsonkit"
)

// https://github.com/mongodb/mongo/blob/master/src/mongo/db/matcher/expression_leaf.cpp

// TopLevelQueryOperators defines the top level query operators
var TopLevelQueryOperators = map[string]Operator{}

// ExpressionQueryOperators defines the expression query operators.
var ExpressionQueryOperators = map[string]Operator{}

// ErrNotMatched is returned by query operators if the document does not match.
var ErrNotMatched = errors.New("not matched")

func init() {
	// register top level query operators
	TopLevelQueryOperators["$and"] = matchAnd
	TopLevelQueryOperators["$or"] = matchOr
	TopLevelQueryOperators["$nor"] = matchNor
	TopLevelQueryOperators["$jsonSchema"] = matchJSONSchema

	// register expression query operators
	ExpressionQueryOperators[""] = matchComp
	ExpressionQueryOperators["$eq"] = matchComp
	ExpressionQueryOperators["$gt"] = matchComp
	ExpressionQueryOperators["$lt"] = matchComp
	ExpressionQueryOperators["$gte"] = matchComp
	ExpressionQueryOperators["$lte"] = matchComp
	ExpressionQueryOperators["$ne"] = matchNe
	ExpressionQueryOperators["$not"] = matchNot
	ExpressionQueryOperators["$in"] = matchIn
	ExpressionQueryOperators["$nin"] = matchNin
	ExpressionQueryOperators["$exists"] = matchExists
	ExpressionQueryOperators["$type"] = matchType
	ExpressionQueryOperators["$all"] = matchAll
	ExpressionQueryOperators["$size"] = matchSize
	ExpressionQueryOperators["$elemMatch"] = matchElem
	ExpressionQueryOperators["$bitsAllClear"] = matchBits
	ExpressionQueryOperators["$bitsAllSet"] = matchBits
	ExpressionQueryOperators["$bitsAnyClear"] = matchBits
	ExpressionQueryOperators["$bitsAnySet"] = matchBits
	ExpressionQueryOperators["$mod"] = matchMod
}

// Match will test if the specified document matches the supplied MongoDB query
// document.
func Match(doc, query bsonkit.Doc) (bool, error) {
	// match document to query
	err := Process(Context{
		TopLevel:   TopLevelQueryOperators,
		Expression: ExpressionQueryOperators,
	}, doc, *query, "", true)
	if err == ErrNotMatched {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return true, nil
}

func matchAnd(ctx Context, doc bsonkit.Doc, name, _ string, v interface{}) error {
	// get array
	array, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("%s: expected array", name)
	}

	// check array
	if len(array) == 0 {
		return fmt.Errorf("%s: empty array", name)
	}

	// match all expressions
	for _, item := range array {
		// coerce item
		query, ok := item.(bson.D)
		if !ok {
			return fmt.Errorf("%s: expected array of documents", name)
		}

		// match document
		err := Process(ctx, doc, query, "", true)
		if err != nil {
			return err
		}
	}

	return nil
}

func matchOr(ctx Context, doc bsonkit.Doc, name, _ string, v interface{}) error {
	// get array
	array, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("%s: expected array", name)
	}

	// check array
	if len(array) == 0 {
		return fmt.Errorf("%s: empty array", name)
	}

	// match first item
	for _, item := range array {
		// coerce item
		query, ok := item.(bson.D)
		if !ok {
			return fmt.Errorf("%s: expected array of documents", name)
		}

		// match document
		err := Process(ctx, doc, query, "", true)
		if err == ErrNotMatched {
			continue
		} else if err != nil {
			return err
		}

		return nil
	}

	return ErrNotMatched
}

func matchNor(ctx Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	return matchNegate(func() error {
		return matchOr(ctx, doc, name, path, v)
	})
}

func matchComp(_ Context, doc bsonkit.Doc, op, path string, v interface{}) error {
	return matchUnwind(doc, path, true, false, func(field interface{}) error {
		// determine if comparable (type bracketing)
		lc, _ := bsonkit.Inspect(field)
		rc, _ := bsonkit.Inspect(v)
		comp := lc == rc

		// compare field with value
		res := bsonkit.Compare(field, v)

		// check operator
		var ok bool
		switch op {
		case "", "$eq":
			ok = comp && res == 0
		case "$gt":
			ok = comp && res > 0
		case "$gte":
			ok = comp && res >= 0
		case "$lt":
			ok = comp && res < 0
		case "$lte":
			ok = comp && res <= 0
		default:
			return fmt.Errorf("unknown comparison operator %q", op)
		}
		if !ok {
			return ErrNotMatched
		}

		return nil
	})
}

func matchNot(ctx Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	// coerce item
	query, ok := v.(bson.D)
	if !ok {
		return fmt.Errorf("%s: expected document", name)
	}

	// check document
	if len(query) == 0 {
		return fmt.Errorf("%s: empty document", name)
	}

	// match all expressions
	for _, exp := range query {
		err := ProcessExpression(ctx, doc, path, exp, false)
		if err == ErrNotMatched {
			return nil
		} else if err != nil {
			return err
		}
	}

	// TODO: Support regular expressions.

	return ErrNotMatched
}

func matchIn(_ Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	return matchUnwind(doc, path, true, false, func(field interface{}) error {
		// get array
		array, ok := v.(bson.A)
		if !ok {
			return fmt.Errorf("%s: expected array", name)
		}

		// check if field is in array
		for _, item := range array {
			if bsonkit.Compare(field, item) == 0 {
				return nil
			}
		}

		// TODO: Support regular expressions.

		return ErrNotMatched
	})
}

func matchNin(ctx Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	return matchNegate(func() error {
		return matchIn(ctx, doc, name, path, v)
	})
}

func matchNe(ctx Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	return matchNegate(func() error {
		return matchComp(ctx, doc, "$eq", path, v)
	})
}

func matchExists(_ Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	// evaluate truthiness MongoDB-style: false, null and any numeric zero
	// are falsy; everything else is truthy
	exists := true
	switch n := v.(type) {
	case bool:
		exists = n
	case nil:
		exists = false
	case int32:
		exists = n != 0
	case int64:
		exists = n != 0
	case float64:
		exists = n != 0
	}

	// collect values along the path; All traverses arrays of subdocs and
	// drops Missing entries when compact is set, so a non-empty result means
	// at least one element along the path produced a value
	value, multi := bsonkit.All(doc, path, true, true)
	found := false
	if multi {
		if arr, ok := value.(bson.A); ok {
			found = len(arr) > 0
		} else {
			found = value != bsonkit.Missing
		}
	} else {
		found = value != bsonkit.Missing
	}

	if exists == found {
		return nil
	}

	return ErrNotMatched
}

func matchType(_ Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	// prepare operands
	var operands []interface{}
	if arr, ok := v.(bson.A); ok {
		if len(arr) == 0 {
			return fmt.Errorf("%s: must match at least one type", name)
		}
		operands = arr
	} else {
		operands = []interface{}{v}
	}

	// resolve types
	var matchNumberClass bool
	var wantTypes []bson.Type
	for _, operand := range operands {
		numberClass, typ, err := resolveType(name, operand)
		if err != nil {
			return err
		}
		if numberClass {
			matchNumberClass = true
		} else {
			wantTypes = append(wantTypes, typ)
		}
	}

	return matchUnwind(doc, path, true, false, func(field interface{}) error {
		class, typ := bsonkit.Inspect(field)
		if matchNumberClass && class == bsonkit.Number {
			return nil
		}
		for _, wantType := range wantTypes {
			if wantType == typ {
				return nil
			}
		}
		return ErrNotMatched
	})
}

func resolveType(name string, v interface{}) (bool, bson.Type, error) {
	switch value := v.(type) {
	case string:
		if value == "number" {
			return true, 0, nil
		}
		vt, ok := bsonkit.Alias2Type[value]
		if !ok {
			return false, 0, fmt.Errorf("%s: unknown type string", name)
		}
		return false, vt, nil
	case int32, int64, float64:
		// coerce to integer; reject fractional or out-of-range values
		var n int64
		switch nn := v.(type) {
		case int32:
			n = int64(nn)
		case int64:
			n = nn
		case float64:
			if nn != float64(int64(nn)) {
				return false, 0, fmt.Errorf("%s: expected integer", name)
			}
			n = int64(nn)
		}
		if n < 0 || n > 0xFF {
			return false, 0, fmt.Errorf("%s: type number out of range", name)
		}
		vt, ok := bsonkit.Number2Type[byte(n)]
		if !ok {
			return false, 0, fmt.Errorf("%s: unknown type number", name)
		}
		return false, vt, nil
	default:
		return false, 0, fmt.Errorf("%s: expected string or number", name)
	}
}

func matchJSONSchema(_ Context, doc bsonkit.Doc, name, _ string, v interface{}) error {
	// get doc
	d, ok := v.(bson.D)
	if !ok {
		return fmt.Errorf("%s: expected document", name)
	}

	// evaluate schema
	err := bsonkit.NewSchema(d).Evaluate(*doc)
	if err == bsonkit.ErrValidationFailed {
		return ErrNotMatched
	} else if err != nil {
		return fmt.Errorf("%s: %s", name, err.Error())
	}

	return nil
}

func matchAll(_ Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	return matchUnwind(doc, path, false, true, func(field interface{}) error {
		// get array
		array, ok := v.(bson.A)
		if !ok {
			return fmt.Errorf("%s: expected array", name)
		}

		// check array
		if len(array) == 0 {
			return ErrNotMatched
		}

		// check if array contains array
		if arr, ok := field.(bson.A); ok {
			matches := true
			for _, value := range array {
				ok := false
				for _, element := range arr {
					if bsonkit.Compare(value, element) == 0 {
						ok = true
					}
				}
				if !ok {
					matches = false
				}
			}
			if matches {
				return nil
			}
		}

		// check if field is in array
		for _, item := range array {
			if bsonkit.Compare(field, item) != 0 {
				return ErrNotMatched
			}
		}

		return nil
	})
}

func matchSize(_ Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	// require an integer value (int32, int64 or whole-valued float64)
	var size int64
	switch n := v.(type) {
	case int32:
		size = int64(n)
	case int64:
		size = n
	case float64:
		if n != float64(int64(n)) {
			return fmt.Errorf("%s: expected integer", name)
		}
		size = int64(n)
	default:
		return fmt.Errorf("%s: expected number", name)
	}

	// reject negative sizes
	if size < 0 {
		return fmt.Errorf("%s: size must be non-negative", name)
	}

	// get value (do not unwind: $size compares against the array at the path,
	// not its elements)
	value, multi := bsonkit.All(doc, path, false, false)

	// check each per-subdocument value when the path crossed a subdoc array
	if multi {
		arr, ok := value.(bson.A)
		if !ok {
			return ErrNotMatched
		}
		for _, item := range arr {
			if a, ok := item.(bson.A); ok && int64(len(a)) == size {
				return nil
			}
		}
		return ErrNotMatched
	}

	// compare length if array
	if arr, ok := value.(bson.A); ok && int64(len(arr)) == size {
		return nil
	}

	return ErrNotMatched
}

func matchElem(ctx Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	// get query
	query, ok := v.(bson.D)
	if !ok {
		return fmt.Errorf("%s: expected document", name)
	}

	// check query
	if len(query) == 0 {
		return ErrNotMatched
	}

	// get value
	value, _ := bsonkit.All(doc, path, true, true)

	// get array
	array, ok := value.(bson.A)
	if !ok {
		return ErrNotMatched
	}

	// match first item
	for _, item := range array {
		// prepare virtual doc
		virtual := bson.D{
			bson.E{Key: "item", Value: item},
		}

		// TODO: Block-list unsupported operators.

		// process virtual document
		err := Process(ctx, &virtual, query, "item", false)
		if err == ErrNotMatched {
			continue
		} else if err != nil {
			return err
		}

		return nil
	}

	return ErrNotMatched
}

func matchMod(_ Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	// get array
	array, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("%s: expected array", name)
	}

	// MongoDB requires exactly two elements: [divisor, remainder]
	if len(array) != 2 {
		return fmt.Errorf("%s: expected array of two elements", name)
	}

	// parse divisor and remainder; doubles are truncated toward zero
	divisor, err := modOperandToInt64(name, "divisor", array[0])
	if err != nil {
		return err
	}
	remainder, err := modOperandToInt64(name, "remainder", array[1])
	if err != nil {
		return err
	}

	// reject zero divisor
	if divisor == 0 {
		return fmt.Errorf("%s: divisor cannot be zero", name)
	}

	return matchUnwind(doc, path, true, false, func(field interface{}) error {
		// non-numeric or non-finite fields do not match
		n, ok := numberToInt64(field)
		if !ok {
			return ErrNotMatched
		}
		if n%divisor != remainder {
			return ErrNotMatched
		}
		return nil
	})
}

func modOperandToInt64(name, role string, v interface{}) (int64, error) {
	switch n := v.(type) {
	case int32:
		return int64(n), nil
	case int64:
		return n, nil
	case float64:
		if math.IsNaN(n) {
			return 0, fmt.Errorf("%s: %s cannot be NaN", name, role)
		}
		if math.IsInf(n, 0) {
			return 0, fmt.Errorf("%s: %s cannot be infinity", name, role)
		}
		// reject doubles that fall outside the int64 range; -float64(MinInt64)
		// is exactly 2^63, the smallest float strictly above MaxInt64
		if n < float64(math.MinInt64) || n >= -float64(math.MinInt64) {
			return 0, fmt.Errorf("%s: %s out of range", name, role)
		}
		return int64(math.Trunc(n)), nil
	default:
		return 0, fmt.Errorf("%s: %s must be a number", name, role)
	}
}

func numberToInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return 0, false
		}
		if n < float64(math.MinInt64) || n >= -float64(math.MinInt64) {
			return 0, false
		}
		return int64(math.Trunc(n)), true
	default:
		return 0, false
	}
}

func matchBits(_ Context, doc bsonkit.Doc, op, path string, v interface{}) error {
	// parse the bitmask once into a list of bit positions
	positions, err := parseBitMask(op, v)
	if err != nil {
		return err
	}

	return matchUnwind(doc, path, true, false, func(field interface{}) error {
		// resolve a per-position bit accessor for the field; non-numeric
		// and non-binary fields never match
		bitAt, ok := bitAccessor(field)
		if !ok {
			return ErrNotMatched
		}

		// count set bits across the requested positions
		set := 0
		for _, pos := range positions {
			if bitAt(pos) {
				set++
			}
		}
		clear := len(positions) - set

		var matched bool
		switch op {
		case "$bitsAllSet":
			matched = set == len(positions)
		case "$bitsAllClear":
			matched = clear == len(positions)
		case "$bitsAnySet":
			matched = set > 0
		case "$bitsAnyClear":
			matched = clear > 0
		default:
			return fmt.Errorf("unknown bits operator %q", op)
		}
		if !matched {
			return ErrNotMatched
		}
		return nil
	})
}

func parseBitMask(name string, v interface{}) ([]uint, error) {
	switch m := v.(type) {
	case int32:
		if m < 0 {
			return nil, fmt.Errorf("%s: bitmask must be non-negative", name)
		}
		return uint64ToPositions(uint64(m)), nil
	case int64:
		if m < 0 {
			return nil, fmt.Errorf("%s: bitmask must be non-negative", name)
		}
		return uint64ToPositions(uint64(m)), nil
	case float64:
		if math.IsNaN(m) || math.IsInf(m, 0) || m != math.Floor(m) {
			return nil, fmt.Errorf("%s: expected integer", name)
		}
		if m < 0 {
			return nil, fmt.Errorf("%s: bitmask must be non-negative", name)
		}
		if m > math.MaxInt64 {
			return nil, fmt.Errorf("%s: bitmask out of range", name)
		}
		return uint64ToPositions(uint64(m)), nil
	case bson.A:
		positions := make([]uint, 0, len(m))
		for _, item := range m {
			pos, err := bitPosition(name, item)
			if err != nil {
				return nil, err
			}
			positions = append(positions, pos)
		}
		return positions, nil
	case bson.Binary:
		positions := make([]uint, 0)
		for byteIdx, b := range m.Data {
			for bitIdx := uint(0); bitIdx < 8; bitIdx++ {
				if b&(1<<bitIdx) != 0 {
					positions = append(positions, uint(byteIdx)*8+bitIdx)
				}
			}
		}
		return positions, nil
	default:
		return nil, fmt.Errorf("%s: expected number, array, or binary", name)
	}
}

func bitPosition(name string, v interface{}) (uint, error) {
	switch n := v.(type) {
	case int32:
		if n < 0 {
			return 0, fmt.Errorf("%s: bit position must be non-negative", name)
		}
		return uint(n), nil
	case int64:
		if n < 0 {
			return 0, fmt.Errorf("%s: bit position must be non-negative", name)
		}
		return uint(n), nil
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) || n != math.Floor(n) {
			return 0, fmt.Errorf("%s: expected integer bit position", name)
		}
		if n < 0 {
			return 0, fmt.Errorf("%s: bit position must be non-negative", name)
		}
		return uint(n), nil
	default:
		return 0, fmt.Errorf("%s: bit position must be a number", name)
	}
}

func uint64ToPositions(v uint64) []uint {
	positions := make([]uint, 0)
	for i := uint(0); i < 64; i++ {
		if v&(1<<i) != 0 {
			positions = append(positions, i)
		}
	}
	return positions
}

func bitAccessor(field interface{}) (func(uint) bool, bool) {
	switch f := field.(type) {
	case int32:
		v := uint64(int64(f))
		return func(pos uint) bool {
			if pos >= 64 {
				return false
			}
			return v&(1<<pos) != 0
		}, true
	case int64:
		v := uint64(f)
		return func(pos uint) bool {
			if pos >= 64 {
				return false
			}
			return v&(1<<pos) != 0
		}, true
	case float64:
		// non-integral, NaN or infinity values do not match
		if math.IsNaN(f) || math.IsInf(f, 0) || f != math.Floor(f) {
			return nil, false
		}
		// out-of-range values do not match; -float64(MinInt64) is exactly
		// 2^63 (the smallest float strictly above MaxInt64)
		if f < float64(math.MinInt64) || f >= -float64(math.MinInt64) {
			return nil, false
		}
		v := uint64(int64(f))
		return func(pos uint) bool {
			if pos >= 64 {
				return false
			}
			return v&(1<<pos) != 0
		}, true
	case bson.Binary:
		data := f.Data
		return func(pos uint) bool {
			byteIdx := pos / 8
			if int(byteIdx) >= len(data) {
				return false
			}
			return data[byteIdx]&(1<<(pos%8)) != 0
		}, true
	default:
		return nil, false
	}
}

func matchUnwind(doc bsonkit.Doc, path string, merge, yieldMerge bool, op func(interface{}) error) error {
	// get value
	value, multi := bsonkit.All(doc, path, true, merge)
	if arr, ok := value.(bson.A); ok {
		for _, field := range arr {
			err := op(field)
			if err == ErrNotMatched {
				continue
			} else if err != nil {
				return err
			}

			return nil
		}
	}

	// match value
	if !multi || yieldMerge {
		return op(value)
	}

	return ErrNotMatched
}

func matchNegate(op func() error) error {
	err := op()
	if err == ErrNotMatched {
		return nil
	} else if err != nil {
		return err
	}

	return ErrNotMatched
}
