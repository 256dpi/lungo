package bsonkit

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
)

// MissingType is the type of the Missing value.
type MissingType struct{}

// Missing represents the absence of a value in a document.
var Missing = MissingType{}

// Get returns the value in the document specified by path. It returns Missing
// if the value has not been found. Dots may be used to descend into nested
// documents e.g. "foo.bar.baz" and numbers may be used to descend into arrays
// e.g. "foo.2.bar".
func Get(doc Doc, path string) interface{} {
	value, _ := get(*doc, path, false, false, false)
	return value
}

// GetLax is the same as Get but supports non-strict BSON types.
func GetLax(v interface{}, path string) interface{} {
	value, _ := get(v, path, false, false, true)
	return value
}

// All has the basic behaviour as Get but additionally collects values from
// embedded documents in arrays. It returns and array and true if values from
// multiple documents, haven been collected. Missing values are skipped and
// intermediary arrays flattened if compact is set to true. By enabling merge,
// a resulting array of embedded document may be merged to on array containing
// all values.
func All(doc Doc, path string, compact, merge bool) (interface{}, bool) {
	return all(*doc, path, compact, merge, false)
}

// AllLax is the same as All but supports non-strict BSON types.
func AllLax(v interface{}, path string, compact, merge bool) (interface{}, bool) {
	return all(v, path, compact, merge, true)
}

func all(v interface{}, path string, compact, merge, lax bool) (interface{}, bool) {
	// get value
	value, nested := get(v, path, true, compact, lax)
	if !nested || !merge {
		return value, nested
	}

	// get array
	array, ok := value.(bson.A)
	if !ok {
		return value, nested
	}

	// prepare result
	result := make(bson.A, 0, len(array))

	// merge arrays
	for _, item := range array {
		if a, ok := item.(bson.A); ok {
			result = append(result, a...)
		} else {
			result = append(result, item)
		}
	}

	return result, nested
}

func get(v interface{}, path string, collect, compact, lax bool) (interface{}, bool) {
	// check path
	if path == PathEnd {
		return v, false
	}

	// check if empty
	if path == "" {
		return Missing, false
	}

	// get key
	key := PathSegment(path)

	// handle document
	if doc, ok := v.(bson.D); ok {
		for _, el := range doc {
			if el.Key == key {
				return get(el.Value, ReducePath(path), collect, compact, lax)
			}
		}
	}

	// handle array
	if arr, ok := v.(bson.A); ok {
		// get indexed array element if number
		if index, ok := ParseIndex(key); ok {
			if index >= 0 && index < len(arr) {
				return get(arr[index], ReducePath(path), collect, compact, lax)
			}
		}

		// collect values from embedded documents
		if collect {
			res := make(bson.A, 0, len(arr))
			for _, item := range arr {
				value, ok := get(item, path, collect, compact, lax)
				if value == Missing && !compact {
					res = append(res, value)
				} else if value != Missing {
					if ok && compact {
						res = append(res, value.(bson.A)...)
					} else {
						res = append(res, value)
					}
				}
			}
			return res, true
		}
	}

	// check lax
	if !lax {
		return Missing, false
	}

	// get value
	value := reflect.ValueOf(v)

	// unwrap pointers
	for value.Kind() == reflect.Pointer {
		value = value.Elem()
	}

	// handle arrays and slices
	if value.Kind() == reflect.Array || value.Kind() == reflect.Slice {
		// get indexed array element if number
		if index, ok := ParseIndex(key); ok {
			if index >= 0 && index < value.Len() {
				return get(value.Index(index).Interface(), ReducePath(path), collect, compact, lax)
			}
		}

		// collect values from embedded documents
		if collect {
			res := make(bson.A, 0, value.Len())
			for i := 0; i < value.Len(); i++ {
				el, ok := get(value.Index(i).Interface(), path, collect, compact, lax)
				if el == Missing && !compact {
					res = append(res, el)
				} else if el != Missing {
					if ok && compact {
						res = append(res, el.(bson.A)...)
					} else {
						res = append(res, el)
					}
				}
			}
			return res, true
		}
	}

	// handle map
	if value.Kind() == reflect.Map {
		if value.Type().Key() != reflect.TypeOf("") {
			return Missing, false
		}
		res := value.MapIndex(reflect.ValueOf(key))
		if res == (reflect.Value{}) {
			return Missing, false
		}
		return get(res.Interface(), ReducePath(path), collect, compact, lax)
	}

	// handle structs
	if value.Kind() == reflect.Struct {
		index, ok := structFieldIndex(value.Type(), key)
		if ok {
			return get(value.Field(index).Interface(), ReducePath(path), collect, compact, lax)
		}
	}

	return Missing, false
}

// Put will store the value in the document at the location specified by path
// and return the previously stored value. It will automatically create document
// fields, array elements and embedded documents to fulfill the request. If
// prepends is set to true, new values are inserted at the beginning of the array
// or document. If the path contains a number e.g. "foo.1.bar" and no array
// exists at that levels, a document with the key "1" is created.
func Put(doc Doc, path string, value interface{}, prepend bool) (interface{}, error) {
	// check value
	if value == Missing {
		return nil, fmt.Errorf("cannot put missing value at %s", path)
	}

	// put value
	res, ok := put(*doc, path, value, prepend, false, func(v interface{}) {
		*doc = v.(bson.D)
	})
	if !ok {
		return nil, fmt.Errorf("cannot put value at %s", path)
	}

	return res, nil
}

// PutLax is the same as Put but supports non-strict BSON types.
func PutLax(v interface{}, path string, value interface{}, prepend bool) (interface{}, error) {
	// check nil
	if v == nil || reflect.ValueOf(v).IsNil() {
		panic("bsonkit: unexpected nil value")
	}

	// check value
	if value == Missing {
		return nil, fmt.Errorf("cannot put missing value at %s", path)
	}

	// put value
	res, ok := put(v, path, value, prepend, true, func(interface{}) {
		// ignore
	})
	if !ok {
		return nil, fmt.Errorf("cannot put value at %s", path)
	}

	return res, nil
}

// Unset will remove the value at the location in the document specified by path
// and return the previously stored value. If the path specifies an array element
// e.g. "foo.2" the element is nilled, but not removed from the array. This
// prevents unintentional effects through position shifts in the array.
func Unset(doc Doc, path string) interface{} {
	// unset value
	res, _ := put(*doc, path, Missing, false, false, func(v interface{}) {
		*doc = v.(bson.D)
	})

	return res
}

// UnsetLax is the same as Unset but supports non-strict BSON types.
func UnsetLax(v interface{}, path string) interface{} {
	// check nil
	if v == nil || reflect.ValueOf(v).IsNil() {
		panic("bsonkit: unexpected nil value")
	}

	// unset value
	res, _ := put(v, path, Missing, false, true, func(interface{}) {
		// ignore
	})

	return res
}

func put(v interface{}, path string, value interface{}, prepend, lax bool, set func(interface{})) (interface{}, bool) {
	// check path
	if path == PathEnd {
		set(value)
		return v, true
	}

	// check if empty
	if path == "" {
		return Missing, false
	}

	// get key
	key := PathSegment(path)

	// handle documents
	if doc, ok := v.(bson.D); ok {
		// update existing field
		for i, el := range doc {
			if el.Key == key {
				return put(doc[i].Value, ReducePath(path), value, prepend, lax, func(v interface{}) {
					if v == Missing {
						set(append(doc[:i], doc[i+1:]...))
					} else {
						doc[i].Value = v
					}
				})
			}
		}

		// check if unset
		if value == Missing {
			return Missing, false
		}

		// prepare field
		e := bson.E{Key: key}
		res, ok := put(Missing, ReducePath(path), value, prepend, lax, func(v interface{}) {
			e.Value = v
		})
		if !ok {
			return res, false
		}

		// set appended/prepended document
		if prepend {
			set(append(bson.D{e}, doc...))
		} else {
			set(append(doc, e))
		}

		return Missing, true
	}

	// handle arrays
	if arr, ok := v.(bson.A); ok {
		// parse index
		index, err := strconv.Atoi(key)
		if err != nil || index < 0 {
			return Missing, false
		}

		// update existing element
		if index < len(arr) {
			return put(arr[index], ReducePath(path), value, prepend, lax, func(v interface{}) {
				if v == Missing {
					arr[index] = nil
				} else {
					arr[index] = v
				}
			})
		}

		// check if unset
		if value == Missing {
			return Missing, false
		}

		// fill with nil elements
		for i := len(arr); i < index+1; i++ {
			arr = append(arr, nil)
		}

		// put in last element
		res, ok := put(Missing, ReducePath(path), value, prepend, lax, func(v interface{}) {
			arr[index] = v
		})
		if !ok {
			return res, false
		}

		// set array
		set(arr)

		return Missing, true
	}

	if lax {
		// get value
		vv := reflect.ValueOf(v)

		// ensure pointers
		for vv.Kind() == reflect.Pointer {
			if vv.IsNil() {
				if value == Missing {
					return Missing, false
				}
				vv = reflect.New(vv.Type().Elem())
				set(vv.Interface())
			}
			vv = vv.Elem()
		}

		// handle arrays sand slices
		if vv.Kind() == reflect.Array || vv.Kind() == reflect.Slice {
			// parse index
			index, err := strconv.Atoi(key)
			if err != nil || index < 0 {
				return Missing, false
			}

			// update existing element
			if index < vv.Len() {
				return put(vv.Index(index).Interface(), ReducePath(path), value, prepend, lax, func(v interface{}) {
					if v == Missing {
						vv.Index(index).Set(reflect.Zero(vv.Type().Elem()))
					} else {
						vv.Index(index).Set(reflect.ValueOf(v))
					}
				})
			}

			// check array
			if vv.Kind() == reflect.Array {
				return Missing, false
			}

			// check if unset
			if value == Missing {
				return Missing, false
			}

			// fill with zero elements
			for i := vv.Len(); i < index+1; i++ {
				vv = reflect.Append(vv, reflect.Zero(vv.Type().Elem()))
			}

			// put in last element
			res, ok := put(Missing, ReducePath(path), value, prepend, lax, func(v interface{}) {
				vv.Index(index).Set(reflect.ValueOf(v))
			})
			if !ok {
				return res, false
			}

			// set array
			set(vv.Interface())

			return Missing, true
		}

		// handle maps
		if vv.Kind() == reflect.Map {
			// ensure map
			if vv.IsNil() {
				if value == Missing {
					return Missing, false
				}
				vv = reflect.MakeMap(vv.Type())
				set(vv.Interface())
			}

			// prepare value
			v := vv.MapIndex(reflect.ValueOf(key))
			if v == (reflect.Value{}) {
				v = reflect.Zero(vv.Type().Elem())
			}

			return put(v.Interface(), ReducePath(path), value, prepend, lax, func(v interface{}) {
				if v == Missing {
					vv.SetMapIndex(reflect.ValueOf(key), reflect.Value{})
				} else {
					vv.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(v))
				}
			})
		}

		// handle structs
		if vv.Kind() == reflect.Struct {
			// get access
			index, ok := structFieldIndex(vv.Type(), key)
			if !ok {
				return Missing, false
			}

			// update field
			return put(vv.Field(index).Interface(), ReducePath(path), value, prepend, lax, func(v interface{}) {
				if v == Missing {
					vv.Field(index).SetZero()
				} else {
					vv.Field(index).Set(reflect.ValueOf(v))
				}
			})
		}
	}

	// check if unset
	if value == Missing {
		return Missing, false
	}

	// put new document
	if v == Missing {
		// capture value
		e := bson.E{Key: key}
		res, ok := put(Missing, ReducePath(path), value, prepend, lax, func(v interface{}) {
			e.Value = v
		})
		if !ok {
			return res, false
		}

		// set document
		set(bson.D{e})

		return Missing, true
	}

	return Missing, false
}

// Increment will add the increment to the value at the location in the document
// specified by path and return the new value. If the value is missing, the
// increment is added to the document. The type of the field may be changed as
// part of the operation.
func Increment(doc Doc, path string, increment interface{}) (interface{}, error) {
	// get field
	field := Get(doc, path)

	// ensure zero
	if field == Missing {
		field = int32(0)
	}

	// increment field
	field = Add(field, increment)
	if field == Missing {
		return nil, fmt.Errorf("incrementee or increment is not a number")
	}

	// update field
	_, err := Put(doc, path, field, false)
	if err != nil {
		return nil, err
	}

	return field, nil
}

// Multiply will multiply the multiplier with the value at the location in the
// document specified by path and return the new value. If the value is missing,
// a zero is added to the document. The type of the field may be changed as part
// of the operation.
func Multiply(doc Doc, path string, multiplier interface{}) (interface{}, error) {
	// get field
	field := Get(doc, path)

	// ensure zero
	if field == Missing {
		field = int32(0)
	}

	// multiply
	field = Mul(field, multiplier)
	if field == Missing {
		return nil, fmt.Errorf("multiplicand or multiplier is not a number")
	}

	// update field
	_, err := Put(doc, path, field, false)
	if err != nil {
		return nil, err
	}

	return field, nil
}

// Push will add the value to the array at the location in the document
// specified by path and return the new value. If the value is missing, the
// value is added to a new array.
func Push(doc Doc, path string, value interface{}) (interface{}, error) {
	// check value
	if value == Missing {
		return nil, fmt.Errorf("cannot push missing value at %s", path)
	}

	// get field
	field := Get(doc, path)

	// push field
	switch val := field.(type) {
	case bson.A:
		field = append(val, value)
	case MissingType:
		field = bson.A{value}
	default:
		return nil, fmt.Errorf("value at path %q is not an array", path)
	}

	// update field
	_, err := Put(doc, path, field, false)
	if err != nil {
		return nil, err
	}

	return field, nil
}

// Pop will remove the first or last element from the array at the location in
// the document specified byt path and return the updated array. If the array is
// empty, the value is missing or not an array, it will do nothing and return
// Missing.
func Pop(doc Doc, path string, last bool) (interface{}, error) {
	// get field
	field := Get(doc, path)

	// check if missing
	if field == Missing {
		return Missing, nil
	}

	// get and check array
	array, ok := field.(bson.A)
	if !ok {
		return nil, fmt.Errorf("value at path %q is not an array", path)
	}

	// check length
	if len(array) == 0 {
		return Missing, nil
	}

	// pop last or first value
	var res interface{}
	if last {
		res = array[len(array)-1]
		field = array[:len(array)-1]
	} else {
		res = array[0]
		field = array[1:]
	}

	// update field
	_, err := Put(doc, path, field, false)
	if err != nil {
		return nil, err
	}

	return res, nil
}

var structFieldsMutex sync.Mutex
var structFieldsCache = map[reflect.Type]map[string]int{}

func structFieldIndex(typ reflect.Type, key string) (int, bool) {
	// acquire mutex
	structFieldsMutex.Lock()
	defer structFieldsMutex.Unlock()

	// check type
	if typ.Kind() != reflect.Struct {
		panic("bsonkit: unexpected type")
	}

	// check cache
	fields := structFieldsCache[typ]
	if fields != nil {
		index, ok := fields[key]
		return index, ok
	}

	// create fields
	fields = map[string]int{}

	// add struct field names
	for i := 0; i < typ.NumField(); i++ {
		fields[typ.Field(i).Name] = i
	}

	// add struct field keys
	for i := 0; i < typ.NumField(); i++ {
		// get field
		field := typ.Field(i)

		// determine BSON key
		key := field.Tag.Get("bson")
		sep := strings.IndexByte(key, ',')
		if sep >= 0 {
			key = key[:sep]
		}
		if key == "" {
			key = strings.ToLower(field.Name)
		}

		// add key
		if key != "-" {
			fields[key] = i
		}
	}

	// save fields
	structFieldsCache[typ] = fields

	// check key
	index, ok := fields[key]

	return index, ok
}
