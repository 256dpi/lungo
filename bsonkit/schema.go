package bsonkit

import (
	"fmt"
	"regexp"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var jsonTypeClass = map[string]Class{
	"null":    Null,
	"boolean": Boolean,
	"number":  Number,
	"string":  String,
	"object":  Document,
	"array":   Array,
}

// ErrValidationFailed is returned when the value failed to validate against
// the schema.
var ErrValidationFailed = fmt.Errorf("validation failed")

// Schema implements the JSON Schema validation algorithm. Specifically, it
// implements the following specifications:
//
// - https://docs.mongodb.com/manual/reference/operator/query/jsonSchema
// - https://tools.ietf.org/html/draft-zyp-json-schema-04
// - https://tools.ietf.org/html/draft-fge-json-schema-validation-00
//
// Note: The Go regex engine ist not ECMA 262 compatible as required by the
// draft.
type Schema struct {
	// The schema document.
	Doc bson.D

	// The regex cache.
	Regexes map[string]*regexp.Regexp
}

// NewSchema will create and return a new schema from the provided document.
func NewSchema(doc bson.D) *Schema {
	return &Schema{
		Doc:     doc,
		Regexes: map[string]*regexp.Regexp{},
	}
}

var emptySchema = NewSchema(bson.D{})

// Evaluate will evaluate the schema against the provided value. It returns
// ErrValidationFailed if the validation failed or another error if the schema
// is invalid or validation cannot be performed.
func (s *Schema) Evaluate(value interface{}) error {
	// inspect
	valueClass, valueType := Inspect(value)

	// evaluate generic keywords
	err := s.evaluateGeneric(value, valueClass, valueType)
	if err != nil {
		return err
	}

	// check number
	if valueClass == Number {
		return s.evaluateNumber(value)
	}

	// check string
	if valueClass == String {
		return s.evaluateString(value.(string))
	}

	// check document
	if valueClass == Document {
		return s.evaluateDocument(value.(bson.D))
	}

	// check array
	if valueClass == Array {
		return s.evaluateArray(value.(bson.A))
	}

	return nil
}

func (s *Schema) evaluateGeneric(value interface{}, valueClass Class, valueType bsontype.Type) error {
	// pre-check exclusion
	if Get(&s.Doc, "type") != Missing && Get(&s.Doc, "bsonType") != Missing {
		return fmt.Errorf("schema cannot contain type and bsonType")
	}

	// evaluate generic keywords
	for _, keyword := range s.Doc {
		switch keyword.Key {
		case "type":
			switch kv := keyword.Value.(type) {
			case string:
				class, ok := jsonTypeClass[kv]
				if !ok {
					return fmt.Errorf("invalid type name: %s", kv)
				}
				if valueClass != class {
					return ErrValidationFailed
				}
			case bson.A:
				if len(kv) == 0 {
					return fmt.Errorf("invalid type value: %v", kv)
				}
				var valid bool
				for _, t := range kv {
					switch t := t.(type) {
					case string:
						class, ok := jsonTypeClass[t]
						if !ok {
							return fmt.Errorf("invalid type name: %s", t)
						}
						if valueClass == class {
							valid = true
						}
					default:
						return fmt.Errorf("invalid type element: %v", t)
					}
				}
				if !valid {
					return ErrValidationFailed
				}
			default:
				return fmt.Errorf("invalid type value: %v", kv)
			}

		case "bsonType":
			switch kv := keyword.Value.(type) {
			case string:
				if kv == "number" {
					if valueClass != Number {
						return ErrValidationFailed
					}
				} else {
					typ, ok := Alias2Type[kv]
					if !ok {
						return fmt.Errorf("invalid bsonType alias: %s", kv)
					}
					if valueType != typ {
						return ErrValidationFailed
					}
				}
			case bson.A:
				if len(kv) == 0 {
					return fmt.Errorf("invalid bsonType value: %v", kv)
				}
				var valid bool
				for _, t := range kv {
					switch t := t.(type) {
					case string:
						if t == "number" {
							if valueClass == Number {
								valid = true
							}
						} else {
							typ, ok := Alias2Type[t]
							if !ok {
								return fmt.Errorf("invalid bsonType alias: %s", t)
							}
							if valueType == typ {
								valid = true
							}
						}
					default:
						return fmt.Errorf("invalid bsonType element: %v", t)
					}
				}
				if !valid {
					return ErrValidationFailed
				}
			default:
				return fmt.Errorf("invalid bsonType value: %v", kv)
			}
		case "enum":
			switch kv := keyword.Value.(type) {
			case bson.A:
				if len(kv) == 0 {
					return fmt.Errorf("invalid enum value: %v", kv)
				}
				var ok bool
				for _, enum := range kv {
					if Compare(enum, value) == 0 {
						ok = true
					}
				}
				if !ok {
					return ErrValidationFailed
				}
			default:
				return fmt.Errorf("invalid enum value: %v", kv)
			}
		case "allOf":
			switch kv := keyword.Value.(type) {
			case bson.A:
				if len(kv) == 0 {
					return fmt.Errorf("invalid allOf value: %v", kv)
				}
				for _, schema := range kv {
					switch schema := schema.(type) {
					case bson.D:
						err := NewSchema(schema).Evaluate(value)
						if err != nil {
							return err
						}
					default:
						return fmt.Errorf("invalid allOf element: %v", schema)
					}
				}
			default:
				return fmt.Errorf("invalid allOf value: %v", kv)
			}
		case "anyOf":
			switch kv := keyword.Value.(type) {
			case bson.A:
				if len(kv) == 0 {
					return fmt.Errorf("invalid anyOf value: %v", kv)
				}
				var ok bool
				for _, schema := range kv {
					switch schema := schema.(type) {
					case bson.D:
						err := NewSchema(schema).Evaluate(value)
						if err != nil && err != ErrValidationFailed {
							return err
						} else if err == nil {
							ok = true
						}
					default:
						return fmt.Errorf("invalid anyOf element: %v", schema)
					}
				}
				if !ok {
					return ErrValidationFailed
				}
			default:
				return fmt.Errorf("invalid anyOf value: %v", kv)
			}
		case "oneOf":
			switch kv := keyword.Value.(type) {
			case bson.A:
				if len(kv) == 0 {
					return fmt.Errorf("invalid oneOf value: %v", kv)
				}
				var ok int
				for _, schema := range kv {
					switch schema := schema.(type) {
					case bson.D:
						err := NewSchema(schema).Evaluate(value)
						if err != nil && err != ErrValidationFailed {
							return err
						} else if err == nil {
							ok++
						}
					default:
						return fmt.Errorf("invalid oneOf element: %v", schema)
					}
				}
				if ok != 1 {
					return ErrValidationFailed
				}
			default:
				return fmt.Errorf("invalid oneOf value: %v", kv)
			}
		case "not":
			switch schema := keyword.Value.(type) {
			case bson.D:
				err := NewSchema(schema).Evaluate(value)
				if err != nil && err != ErrValidationFailed {
					return err
				} else if err == nil {
					return ErrValidationFailed
				}
			default:
				return fmt.Errorf("invalid not value: %v", schema)
			}
		}
	}

	return nil
}

func (s *Schema) evaluateNumber(num interface{}) error {
	// preflight number keywords
	var exclusiveMinimum bool
	var exclusiveMaximum bool
	for _, keyword := range s.Doc {
		switch keyword.Key {
		case "exclusiveMinimum":
			if val, ok := keyword.Value.(bool); ok {
				exclusiveMinimum = val
			} else {
				return fmt.Errorf("invalid exclusiveMinimum value: %v", keyword.Value)
			}
			if Get(&s.Doc, "minimum") == Missing {
				return fmt.Errorf("exclusiveMinimum requires minimum")
			}
		case "exclusiveMaximum":
			if val, ok := keyword.Value.(bool); ok {
				exclusiveMaximum = val
			} else {
				return fmt.Errorf("invalid exclusiveMaximum value: %v", keyword.Value)
			}
			if Get(&s.Doc, "maximum") == Missing {
				return fmt.Errorf("exclusiveMaximum requires maximum")
			}
		}
	}

	// evaluate number keywords
	for _, keyword := range s.Doc {
		switch keyword.Key {
		case "multipleOf":
			switch kv := keyword.Value.(type) {
			case int32, int64, float64, primitive.Decimal128:
				if Compare(kv, int32(0)) <= 0 {
					return fmt.Errorf("invalid multipleOf value: %v", kv)
				}
				if Compare(Mod(num, kv), int32(0)) != 0 {
					return ErrValidationFailed
				}
			default:
				return fmt.Errorf("invalid multipleOf value: %v", kv)
			}
		case "minimum":
			switch kv := keyword.Value.(type) {
			case int32, int64, float64, primitive.Decimal128:
				res := Compare(num, kv)
				if exclusiveMinimum && res <= 0 {
					return ErrValidationFailed
				} else if !exclusiveMinimum && res < 0 {
					return ErrValidationFailed
				}
			default:
				return fmt.Errorf("invalid minimum value: %v", kv)
			}
		case "maximum":
			switch kv := keyword.Value.(type) {
			case int32, int64, float64, primitive.Decimal128:
				res := Compare(num, kv)
				if exclusiveMaximum && res >= 0 {
					return ErrValidationFailed
				} else if !exclusiveMaximum && res > 0 {
					return ErrValidationFailed
				}
			default:
				return fmt.Errorf("invalid maximum value: %v", kv)
			}
		}
	}

	return nil
}

func (s *Schema) evaluateString(str string) error {
	// evaluate string keywords
	for _, keyword := range s.Doc {
		switch keyword.Key {
		case "minLength":
			switch kv := keyword.Value.(type) {
			case int32, int64:
				if Compare(kv, int32(0)) < 0 {
					return fmt.Errorf("invalid minLength value: %v", kv)
				}
				if Compare(int64(len(str)), kv) < 0 {
					return ErrValidationFailed
				}
			default:
				return fmt.Errorf("invalid minLength value: %v", kv)
			}
		case "maxLength":
			switch kv := keyword.Value.(type) {
			case int32, int64:
				if Compare(kv, int32(0)) < 0 {
					return fmt.Errorf("invalid maxLength value: %v", kv)
				}
				if Compare(int64(len(str)), kv) > 0 {
					return ErrValidationFailed
				}
			default:
				return fmt.Errorf("invalid maxLength value: %v", kv)
			}
		case "pattern":
			switch kv := keyword.Value.(type) {
			case string:
				regex, err := s.compileRegex(kv)
				if err != nil {
					return fmt.Errorf("invalid pattern regex: %v", kv)
				}
				if !regex.MatchString(str) {
					return ErrValidationFailed
				}
			default:
				return fmt.Errorf("invalid pattern value: %v", kv)
			}
		}
	}

	return nil
}

func (s *Schema) evaluateDocument(doc bson.D) error {
	// evaluate stateless object keywords
	for _, keyword := range s.Doc {
		switch keyword.Key {
		case "required":
			switch kv := keyword.Value.(type) {
			case bson.A:
				if len(kv) == 0 {
					return fmt.Errorf("invalid required value: %v", kv)
				}
				for _, item := range kv {
					switch item := item.(type) {
					case string:
						if Get(&doc, item) == Missing {
							return ErrValidationFailed
						}
					default:
						return fmt.Errorf("invalid required element: %v", item)
					}
				}
			default:
				return fmt.Errorf("invalid required value: %v", kv)
			}
		case "minProperties":
			switch kv := keyword.Value.(type) {
			case int32, int64:
				if Compare(kv, int32(0)) < 0 {
					return fmt.Errorf("invalid minProperties value: %v", kv)
				}
				if Compare(int64(len(doc)), kv) < 0 {
					return ErrValidationFailed
				}
			default:
				return fmt.Errorf("invalid minProperties value: %v", kv)
			}
		case "maxProperties":
			switch kv := keyword.Value.(type) {
			case int32, int64:
				if Compare(kv, int32(0)) < 0 {
					return fmt.Errorf("invalid maxProperties value: %v", kv)
				}
				if Compare(int64(len(doc)), kv) > 0 {
					return ErrValidationFailed
				}
			default:
				return fmt.Errorf("invalid maxProperties value: %v", kv)
			}
		case "dependencies":
			switch kv := keyword.Value.(type) {
			case bson.D:
				for _, item := range kv {
					switch v := item.Value.(type) {
					case bson.D:
						if Get(&doc, item.Key) != Missing {
							err := NewSchema(v).Evaluate(doc)
							if err != nil {
								return err
							}
						}
					case bson.A:
						if len(v) == 0 {
							return fmt.Errorf("invalid dependencies element: %v", item)
						}
						for _, p := range v {
							switch p := p.(type) {
							case string:
								if Get(&doc, p) == Missing {
									return ErrValidationFailed
								}
							default:
								return fmt.Errorf("invalid dependencies element value: %v", p)
							}
						}
					default:
						return fmt.Errorf("invalid dependencies element: %v", item)
					}
				}
			default:
				return fmt.Errorf("invalid dependencies value: %v", kv)
			}
		}
	}

	// pre-flight stateful object keywords
	properties := map[string]*Schema{}
	patternProperties := map[*regexp.Regexp]*Schema{}
	additionalProperties := emptySchema
	for _, keyword := range s.Doc {
		switch keyword.Key {
		case "properties":
			switch kv := keyword.Value.(type) {
			case bson.D:
				for _, v := range kv {
					switch s := v.Value.(type) {
					case bson.D:
						properties[v.Key] = NewSchema(s)
					default:
						return fmt.Errorf("invalid properties element: %v", v)
					}
				}
			default:
				return fmt.Errorf("invalid properties value: %v", kv)
			}
		case "patternProperties":
			switch kv := keyword.Value.(type) {
			case bson.D:
				for _, v := range kv {
					regex, err := s.compileRegex(v.Key)
					if err != nil {
						return fmt.Errorf("invalid patternProperties regex: %v", v.Key)
					}
					switch s := v.Value.(type) {
					case bson.D:
						patternProperties[regex] = NewSchema(s)
					default:
						return fmt.Errorf("invalid patternProperties element: %v", v)
					}
				}
			default:
				return fmt.Errorf("invalid patternProperties value: %v", kv)
			}
		case "additionalProperties":
			switch kv := keyword.Value.(type) {
			case bool:
				if !kv {
					additionalProperties = nil
				}
			case bson.D:
				additionalProperties = NewSchema(kv)
			default:
				return fmt.Errorf("invalid additionalProperties value: %v", kv)
			}
		}
	}

	// evaluate stateful object keywords
	for _, member := range doc {
		// prepare schemas
		var schemas []*Schema

		// check properties
		if schema := properties[member.Key]; schema != nil {
			schemas = append(schemas, schema)
		}

		// check pattern properties
		for regex, schema := range patternProperties {
			if regex.MatchString(member.Key) {
				schemas = append(schemas, schema)
			}
		}

		// check additional properties
		if len(schemas) == 0 {
			if additionalProperties != nil {
				schemas = append(schemas, additionalProperties)
			} else {
				return ErrValidationFailed
			}
		}

		// evaluate schemas
		for _, schema := range schemas {
			err := schema.Evaluate(member.Value)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Schema) evaluateArray(arr bson.A) error {
	// preflight array keywords
	additionalItems := emptySchema
	for _, keyword := range s.Doc {
		switch keyword.Key {
		case "additionalItems":
			switch kv := keyword.Value.(type) {
			case bool:
				if !kv {
					additionalItems = nil
				}
			case bson.D:
				additionalItems = NewSchema(kv)
			default:
				return fmt.Errorf("invalid additionalItems value: %v", kv)
			}
		}
	}

	// evaluate array keywords
	for _, keyword := range s.Doc {
		switch keyword.Key {
		case "minItems":
			switch kv := keyword.Value.(type) {
			case int32, int64:
				if Compare(kv, int32(0)) < 0 {
					return fmt.Errorf("invalid minItems value: %v", kv)
				}
				if Compare(int64(len(arr)), kv) < 0 {
					return ErrValidationFailed
				}
			default:
				return fmt.Errorf("invalid minItems value: %v", kv)
			}
		case "maxItems":
			switch kv := keyword.Value.(type) {
			case int32, int64:
				if Compare(kv, int32(0)) < 0 {
					return fmt.Errorf("invalid maxItems value: %v", kv)
				}
				if Compare(int64(len(arr)), kv) > 0 {
					return ErrValidationFailed
				}
			default:
				return fmt.Errorf("invalid maxItems value: %v", kv)
			}
		case "uniqueItems":
			switch kv := keyword.Value.(type) {
			case bool:
				if kv {
					for i := 0; i < len(arr)-1; i++ {
						for j := i + 1; j < len(arr); j++ {
							if Compare(arr[i], arr[j]) == 0 {
								return ErrValidationFailed
							}
						}
					}
				}
			default:
				return fmt.Errorf("invalid uniqueItems value: %v", kv)
			}
		case "items":
			switch kv := keyword.Value.(type) {
			case bson.D:
				schema := NewSchema(kv)
				for _, item := range arr {
					err := schema.Evaluate(item)
					if err != nil {
						return err
					}
				}
			case bson.A:
				schemas := make([]*Schema, 0, len(kv))
				for _, v := range kv {
					switch v := v.(type) {
					case bson.D:
						schemas = append(schemas, NewSchema(v))
					default:
						return fmt.Errorf("invalid items element: %v", v)
					}
				}
				for i, item := range arr {
					if i < len(schemas) {
						err := schemas[i].Evaluate(item)
						if err != nil {
							return err
						}
					} else if additionalItems != nil {
						err := additionalItems.Evaluate(item)
						if err != nil {
							return err
						}
					} else {
						return ErrValidationFailed
					}
				}
			default:
				return fmt.Errorf("invalid items value: %v", kv)
			}
		}
	}

	return nil
}

func (s *Schema) compileRegex(pattern string) (*regexp.Regexp, error) {
	// check cache
	if regex, ok := s.Regexes[pattern]; ok {
		return regex, nil
	}

	// compile regex
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	// cache regex
	s.Regexes[pattern] = regex

	return regex, nil
}
