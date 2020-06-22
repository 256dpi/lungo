package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func newSchema(m bson.M) *Schema {
	return NewSchema(MustConvertValue(m).(bson.D))
}

func validateSchema(t *testing.T, schema bson.M, value interface{}, msg string) {
	err := newSchema(schema).Evaluate(MustConvertValue(value))
	assert.Error(t, err)
	assert.Equal(t, msg, err.Error())
}

func evaluateSchema(t *testing.T, schema *Schema, ok bool, value interface{}) {
	err := schema.Evaluate(MustConvertValue(value))
	if ok {
		assert.NoError(t, err)
	} else {
		assert.Error(t, err)
		assert.Equal(t, ErrValidationFailed, err)
	}
}

func TestSchemaEvaluateGeneric(t *testing.T) {
	// empty
	schema := newSchema(bson.M{})
	evaluateSchema(t, schema, true, float64(0))
	evaluateSchema(t, schema, true, "")
	evaluateSchema(t, schema, true, bson.D{})
	evaluateSchema(t, schema, true, bson.A{})
	evaluateSchema(t, schema, true, primitive.NewObjectID())

	// invalid type
	validateSchema(t, bson.M{"type": 2}, "", "invalid type value: 2")
	validateSchema(t, bson.M{"type": "foo"}, "", "invalid type name: foo")
	validateSchema(t, bson.M{"type": bson.A{}}, "", "invalid type value: []")
	validateSchema(t, bson.M{"type": bson.A{2}}, "", "invalid type element: 2")
	validateSchema(t, bson.M{"type": bson.A{"foo"}}, "", "invalid type name: foo")

	// type
	schema = newSchema(bson.M{"type": "null"})
	evaluateSchema(t, schema, false, float64(42))
	evaluateSchema(t, schema, false, "foo")
	evaluateSchema(t, schema, false, bson.D{{Key: "foo", Value: "bar"}})
	evaluateSchema(t, schema, false, bson.A{"foo"})
	schema = newSchema(bson.M{"type": "number"})
	evaluateSchema(t, schema, true, float64(42))
	schema = newSchema(bson.M{"type": "string"})
	evaluateSchema(t, schema, true, "foo")
	schema = newSchema(bson.M{"type": "object"})
	evaluateSchema(t, schema, true, bson.D{{Key: "foo", Value: "bar"}})
	schema = newSchema(bson.M{"type": "array"})
	evaluateSchema(t, schema, true, bson.A{"foo"})
	schema = newSchema(bson.M{"type": bson.A{"string", "number"}})
	evaluateSchema(t, schema, true, "foo")
	evaluateSchema(t, schema, true, int32(2))
	evaluateSchema(t, schema, false, false)

	// invalid BSON type
	validateSchema(t, bson.M{"bsonType": 2}, "", "invalid bsonType value: 2")
	validateSchema(t, bson.M{"bsonType": "foo"}, "", "invalid bsonType alias: foo")
	validateSchema(t, bson.M{"bsonType": bson.A{}}, "", "invalid bsonType value: []")
	validateSchema(t, bson.M{"bsonType": bson.A{2}}, "", "invalid bsonType element: 2")
	validateSchema(t, bson.M{"bsonType": bson.A{"foo"}}, "", "invalid bsonType alias: foo")

	// BSON type
	schema = newSchema(bson.M{"bsonType": "null"})
	evaluateSchema(t, schema, false, float64(42))
	evaluateSchema(t, schema, false, "foo")
	evaluateSchema(t, schema, false, bson.D{{Key: "foo", Value: "bar"}})
	evaluateSchema(t, schema, false, bson.A{"foo"})
	schema = newSchema(bson.M{"bsonType": "double"})
	evaluateSchema(t, schema, true, float64(42))
	schema = newSchema(bson.M{"bsonType": "number"})
	evaluateSchema(t, schema, true, float64(42))
	schema = newSchema(bson.M{"bsonType": "string"})
	evaluateSchema(t, schema, true, "foo")
	schema = newSchema(bson.M{"bsonType": "object"})
	evaluateSchema(t, schema, true, bson.D{{Key: "foo", Value: "bar"}})
	schema = newSchema(bson.M{"bsonType": "array"})
	evaluateSchema(t, schema, true, bson.A{"foo"})
	schema = newSchema(bson.M{"bsonType": bson.A{"string", "number"}})
	evaluateSchema(t, schema, true, "foo")
	evaluateSchema(t, schema, true, int32(2))
	evaluateSchema(t, schema, false, false)

	// invalid type and bson type
	validateSchema(t, bson.M{"type": "string", "bsonType": "string"}, "", "schema cannot contain type and bsonType")

	// invalid enum
	validateSchema(t, bson.M{"enum": 2}, "", "invalid enum value: 2")
	validateSchema(t, bson.M{"enum": bson.A{}}, "", "invalid enum value: []")

	// enum
	schema = newSchema(bson.M{"enum": bson.A{nil, "foo", int32(7), false, float64(42)}})
	evaluateSchema(t, schema, false, float64(0))
	evaluateSchema(t, schema, true, float64(7))
	evaluateSchema(t, schema, true, int64(42))
	evaluateSchema(t, schema, false, float64(99))
	schema = newSchema(bson.M{"enum": bson.A{nil, "foo", "bar", int64(1), false}})
	evaluateSchema(t, schema, false, "")
	evaluateSchema(t, schema, true, "foo")
	evaluateSchema(t, schema, true, "bar")
	evaluateSchema(t, schema, false, "baz")
	schema = newSchema(bson.M{"enum": bson.A{nil, "foo", bson.D{{Key: "foo", Value: "bar"}}, int64(1), false}})
	evaluateSchema(t, schema, false, bson.D{})
	evaluateSchema(t, schema, true, bson.D{{Key: "foo", Value: "bar"}})
	evaluateSchema(t, schema, false, bson.D{{Key: "foo", Value: "baz"}})
	schema = newSchema(bson.M{"enum": bson.A{nil, "foo", bson.A{"foo"}, int64(1), false}})
	evaluateSchema(t, schema, false, bson.A{})
	evaluateSchema(t, schema, true, bson.A{"foo"})
	evaluateSchema(t, schema, false, bson.A{"bar"})

	// invalid all of
	validateSchema(t, bson.M{"allOf": 2}, "", "invalid allOf value: 2")
	validateSchema(t, bson.M{"allOf": bson.A{}}, "", "invalid allOf value: []")
	validateSchema(t, bson.M{"allOf": bson.A{"foo"}}, "", "invalid allOf element: foo")
	validateSchema(t, bson.M{"allOf": bson.A{bson.M{"enum": bson.A{}}}}, "", "invalid enum value: []")

	// all of
	schema = newSchema(bson.M{"allOf": bson.A{
		bson.M{"enum": bson.A{"foo"}},
		bson.M{"enum": bson.A{"foo", "bar"}},
	}})
	evaluateSchema(t, schema, true, "foo")
	evaluateSchema(t, schema, false, "bar")
	evaluateSchema(t, schema, false, "baz")

	// invalid any of
	validateSchema(t, bson.M{"anyOf": 2}, "", "invalid anyOf value: 2")
	validateSchema(t, bson.M{"anyOf": bson.A{}}, "", "invalid anyOf value: []")
	validateSchema(t, bson.M{"anyOf": bson.A{"foo"}}, "", "invalid anyOf element: foo")
	validateSchema(t, bson.M{"anyOf": bson.A{bson.M{"enum": bson.A{}}}}, "", "invalid enum value: []")

	// any of
	schema = newSchema(bson.M{"anyOf": bson.A{
		bson.M{"enum": bson.A{"foo"}},
		bson.M{"enum": bson.A{"foo", "bar"}},
	}})
	evaluateSchema(t, schema, true, "foo")
	evaluateSchema(t, schema, true, "bar")
	evaluateSchema(t, schema, false, "baz")

	// invalid one of
	validateSchema(t, bson.M{"oneOf": 2}, "", "invalid oneOf value: 2")
	validateSchema(t, bson.M{"oneOf": bson.A{}}, "", "invalid oneOf value: []")
	validateSchema(t, bson.M{"oneOf": bson.A{"foo"}}, "", "invalid oneOf element: foo")
	validateSchema(t, bson.M{"oneOf": bson.A{bson.M{"enum": bson.A{}}}}, "", "invalid enum value: []")

	// one of
	schema = newSchema(bson.M{"oneOf": bson.A{
		bson.M{"enum": bson.A{"foo"}},
		bson.M{"enum": bson.A{"foo", "bar"}},
	}})
	evaluateSchema(t, schema, false, "foo")
	evaluateSchema(t, schema, true, "bar")
	evaluateSchema(t, schema, false, "baz")

	// invalid not
	validateSchema(t, bson.M{"not": 2}, "", "invalid not value: 2")
	validateSchema(t, bson.M{"not": bson.M{"enum": bson.A{}}}, "", "invalid enum value: []")

	// not
	schema = newSchema(bson.M{"not": bson.M{"enum": bson.A{"foo"}}})
	evaluateSchema(t, schema, false, "foo")
	evaluateSchema(t, schema, true, "bar")
	evaluateSchema(t, schema, true, "baz")
}

func TestSchemaEvaluateNumber(t *testing.T) {
	// invalid multiple of
	validateSchema(t, bson.M{"multipleOf": "foo"}, int32(0), "invalid multipleOf value: foo")
	validateSchema(t, bson.M{"multipleOf": -2}, int32(0), "invalid multipleOf value: -2")

	// multiple of
	schema := newSchema(bson.M{"multipleOf": 2})
	evaluateSchema(t, schema, true, float64(0))
	evaluateSchema(t, schema, true, float64(2))
	evaluateSchema(t, schema, false, float64(7))
	evaluateSchema(t, schema, false, float64(9))

	// invalid min max
	validateSchema(t, bson.M{"minimum": "foo"}, int32(0), "invalid minimum value: foo")
	validateSchema(t, bson.M{"maximum": "foo"}, int32(0), "invalid maximum value: foo")

	// min and max
	schema = newSchema(bson.M{"minimum": 2, "maximum": 9})
	evaluateSchema(t, schema, false, float64(0))
	evaluateSchema(t, schema, true, float64(2))
	evaluateSchema(t, schema, true, float64(7))
	evaluateSchema(t, schema, true, float64(9))
	evaluateSchema(t, schema, false, float64(42))

	// invalid min and max exclusive
	validateSchema(t, bson.M{"exclusiveMinimum": 2}, int32(0), "invalid exclusiveMinimum value: 2")
	validateSchema(t, bson.M{"exclusiveMaximum": 2}, int32(0), "invalid exclusiveMaximum value: 2")
	validateSchema(t, bson.M{"exclusiveMinimum": true}, int32(0), "exclusiveMinimum requires minimum")
	validateSchema(t, bson.M{"exclusiveMaximum": true}, int32(0), "exclusiveMaximum requires maximum")

	// min and max exclusive
	schema = newSchema(bson.M{"minimum": 2, "maximum": 9, "exclusiveMinimum": true, "exclusiveMaximum": true})
	evaluateSchema(t, schema, false, float64(0))
	evaluateSchema(t, schema, false, float64(2))
	evaluateSchema(t, schema, true, float64(7))
	evaluateSchema(t, schema, false, float64(9))
	evaluateSchema(t, schema, false, float64(42))
}

func TestSchemaEvaluateString(t *testing.T) {
	// invalid min and max length
	validateSchema(t, bson.M{"minLength": "foo"}, "", "invalid minLength value: foo")
	validateSchema(t, bson.M{"maxLength": "foo"}, "", "invalid maxLength value: foo")
	validateSchema(t, bson.M{"minLength": int32(-1)}, "", "invalid minLength value: -1")
	validateSchema(t, bson.M{"maxLength": int32(-1)}, "", "invalid maxLength value: -1")

	// min and max length
	schema := newSchema(bson.M{"minLength": 1, "maxLength": 3})
	evaluateSchema(t, schema, false, "")
	evaluateSchema(t, schema, true, "foo")
	evaluateSchema(t, schema, false, "foos")

	// invalid pattern
	validateSchema(t, bson.M{"pattern": 2}, "", "invalid pattern value: 2")
	validateSchema(t, bson.M{"pattern": "[0"}, "", "invalid pattern regex: [0")

	// pattern
	schema = newSchema(bson.M{"pattern": "[fo]+"})
	evaluateSchema(t, schema, false, "")
	evaluateSchema(t, schema, true, "foo")
	evaluateSchema(t, schema, false, "bar")
}

func TestSchemaEvaluateDocument(t *testing.T) {
	// invalid required
	validateSchema(t, bson.M{"required": "foo"}, bson.D{}, "invalid required value: foo")
	validateSchema(t, bson.M{"required": bson.A{}}, bson.D{}, "invalid required value: []")
	validateSchema(t, bson.M{"required": bson.A{2.0}}, bson.D{}, "invalid required element: 2")

	// required
	schema := newSchema(bson.M{"required": bson.A{"foo", "bar"}})
	evaluateSchema(t, schema, false, bson.M{"foo": "bar"})
	evaluateSchema(t, schema, true, bson.M{"foo": "bar", "bar": "baz"})
	evaluateSchema(t, schema, true, bson.M{"foo": "bar", "bar": "baz", "quz": "qux"})

	// invalid min and max properties
	validateSchema(t, bson.M{"minProperties": "foo"}, bson.D{}, "invalid minProperties value: foo")
	validateSchema(t, bson.M{"maxProperties": "foo"}, bson.D{}, "invalid maxProperties value: foo")
	validateSchema(t, bson.M{"minProperties": -2}, bson.D{}, "invalid minProperties value: -2")
	validateSchema(t, bson.M{"maxProperties": -2}, bson.D{}, "invalid maxProperties value: -2")

	// min and max properties
	schema = newSchema(bson.M{"minProperties": 1, "maxProperties": 2})
	evaluateSchema(t, schema, false, bson.M{})
	evaluateSchema(t, schema, true, bson.M{"foo": "bar"})
	evaluateSchema(t, schema, true, bson.M{"foo": "bar", "bar": "baz"})
	evaluateSchema(t, schema, false, bson.M{"foo": "bar", "bar": "baz", "baz": "quz"})

	// invalid dependencies
	validateSchema(t, bson.M{"dependencies": "foo"}, bson.D{}, "invalid dependencies value: foo")
	validateSchema(t, bson.M{"dependencies": bson.M{"foo": "bar"}}, bson.D{}, "invalid dependencies element: {foo bar}")
	validateSchema(t, bson.M{"dependencies": bson.M{"foo": bson.A{}}}, bson.D{}, "invalid dependencies element: {foo []}")
	validateSchema(t, bson.M{"dependencies": bson.M{"foo": bson.A{2}}}, bson.D{}, "invalid dependencies element value: 2")

	// dependencies
	schema = newSchema(bson.M{"dependencies": bson.M{"foo": bson.M{"maxProperties": int32(1)}}})
	evaluateSchema(t, schema, true, bson.M{"foo": "bar"})
	evaluateSchema(t, schema, false, bson.M{"foo": "bar", "bar": "baz"})
	evaluateSchema(t, schema, true, bson.M{"bar": "foo", "baz": "bar"})
	schema = newSchema(bson.M{"dependencies": bson.M{"foo": bson.A{"foo", "bar"}}})
	evaluateSchema(t, schema, false, bson.M{"foo": "bar"})
	evaluateSchema(t, schema, true, bson.M{"foo": "bar", "bar": "baz"})
	evaluateSchema(t, schema, false, bson.M{"bar": "foo", "baz": "bar"})

	// invalid properties, patternProperties and additionalProperties
	validateSchema(t, bson.M{"properties": "foo"}, bson.D{}, "invalid properties value: foo")
	validateSchema(t, bson.M{"properties": bson.M{"foo": "bar"}}, bson.D{}, "invalid properties element: {foo bar}")
	validateSchema(t, bson.M{"patternProperties": "foo"}, bson.D{}, "invalid patternProperties value: foo")
	validateSchema(t, bson.M{"patternProperties": bson.M{"foo": "bar"}}, bson.D{}, "invalid patternProperties element: {foo bar}")
	validateSchema(t, bson.M{"patternProperties": bson.M{"[0": "bar"}}, bson.D{}, "invalid patternProperties regex: [0")
	validateSchema(t, bson.M{"additionalProperties": "foo"}, bson.D{}, "invalid additionalProperties value: foo")

	// properties, patternProperties and additionalProperties
	schema = newSchema(bson.M{"properties": bson.M{"foo": bson.M{"minLength": 2}}})
	evaluateSchema(t, schema, false, bson.M{"foo": "a"})
	evaluateSchema(t, schema, true, bson.M{"foo": "bar"})
	evaluateSchema(t, schema, true, bson.M{"bar": "bar"})
	schema = newSchema(bson.M{"patternProperties": bson.M{"[fo]+": bson.M{"minLength": 2}}})
	evaluateSchema(t, schema, false, bson.M{"foo": "a"})
	evaluateSchema(t, schema, true, bson.M{"foo": "bar"})
	evaluateSchema(t, schema, true, bson.M{"bar": "bar"})
	schema = newSchema(bson.M{"properties": bson.M{"foo": bson.M{"minLength": 2}}, "additionalProperties": false})
	evaluateSchema(t, schema, false, bson.M{"bar": "bar"})
	schema = newSchema(bson.M{"properties": bson.M{"foo": bson.M{"minLength": 2}}, "additionalProperties": true})
	evaluateSchema(t, schema, true, bson.M{"bar": "bar"})
	schema = newSchema(bson.M{"properties": bson.M{"foo": bson.M{"minLength": 2}}, "additionalProperties": bson.M{"minLength": 2}})
	evaluateSchema(t, schema, false, bson.M{"bar": "a"})
	evaluateSchema(t, schema, true, bson.M{"bar": "bar"})
}

func TestSchemaEvaluateArray(t *testing.T) {
	// invalid min and max items
	validateSchema(t, bson.M{"minItems": "foo"}, bson.A{}, "invalid minItems value: foo")
	validateSchema(t, bson.M{"maxItems": "foo"}, bson.A{}, "invalid maxItems value: foo")
	validateSchema(t, bson.M{"minItems": int32(-1)}, bson.A{}, "invalid minItems value: -1")
	validateSchema(t, bson.M{"maxItems": int32(-1)}, bson.A{}, "invalid maxItems value: -1")

	// min and max items
	schema := newSchema(bson.M{"minItems": 2, "maxItems": 4})
	evaluateSchema(t, schema, false, bson.A{"foo"})
	evaluateSchema(t, schema, true, bson.A{"foo", "bar", "baz"})
	evaluateSchema(t, schema, false, bson.A{"foo", "bar", "baz", "qux", "quz"})

	// invalid unique items
	validateSchema(t, bson.M{"uniqueItems": "foo"}, bson.A{}, "invalid uniqueItems value: foo")

	// unique items
	schema = newSchema(bson.M{"uniqueItems": false})
	evaluateSchema(t, schema, true, bson.A{"foo", "bar"})
	evaluateSchema(t, schema, true, bson.A{"foo", "bar", "foo"})
	schema = newSchema(bson.M{"uniqueItems": true})
	evaluateSchema(t, schema, true, bson.A{"foo", "bar"})
	evaluateSchema(t, schema, false, bson.A{"foo", "bar", "foo"})

	// invalid items and additional properties
	validateSchema(t, bson.M{"items": "foo"}, bson.A{}, "invalid items value: foo")
	validateSchema(t, bson.M{"items": bson.A{"foo"}}, bson.A{}, "invalid items element: foo")
	validateSchema(t, bson.M{"additionalItems": "foo"}, bson.A{}, "invalid additionalItems value: foo")

	// items
	schema = newSchema(bson.M{"items": bson.M{"minLength": 2}})
	evaluateSchema(t, schema, true, bson.A{})
	evaluateSchema(t, schema, false, bson.A{"a"})
	evaluateSchema(t, schema, true, bson.A{"foo"})
	evaluateSchema(t, schema, false, bson.A{"foo", "a"})
	schema = newSchema(bson.M{"items": bson.A{bson.M{"minLength": 1}, bson.M{"minLength": 3}}})
	evaluateSchema(t, schema, true, bson.A{})
	evaluateSchema(t, schema, true, bson.A{"a"})
	evaluateSchema(t, schema, true, bson.A{"a", "foo"})
	evaluateSchema(t, schema, false, bson.A{"foo", "a"})
	evaluateSchema(t, schema, false, bson.A{"a", "b"})
	evaluateSchema(t, schema, true, bson.A{"a", "foo", "b"})

	// invalid items and additionalItems
	schema = newSchema(bson.M{"items": bson.A{bson.M{"minLength": 3}}, "additionalItems": false})
	evaluateSchema(t, schema, false, bson.A{"a"})
	evaluateSchema(t, schema, true, bson.A{"foo"})
	evaluateSchema(t, schema, false, bson.A{"foo", "a"})
	schema = newSchema(bson.M{"items": bson.A{bson.M{"minLength": 3}}, "additionalItems": true})
	evaluateSchema(t, schema, false, bson.A{"a"})
	evaluateSchema(t, schema, true, bson.A{"foo"})
	evaluateSchema(t, schema, true, bson.A{"foo", "a"})
	schema = newSchema(bson.M{"items": bson.A{bson.M{"minLength": 3}}, "additionalItems": bson.M{"minLength": 3}})
	evaluateSchema(t, schema, false, bson.A{"a"})
	evaluateSchema(t, schema, false, bson.A{"a", "foo"})
	evaluateSchema(t, schema, false, bson.A{"foo", "a"})
	evaluateSchema(t, schema, true, bson.A{"foo", "foo"})
	evaluateSchema(t, schema, false, bson.A{"foo", "foo", "a"})
	evaluateSchema(t, schema, true, bson.A{"foo", "foo", "foo"})
}
