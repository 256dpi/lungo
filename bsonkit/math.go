package bsonkit

import (
	"math"
)

// Add will add together two numerical values. It accepts and returns int32,
// int64 and float64.
func Add(num, inc interface{}) interface{} {
	switch num := num.(type) {
	case int32:
		switch inc := inc.(type) {
		case int32:
			return num + inc
		case int64:
			return int64(num) + inc
		case float64:
			return float64(num) + inc
		default:
			return Missing
		}
	case int64:
		switch inc := inc.(type) {
		case int32:
			return num + int64(inc)
		case int64:
			return num + inc
		case float64:
			return float64(num) + inc
		default:
			return Missing
		}
	case float64:
		switch inc := inc.(type) {
		case int32:
			return num + float64(inc)
		case int64:
			return num + float64(inc)
		case float64:
			return num + inc
		default:
			return Missing
		}
	default:
		return Missing
	}
}

// Mul will multiply the two numerical values. It accepts and returns int32,
// int64 and float64.
func Mul(num, mul interface{}) interface{} {
	switch num := num.(type) {
	case int32:
		switch mul := mul.(type) {
		case int32:
			return num * mul
		case int64:
			return int64(num) * mul
		case float64:
			return float64(num) * mul
		default:
			return Missing
		}
	case int64:
		switch mul := mul.(type) {
		case int32:
			return num * int64(mul)
		case int64:
			return num * mul
		case float64:
			return float64(num) * mul
		default:
			return Missing
		}
	case float64:
		switch mul := mul.(type) {
		case int32:
			return num * float64(mul)
		case int64:
			return num * float64(mul)
		case float64:
			return num * mul
		default:
			return Missing
		}
	default:
		return Missing
	}
}

// Mod will compute the modulo of the two values. It accepts and returns int32,
// in64 and float64.
func Mod(num, div interface{}) interface{} {
	switch num := num.(type) {
	case float64:
		switch div := div.(type) {
		case float64:
			return math.Mod(num, div)
		case int32:
			return math.Mod(num, float64(div))
		case int64:
			return math.Mod(num, float64(div))
		default:
			return Missing
		}
	case int32:
		switch div := div.(type) {
		case float64:
			return math.Mod(float64(num), div)
		case int32:
			return num % div
		case int64:
			return int64(num) % div
		default:
			return Missing
		}
	case int64:
		switch div := div.(type) {
		case float64:
			return math.Mod(float64(num), div)
		case int32:
			return num % int64(div)
		case int64:
			return num % div
		default:
			return Missing
		}
	default:
		return Missing
	}
}
