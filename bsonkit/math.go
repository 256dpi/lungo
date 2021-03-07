package bsonkit

import (
	"math"

	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func d128ToDec(d primitive.Decimal128) decimal.Decimal {
	big, exp, _ := d.BigInt()
	return decimal.NewFromBigInt(big, int32(exp))
}

func decTod128(d decimal.Decimal) primitive.Decimal128 {
	dd, _ := primitive.ParseDecimal128FromBigInt(d.Coefficient(), int(d.Exponent()))
	return dd
}

// Add will add together two numerical values. It accepts and returns int32,
// int64, float64 and decimal128.
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
		case primitive.Decimal128:
			return decTod128(decimal.NewFromInt(int64(num)).Add(d128ToDec(inc)))
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
		case primitive.Decimal128:
			return decTod128(decimal.NewFromInt(num).Add(d128ToDec(inc)))
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
		case primitive.Decimal128:
			return decTod128(decimal.NewFromFloat(num).Add(d128ToDec(inc)))
		default:
			return Missing
		}
	case primitive.Decimal128:
		switch inc := inc.(type) {
		case int32:
			return decTod128(d128ToDec(num).Add(decimal.NewFromInt(int64(inc))))
		case int64:
			return decTod128(d128ToDec(num).Add(decimal.NewFromInt(inc)))
		case float64:
			return decTod128(d128ToDec(num).Add(decimal.NewFromFloat(inc)))
		case primitive.Decimal128:
			return decTod128(d128ToDec(num).Add(d128ToDec(inc)))
		default:
			return Missing
		}
	default:
		return Missing
	}
}

// Mul will multiply the two numerical values. It accepts and returns int32,
// int64, float64 and decimal128.
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
		case primitive.Decimal128:
			return decTod128(decimal.NewFromInt(int64(num)).Mul(d128ToDec(mul)))
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
		case primitive.Decimal128:
			return decTod128(decimal.NewFromInt(num).Mul(d128ToDec(mul)))
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
		case primitive.Decimal128:
			return decTod128(decimal.NewFromFloat(num).Mul(d128ToDec(mul)))
		default:
			return Missing
		}
	case primitive.Decimal128:
		switch mul := mul.(type) {
		case int32:
			return decTod128(d128ToDec(num).Mul(decimal.NewFromInt(int64(mul))))
		case int64:
			return decTod128(d128ToDec(num).Mul(decimal.NewFromInt(mul)))
		case float64:
			return decTod128(d128ToDec(num).Mul(decimal.NewFromFloat(mul)))
		case primitive.Decimal128:
			return decTod128(d128ToDec(num).Mul(d128ToDec(mul)))
		default:
			return Missing
		}
	default:
		return Missing
	}
}

// Mod will compute the modulo of the two values. It accepts and returns int32,
// in64, float64 and decimal128.
func Mod(num, div interface{}) interface{} {
	switch num := num.(type) {
	case int32:
		switch div := div.(type) {
		case int32:
			return num % div
		case int64:
			return int64(num) % div
		case float64:
			return math.Mod(float64(num), div)
		case primitive.Decimal128:
			return decTod128(decimal.NewFromInt(int64(num)).Mod(d128ToDec(div)))
		default:
			return Missing
		}
	case int64:
		switch div := div.(type) {
		case int32:
			return num % int64(div)
		case int64:
			return num % div
		case float64:
			return math.Mod(float64(num), div)
		case primitive.Decimal128:
			return decTod128(decimal.NewFromInt(num).Mod(d128ToDec(div)))
		default:
			return Missing
		}
	case float64:
		switch div := div.(type) {
		case int32:
			return math.Mod(num, float64(div))
		case int64:
			return math.Mod(num, float64(div))
		case float64:
			return math.Mod(num, div)
		case primitive.Decimal128:
			return decTod128(decimal.NewFromFloat(num).Mod(d128ToDec(div)))
		default:
			return Missing
		}
	case primitive.Decimal128:
		switch div := div.(type) {
		case int32:
			return decTod128(d128ToDec(num).Mod(decimal.NewFromInt(int64(div))))
		case int64:
			return decTod128(d128ToDec(num).Mod(decimal.NewFromInt(div)))
		case float64:
			return decTod128(d128ToDec(num).Mod(decimal.NewFromFloat(div)))
		case primitive.Decimal128:
			return decTod128(d128ToDec(num).Mod(d128ToDec(div)))
		default:
			return Missing
		}
	default:
		return Missing
	}
}
