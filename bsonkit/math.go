package bsonkit

import (
	"math"

	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// TODO: IEEE-754 propagation for non-finite operands (Decimal128 NaN / ±Inf
//  and float64 NaN / ±Inf when used with a Decimal128 partner). They
//  currently collapse to zero on conversion, so Add/Mul/Mod produce
//  numerically wrong (but non-panicking) results — MongoDB instead promotes
//  to Decimal128 and propagates the special value (NaN op anything = NaN;
//  sign rules on ±Inf; finite % ±Inf = dividend). A proper fix would
//  classify each operand (Decimal128 via BigInt's ErrParseNaN /
//  ErrParseInf / ErrParseNegInf sentinels; float64 via math.IsNaN /
//  math.IsInf), short-circuit arithmetic with the canonical Decimal128
//  special-value singletons, and drop the safeDecMod / safeFloatToDec
//  workarounds for the non-finite case.

func decToD128(d decimal.Decimal) bson.Decimal128 {
	dd, _ := bson.ParseDecimal128FromBigInt(d.Coefficient(), int(d.Exponent()))
	return dd
}

func safeD128ToDec(d bson.Decimal128) decimal.Decimal {
	big, exp, err := d.BigInt()
	if err != nil {
		return decimal.Decimal{}
	}
	return decimal.NewFromBigInt(big, int32(exp))
}

func safeDecMod(num, div decimal.Decimal) decimal.Decimal {
	if div.IsZero() {
		return decimal.Decimal{}
	}
	return num.Mod(div)
}

func safeFloatToDec(f float64) decimal.Decimal {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return decimal.Decimal{}
	}
	return decimal.NewFromFloat(f)
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
		case bson.Decimal128:
			return decToD128(decimal.NewFromInt(int64(num)).Add(safeD128ToDec(inc)))
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
		case bson.Decimal128:
			return decToD128(decimal.NewFromInt(num).Add(safeD128ToDec(inc)))
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
		case bson.Decimal128:
			return decToD128(safeFloatToDec(num).Add(safeD128ToDec(inc)))
		default:
			return Missing
		}
	case bson.Decimal128:
		switch inc := inc.(type) {
		case int32:
			return decToD128(safeD128ToDec(num).Add(decimal.NewFromInt(int64(inc))))
		case int64:
			return decToD128(safeD128ToDec(num).Add(decimal.NewFromInt(inc)))
		case float64:
			return decToD128(safeD128ToDec(num).Add(safeFloatToDec(inc)))
		case bson.Decimal128:
			return decToD128(safeD128ToDec(num).Add(safeD128ToDec(inc)))
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
		case bson.Decimal128:
			return decToD128(decimal.NewFromInt(int64(num)).Mul(safeD128ToDec(mul)))
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
		case bson.Decimal128:
			return decToD128(decimal.NewFromInt(num).Mul(safeD128ToDec(mul)))
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
		case bson.Decimal128:
			return decToD128(safeFloatToDec(num).Mul(safeD128ToDec(mul)))
		default:
			return Missing
		}
	case bson.Decimal128:
		switch mul := mul.(type) {
		case int32:
			return decToD128(safeD128ToDec(num).Mul(decimal.NewFromInt(int64(mul))))
		case int64:
			return decToD128(safeD128ToDec(num).Mul(decimal.NewFromInt(mul)))
		case float64:
			return decToD128(safeD128ToDec(num).Mul(safeFloatToDec(mul)))
		case bson.Decimal128:
			return decToD128(safeD128ToDec(num).Mul(safeD128ToDec(mul)))
		default:
			return Missing
		}
	default:
		return Missing
	}
}

// Mod will compute the modulo of the two values. It accepts and returns int32,
// in64, float64 and decimal128. A zero divisor returns Missing, except for
// float64 divisors that return NaN.
func Mod(num, div interface{}) interface{} {
	// guard against zero divisors that would otherwise panic at runtime;
	// non-finite Decimal128 divisors are still handled downstream by
	// safeDecMod after safeD128ToDec collapses them to zero
	switch d := div.(type) {
	case int32:
		if d == 0 {
			return Missing
		}
	case int64:
		if d == 0 {
			return Missing
		}
	case bson.Decimal128:
		if dec, _, err := d.BigInt(); err == nil && dec.Sign() == 0 {
			return Missing
		}
	}

	// calculate modulo
	switch num := num.(type) {
	case int32:
		switch div := div.(type) {
		case int32:
			return num % div
		case int64:
			return int64(num) % div
		case float64:
			return math.Mod(float64(num), div)
		case bson.Decimal128:
			return decToD128(safeDecMod(decimal.NewFromInt(int64(num)), safeD128ToDec(div)))
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
		case bson.Decimal128:
			return decToD128(safeDecMod(decimal.NewFromInt(num), safeD128ToDec(div)))
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
		case bson.Decimal128:
			return decToD128(safeDecMod(safeFloatToDec(num), safeD128ToDec(div)))
		default:
			return Missing
		}
	case bson.Decimal128:
		switch div := div.(type) {
		case int32:
			return decToD128(safeDecMod(safeD128ToDec(num), decimal.NewFromInt(int64(div))))
		case int64:
			return decToD128(safeDecMod(safeD128ToDec(num), decimal.NewFromInt(div)))
		case float64:
			return decToD128(safeDecMod(safeD128ToDec(num), safeFloatToDec(div)))
		case bson.Decimal128:
			return decToD128(safeDecMod(safeD128ToDec(num), safeD128ToDec(div)))
		default:
			return Missing
		}
	default:
		return Missing
	}
}
