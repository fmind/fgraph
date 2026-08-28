package fgraph

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type Field struct {
	Value any
	Name  string
}

// Object retains duplicate-safe decoded members; consumers apply canonical key ordering.
type Object struct{ Fields []Field }

// marshalOrderedObject is for stable public wire objects whose in-memory field
// order is independently optimized. Canonical storage still uses
// writeCanonicalJSON, which sorts keys according to the file-format contract.
func marshalOrderedObject(fields []Field) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for index, field := range fields {
		if index > 0 {
			buf.WriteByte(',')
		}
		// Public wire field names are fixed ASCII identifiers, so quoting cannot fail.
		name := strconv.Quote(field.Name)
		value, err := json.Marshal(field.Value)
		if err != nil {
			return nil, wrap(ErrType, err, "cannot encode JSON object field %q", field.Name)
		}
		buf.WriteString(name)
		buf.WriteByte(':')
		buf.Write(value)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func DecodeJSON(r io.Reader) (any, error) {
	return decodeJSON(r, false, MaxJSONDocumentDepth)
}

func decodeInternalJSON(r io.Reader) (any, error) {
	return decodeJSON(r, true, MaxJSONDepth)
}

func decodeInternalDocumentJSON(r io.Reader) (any, error) {
	return decodeJSON(r, true, MaxJSONDocumentDepth)
}

func decodeJSON(r io.Reader, allowIntegralFloat bool, maxDepth int) (any, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, fail(ErrType, "cannot read JSON input; provide valid UTF-8 JSON: %v", err)
	}
	if !utf8.Valid(raw) {
		return nil, fail(ErrType, "JSON input is not valid UTF-8; encode binary data with the bytes wrapper")
	}
	if unicodeErr := validateJSONUnicodeEscapes(raw); unicodeErr != nil {
		return nil, fail(ErrType, "JSON input has invalid Unicode; pair UTF-16 surrogate escapes: %v", unicodeErr)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	v, err := decodeValue(dec, allowIntegralFloat, 0, maxDepth)
	if err != nil {
		if errors.Is(err, ErrTooLarge) {
			return nil, err
		}
		return nil, fail(ErrType, "invalid JSON input; provide a JSON map, operation, or transaction array: %v", err)
	}
	if tok, err := dec.Token(); err != io.EOF {
		if err == nil {
			return nil, fail(ErrType, "unexpected trailing JSON token %v; provide one JSON value", tok)
		}
		return nil, fail(ErrType, "invalid trailing JSON: %v", err)
	}
	return v, nil
}

func validateJSONUnicodeEscapes(raw []byte) error {
	inString := false
	for index := 0; index < len(raw); index++ {
		switch raw[index] {
		case '"':
			inString = !inString
		case '\\':
			if !inString || index+1 >= len(raw) {
				continue
			}
			if raw[index+1] != 'u' {
				index++ // The escaped byte cannot terminate the JSON string.
				continue
			}
			value, ok := jsonHexEscape(raw[index+2:])
			if !ok {
				continue // The JSON decoder reports malformed hexadecimal escapes.
			}
			switch {
			case value >= 0xd800 && value <= 0xdbff:
				if index+12 > len(raw) || raw[index+6] != '\\' || raw[index+7] != 'u' {
					return fmt.Errorf("high surrogate escape at byte %d has no low surrogate", index)
				}
				low, lowOK := jsonHexEscape(raw[index+8:])
				if !lowOK || low < 0xdc00 || low > 0xdfff {
					return fmt.Errorf("high surrogate escape at byte %d has no low surrogate", index)
				}
				index += 11
			case value >= 0xdc00 && value <= 0xdfff:
				return fmt.Errorf("low surrogate escape at byte %d has no high surrogate", index)
			default:
				index += 5
			}
		}
	}
	return nil
}

func jsonHexEscape(raw []byte) (uint16, bool) {
	if len(raw) < 4 {
		return 0, false
	}
	var value uint16
	for _, char := range raw[:4] {
		value <<= 4
		switch {
		case char >= '0' && char <= '9':
			value += uint16(char - '0')
		case char >= 'a' && char <= 'f':
			value += uint16(char-'a') + 10
		case char >= 'A' && char <= 'F':
			value += uint16(char-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

func decodeValue(dec *json.Decoder, allowIntegralFloat bool, depth, maxDepth int) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch value := tok.(type) {
	case json.Delim:
		if depth >= maxDepth {
			return nil, fail(ErrTooLarge, "JSON nesting exceeds %d containers; flatten the value", maxDepth)
		}
		switch value {
		case '{':
			obj := Object{}
			seen := map[string]bool{}
			for dec.More() {
				nameToken, tokenErr := dec.Token()
				if tokenErr != nil {
					return nil, tokenErr
				}
				name, ok := nameToken.(string)
				if !ok {
					return nil, errors.New("object key is not text")
				}
				if seen[name] {
					return nil, fmt.Errorf("duplicate object key %q", name)
				}
				seen[name] = true
				child, childErr := decodeValue(dec, allowIntegralFloat, depth+1, maxDepth)
				if childErr != nil {
					return nil, childErr
				}
				obj.Fields = append(obj.Fields, Field{Name: name, Value: child})
			}
			_, err = dec.Token()
			return obj, err
		case '[':
			items := []any{}
			for dec.More() {
				child, childErr := decodeValue(dec, allowIntegralFloat, depth+1, maxDepth)
				if childErr != nil {
					return nil, childErr
				}
				items = append(items, child)
			}
			_, err = dec.Token()
			return items, err
		default:
			return nil, fmt.Errorf("unexpected delimiter %q", value)
		}
	case json.Number:
		if !strings.ContainsAny(string(value), ".eE") {
			if integer, integerErr := value.Int64(); integerErr == nil {
				return integer, nil
			}
			if !allowIntegralFloat {
				return nil, fmt.Errorf("integer %q exceeds signed 64-bit range", value)
			}
		}
		f, err := value.Float64()
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, fmt.Errorf("invalid finite number %q", value)
		}
		return f, nil
	case string, bool, nil:
		return value, nil
	default:
		return nil, fmt.Errorf("unsupported JSON token %T", value)
	}
}

func objectFields(value any) ([]Field, bool) {
	switch value := value.(type) {
	case Object:
		items := make(map[string]any, len(value.Fields))
		for _, field := range value.Fields {
			items[field.Name] = field.Value
		}
		return sortedFields(items), true
	case E:
		return sortedFields(map[string]any(value)), true
	case map[string]any:
		return sortedFields(value), true
	default:
		return nil, false
	}
}

func sortedFields(value map[string]any) []Field {
	fields := make([]Field, 0, len(value))
	if id, ok := value["id"]; ok {
		fields = append(fields, Field{Name: "id", Value: id})
	}
	names := make([]string, 0, len(value))
	for name := range value {
		if name != "id" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		fields = append(fields, Field{Name: name, Value: value[name]})
	}
	return fields
}

func objectMap(value any) (map[string]any, bool) {
	fields, ok := objectFields(value)
	if !ok {
		return nil, false
	}
	result := make(map[string]any, len(fields))
	for _, field := range fields {
		result[field.Name] = field.Value
	}
	return result, true
}

func canonicalJSON(value any) ([]byte, error) {
	plain, err := plainJSONDepth(value, 0, MaxJSONDocumentDepth)
	if err != nil {
		return nil, err
	}
	if err := validateJSONDepth(plain, 0, MaxJSONDocumentDepth); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := writeCanonicalJSONDepth(&buf, plain, 0, MaxJSONDocumentDepth); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeCanonicalJSON(buf *bytes.Buffer, value any) error {
	return writeCanonicalJSONDepth(buf, value, 0, MaxJSONDepth)
}

func writeCanonicalJSONDepth(buf *bytes.Buffer, value any, depth, maxDepth int) error {
	switch value := value.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		if value {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case string:
		if !utf8.ValidString(value) {
			return fail(ErrType, "JSON text is not valid UTF-8; use the bytes wrapper for binary data")
		}
		writeCanonicalString(buf, value)
	case int:
		buf.WriteString(strconv.FormatInt(int64(value), 10))
	case int8:
		buf.WriteString(strconv.FormatInt(int64(value), 10))
	case int16:
		buf.WriteString(strconv.FormatInt(int64(value), 10))
	case int32:
		buf.WriteString(strconv.FormatInt(int64(value), 10))
	case int64:
		buf.WriteString(strconv.FormatInt(value, 10))
	case uint:
		buf.WriteString(strconv.FormatUint(uint64(value), 10))
	case uint8:
		buf.WriteString(strconv.FormatUint(uint64(value), 10))
	case uint16:
		buf.WriteString(strconv.FormatUint(uint64(value), 10))
	case uint32:
		buf.WriteString(strconv.FormatUint(uint64(value), 10))
	case uint64:
		buf.WriteString(strconv.FormatUint(value, 10))
	case float32:
		buf.WriteString(canonicalFloat(float64(value), 32))
	case float64:
		buf.WriteString(canonicalFloat(value, 64))
	case []any:
		if depth >= maxDepth {
			return fail(ErrTooLarge, "JSON nesting exceeds %d containers; flatten the value", maxDepth)
		}
		buf.WriteByte('[')
		for i, item := range value {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonicalJSONDepth(buf, item, depth+1, maxDepth); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case map[string]any:
		if depth >= maxDepth {
			return fail(ErrTooLarge, "JSON nesting exceeds %d containers; flatten the value", maxDepth)
		}
		buf.WriteByte('{')
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for i, key := range keys {
			if !utf8.ValidString(key) {
				return fail(ErrType, "JSON object key is not valid UTF-8; use valid Unicode keys")
			}
			if i > 0 {
				buf.WriteByte(',')
			}
			writeCanonicalString(buf, key)
			buf.WriteByte(':')
			if err := writeCanonicalJSONDepth(buf, value[key], depth+1, maxDepth); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	default:
		return fail(ErrType, "JSON contains unsupported %T; use JSON scalars, arrays, and string-keyed objects", value)
	}
	return nil
}

func writeCanonicalString(buf *bytes.Buffer, value string) {
	const hex = "0123456789abcdef"
	buf.WriteByte('"')
	for _, char := range value {
		switch char {
		case '"', '\\':
			buf.WriteByte('\\')
			buf.WriteRune(char)
		case '\b':
			buf.WriteString(`\b`)
		case '\f':
			buf.WriteString(`\f`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		default:
			if char < 0x20 {
				buf.WriteString(`\u00`)
				buf.WriteByte(hex[int(char)>>4])
				buf.WriteByte(hex[int(char)&0xf])
			} else {
				buf.WriteRune(char)
			}
		}
	}
	buf.WriteByte('"')
}

const floatInt64Limit = 9_223_372_036_854_775_808.0

func exactInt64Float(value float64) (int64, bool) {
	if math.IsNaN(value) || math.IsInf(value, 0) || math.Trunc(value) != value || value < -floatInt64Limit || value >= floatInt64Limit {
		return 0, false
	}
	return int64(value), true
}

func canonicalFloat(value float64, bits int) string {
	if value == 0 {
		return "0"
	}
	// float64(math.MaxInt64) rounds to 2^63, whose int64 conversion wraps. The
	// open upper bound makes the integral fast path exact at that boundary.
	if integer, ok := exactInt64Float(value); ok {
		return strconv.FormatInt(integer, 10)
	}
	abs := math.Abs(value)
	if abs >= 1e-6 && abs < 1e21 && math.Trunc(value) != value {
		return strconv.FormatFloat(value, 'f', -1, bits)
	}
	text := strconv.FormatFloat(value, 'e', -1, bits)
	parts := strings.SplitN(text, "e", 2)
	exponent, err := strconv.Atoi(parts[1])
	if err != nil {
		return text
	}
	return parts[0] + "e" + fmt.Sprintf("%+d", exponent)
}

func plainJSON(value any) (any, error) {
	return plainJSONDepth(value, 0, MaxJSONDepth)
}

func float32JSON(values []float32) []any {
	result := make([]any, len(values))
	for index, value := range values {
		result[index] = float64(value)
	}
	return result
}

func plainJSONDepth(value any, depth, maxDepth int) (any, error) {
	switch value := value.(type) {
	case Object:
		if depth >= maxDepth {
			return nil, fail(ErrTooLarge, "JSON nesting exceeds %d containers; flatten the value", maxDepth)
		}
		m := make(map[string]any, len(value.Fields))
		for _, field := range value.Fields {
			item, err := plainJSONDepth(field.Value, depth+1, maxDepth)
			if err != nil {
				return nil, err
			}
			m[field.Name] = item
		}
		return m, nil
	case []any:
		if depth >= maxDepth {
			return nil, fail(ErrTooLarge, "JSON nesting exceeds %d containers; flatten the value", maxDepth)
		}
		out := make([]any, len(value))
		for i, item := range value {
			plain, err := plainJSONDepth(item, depth+1, maxDepth)
			if err != nil {
				return nil, err
			}
			out[i] = plain
		}
		return out, nil
	case []float32:
		if depth >= maxDepth {
			return nil, fail(ErrTooLarge, "JSON nesting exceeds %d containers; flatten the value", maxDepth)
		}
		return float32JSON(value), nil
	case E:
		if depth >= maxDepth {
			return nil, fail(ErrTooLarge, "JSON nesting exceeds %d containers; flatten the value", maxDepth)
		}
		m := make(map[string]any, len(value))
		for key, item := range value {
			plain, err := plainJSONDepth(item, depth+1, maxDepth)
			if err != nil {
				return nil, err
			}
			m[key] = plain
		}
		return m, nil
	case map[string]any:
		if depth >= maxDepth {
			return nil, fail(ErrTooLarge, "JSON nesting exceeds %d containers; flatten the value", maxDepth)
		}
		m := make(map[string]any, len(value))
		for key, item := range value {
			plain, err := plainJSONDepth(item, depth+1, maxDepth)
			if err != nil {
				return nil, err
			}
			m[key] = plain
		}
		return m, nil
	default:
		return value, nil
	}
}

func validateJSON(value any) error {
	return validateJSONDepth(value, 0, MaxJSONDepth)
}

func validateJSONDepth(value any, depth, maxDepth int) error {
	switch value := value.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint8, uint16, uint32:
		return nil
	case string:
		if !utf8.ValidString(value) {
			return fail(ErrType, "JSON text is not valid UTF-8; use the bytes wrapper for binary data")
		}
	case uint:
		if uint64(value) > math.MaxInt64 {
			return fail(ErrType, "JSON integer %d exceeds signed 64-bit range; store it as text", value)
		}
	case uint64:
		if value > math.MaxInt64 {
			return fail(ErrType, "JSON integer %d exceeds signed 64-bit range; store it as text", value)
		}
	case float32:
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return fail(ErrType, "JSON contains NaN or infinity; use a finite number")
		}
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fail(ErrType, "JSON contains NaN or infinity; use a finite number")
		}
	case []any:
		if depth >= maxDepth {
			return fail(ErrTooLarge, "JSON nesting exceeds %d containers; flatten the value", maxDepth)
		}
		for _, item := range value {
			if err := validateJSONDepth(item, depth+1, maxDepth); err != nil {
				return err
			}
		}
	case []float32:
		if depth >= maxDepth {
			return fail(ErrTooLarge, "JSON nesting exceeds %d containers; flatten the value", maxDepth)
		}
		for _, item := range value {
			if math.IsNaN(float64(item)) || math.IsInf(float64(item), 0) {
				return fail(ErrType, "JSON contains NaN or infinity; use a finite number")
			}
		}
	case map[string]any:
		if depth >= maxDepth {
			return fail(ErrTooLarge, "JSON nesting exceeds %d containers; flatten the value", maxDepth)
		}
		for key, item := range value {
			if !utf8.ValidString(key) {
				return fail(ErrType, "JSON object key is not valid UTF-8; use valid Unicode keys")
			}
			if err := validateJSONDepth(item, depth+1, maxDepth); err != nil {
				return err
			}
		}
	default:
		return fail(ErrType, "JSON contains unsupported %T; use JSON scalars, arrays, and string-keyed objects", value)
	}
	return nil
}

type storedValue struct {
	logical any
	storage any
	blob    any
	hash    []byte
	tag     Tag
}

const (
	minInstantMicros int64 = -62_135_596_800_000_000
	maxInstantMicros int64 = 253_402_300_799_999_999
)

func instantStored(micros int64) (storedValue, error) {
	if err := validateInstantMicros(micros); err != nil {
		return storedValue{}, err
	}
	return storedValue{logical: micros, storage: micros, tag: TagInstant}, nil
}

func validateInstantMicros(micros int64) error {
	if micros < minInstantMicros || micros > maxInstantMicros {
		return fail(ErrType, "instant %d is outside RFC 3339 UTC years 0001..9999; use microseconds in [%d,%d]", micros, minInstantMicros, maxInstantMicros)
	}
	return nil
}

func scalarValue(input any) (storedValue, error) {
	if fields, ok := objectFields(input); ok {
		if len(fields) != 1 {
			return storedValue{}, fail(ErrType, "typed value object has %d keys; use exactly one wrapper such as {\"ref\": ...} or {\"json\": ...}", len(fields))
		}
		return wrappedValue(fields[0].Name, fields[0].Value)
	}
	switch value := input.(type) {
	case RefValue:
		return storedValue{logical: value, tag: TagRef}, nil
	case InstantValue:
		return instantStored(value.Micros)
	case BytesValue:
		return bytesStored([]byte(value))
	case []byte:
		return bytesStored(value)
	case VectorValue:
		return vectorStored([]float32(value))
	case []float32:
		return vectorStored(value)
	case JSONValue:
		return jsonStored(value.Value)
	case bool:
		integer := int64(0)
		if value {
			integer = 1
		}
		return storedValue{logical: value, storage: integer, tag: TagBool}, nil
	case int:
		return storedValue{logical: int64(value), storage: int64(value), tag: TagInt}, nil
	case int8:
		return storedValue{logical: int64(value), storage: int64(value), tag: TagInt}, nil
	case int16:
		return storedValue{logical: int64(value), storage: int64(value), tag: TagInt}, nil
	case int32:
		return storedValue{logical: int64(value), storage: int64(value), tag: TagInt}, nil
	case int64:
		return storedValue{logical: value, storage: value, tag: TagInt}, nil
	case uint:
		if uint64(value) > math.MaxInt64 {
			return storedValue{}, fail(ErrType, "integer %d exceeds signed 64-bit range; store it as text", value)
		}
		return storedValue{logical: int64(value), storage: int64(value), tag: TagInt}, nil
	case uint8:
		return storedValue{logical: int64(value), storage: int64(value), tag: TagInt}, nil
	case uint16:
		return storedValue{logical: int64(value), storage: int64(value), tag: TagInt}, nil
	case uint32:
		return storedValue{logical: int64(value), storage: int64(value), tag: TagInt}, nil
	case uint64:
		if value > math.MaxInt64 {
			return storedValue{}, fail(ErrType, "integer %d exceeds signed 64-bit range; store it as text", value)
		}
		return storedValue{logical: int64(value), storage: int64(value), tag: TagInt}, nil
	case float32:
		return floatStored(float64(value))
	case float64:
		return floatStored(value)
	case string:
		return textStored(value)
	case time.Time:
		micros := value.UTC().UnixMicro()
		return instantStored(micros)
	case nil:
		return storedValue{}, fail(ErrType, "null is not a fact value; retract the fact instead")
	default:
		return storedValue{}, fail(ErrType, "unsupported fact value %T; use a scalar or typed wrapper", input)
	}
}

func wrappedValue(name string, value any) (storedValue, error) {
	switch name {
	case "ref":
		return storedValue{logical: RefValue{Target: value}, tag: TagRef}, nil
	case "instant":
		switch value := value.(type) {
		case int64:
			return instantStored(value)
		case string:
			instant, err := parseRFC3339(value)
			if err != nil {
				return storedValue{}, fail(ErrType, "instant %q is invalid; use RFC 3339 UTC or integer microseconds", value)
			}
			micros := instant.UTC().UnixMicro()
			return instantStored(micros)
		default:
			return storedValue{}, fail(ErrType, "instant has type %T; use RFC 3339 text or integer microseconds", value)
		}
	case "bytes":
		encoded, ok := value.(string)
		if !ok {
			return storedValue{}, fail(ErrType, "bytes wrapper has type %T; use padded standard base64 text", value)
		}
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return storedValue{}, fail(ErrType, "bytes value is invalid base64; use padded standard base64: %v", err)
		}
		return bytesStored(decoded)
	case "vector":
		items, ok := value.([]any)
		if !ok {
			return storedValue{}, fail(ErrType, "vector wrapper has type %T; use an array of finite numbers", value)
		}
		vector := make([]float32, 0, len(items))
		for _, item := range items {
			var f float64
			switch item := item.(type) {
			case int64:
				f = float64(item)
			case float64:
				f = item
			default:
				return storedValue{}, fail(ErrType, "vector component has type %T; use finite numbers", item)
			}
			if math.IsNaN(f) || math.IsInf(f, 0) || math.IsInf(float64(float32(f)), 0) {
				return storedValue{}, fail(ErrType, "vector component %v is not a finite float32; use finite float32 values", f)
			}
			vector = append(vector, float32(f))
		}
		return vectorStored(vector)
	case "json":
		return jsonStored(value)
	case "tmp":
		return storedValue{}, fail(ErrType, "tempid is valid only in an entity or ref position; use {\"json\": ...} for literal data")
	default:
		return storedValue{}, fail(ErrType, "unknown typed wrapper %q; use ref, instant, bytes, vector, or json", name)
	}
}

func parseRFC3339(value string) (time.Time, error) {
	zoneStart := len(value) - 1
	if len(value) == 0 || value[zoneStart] != 'Z' {
		zoneStart = len(value) - 6
		if zoneStart < 19 || (value[zoneStart] != '+' && value[zoneStart] != '-') || value[zoneStart+3] != ':' {
			return time.Time{}, fail(ErrType, "instant %q is invalid; use RFC 3339 with Z or a ±HH:MM offset", value)
		}
	}
	if zoneStart < 19 || len(value) < 20 || value[4] != '-' || value[7] != '-' || value[10] != 'T' ||
		value[13] != ':' || value[16] != ':' {
		return time.Time{}, fail(ErrType, "instant %q is invalid; use RFC 3339 date and time separators", value)
	}
	for index := 0; index < zoneStart; index++ {
		switch index {
		case 4, 7, 10, 13, 16:
			continue
		case 19:
			if value[index] != '.' {
				return time.Time{}, fail(ErrType, "instant %q is invalid; fractional seconds must use a dot", value)
			}
		default:
			if value[index] < '0' || value[index] > '9' {
				return time.Time{}, fail(ErrType, "instant %q is invalid; use decimal date and time fields", value)
			}
		}
	}
	if zoneStart == 20 || (zoneStart > 19 && value[19] != '.') {
		return time.Time{}, fail(ErrType, "instant %q is invalid; provide at least one fractional digit after the dot", value)
	}
	if value[len(value)-1] != 'Z' {
		for _, index := range []int{zoneStart + 1, zoneStart + 2, zoneStart + 4, zoneStart + 5} {
			if value[index] < '0' || value[index] > '9' {
				return time.Time{}, fail(ErrType, "instant %q has an invalid timezone offset", value)
			}
		}
	}
	instant, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, err
	}
	return instant, nil
}

func textStored(value string) (storedValue, error) {
	if !utf8.ValidString(value) {
		return storedValue{}, fail(ErrType, "text is not valid UTF-8; use the bytes wrapper for binary data")
	}
	if len(value) > MaxValueBytes {
		return storedValue{}, fail(ErrTooLarge, "text is %d bytes; keep values at or below %d bytes", len(value), MaxValueBytes)
	}
	if len(value) > BlobThreshold {
		hash := indirectDigest(TagTextRef, []byte(value))
		return storedValue{logical: value, storage: hash[:], tag: TagTextRef, blob: value, hash: hash[:]}, nil
	}
	return storedValue{logical: value, storage: value, tag: TagText}, nil
}

func bytesStored(value []byte) (storedValue, error) {
	if len(value) > MaxValueBytes {
		return storedValue{}, fail(ErrTooLarge, "bytes value is %d bytes; keep values at or below %d bytes", len(value), MaxValueBytes)
	}
	copyValue := append([]byte(nil), value...)
	if len(value) > BlobThreshold {
		hash := indirectDigest(TagBytesRef, value)
		return storedValue{logical: copyValue, storage: hash[:], tag: TagBytesRef, blob: copyValue, hash: hash[:]}, nil
	}
	return storedValue{logical: copyValue, storage: copyValue, tag: TagBytes}, nil
}

func vectorStored(value []float32) (storedValue, error) {
	if len(value) == 0 {
		return storedValue{}, fail(ErrType, "vector is empty; provide at least one finite float32 component")
	}
	if len(value)*4 > MaxValueBytes {
		return storedValue{}, fail(ErrTooLarge, "vector is %d bytes; keep values at or below %d bytes", len(value)*4, MaxValueBytes)
	}
	data := make([]byte, len(value)*4)
	copyValue := make([]float32, len(value))
	for i, component := range value {
		if math.IsNaN(float64(component)) || math.IsInf(float64(component), 0) {
			return storedValue{}, fail(ErrType, "vector component %v is not finite; use finite float32 values", component)
		}
		copyValue[i] = component
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(component))
	}
	hash := indirectDigest(TagVector, data)
	return storedValue{logical: copyValue, storage: hash[:], tag: TagVector, blob: data, hash: hash[:]}, nil
}

func indirectDigest(tag Tag, data []byte) [sha256.Size]byte {
	var domain byte
	switch tag {
	case TagVector:
		domain = 7
	case TagTextRef:
		domain = 8
	case TagBytesRef:
		domain = 9
	}
	hasher := sha256.New()
	_, _ = hasher.Write([]byte{domain})
	_, _ = hasher.Write(data)
	var result [sha256.Size]byte
	copy(result[:], hasher.Sum(nil))
	return result
}

func jsonStored(value any) (storedValue, error) {
	plain, err := plainJSON(value)
	if err != nil {
		return storedValue{}, err
	}
	if validationErr := validateJSON(plain); validationErr != nil {
		return storedValue{}, validationErr
	}
	data, err := canonicalJSON(plain)
	if err != nil {
		return storedValue{}, err
	}
	if len(data) > MaxValueBytes {
		return storedValue{}, fail(ErrTooLarge, "canonical JSON is %d bytes; keep values at or below %d bytes", len(data), MaxValueBytes)
	}
	var logical any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&logical); err != nil {
		return storedValue{}, wrap(ErrType, err, "canonical JSON cannot be decoded; use JSON-compatible data")
	}
	logical = normalizeJSONNumbers(logical)
	return storedValue{logical: logical, storage: string(data), tag: TagJSON}, nil
}

func normalizeJSONNumbers(value any) any {
	switch value := value.(type) {
	case json.Number:
		if !strings.ContainsAny(string(value), ".eE") {
			if integer, err := value.Int64(); err == nil {
				return integer
			}
		}
		f, err := value.Float64()
		if err != nil {
			return value.String()
		}
		return f
	case []any:
		for i := range value {
			value[i] = normalizeJSONNumbers(value[i])
		}
	case map[string]any:
		for key := range value {
			value[key] = normalizeJSONNumbers(value[key])
		}
	}
	return value
}

func floatStored(value float64) (storedValue, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return storedValue{}, fail(ErrType, "float %v is not finite; use a finite IEEE 754 value", value)
	}
	return storedValue{logical: value, storage: value, tag: TagFloat}, nil
}

func parseTagName(name string) (Tag, bool) {
	for tag, candidate := range tagNames {
		if name == candidate || (name == "text" && candidate == "text_ref") || (name == "bytes" && candidate == "bytes_ref") {
			return Tag(tag), true
		}
	}
	return 0, false
}

func tagCompatible(expected string, actual Tag) bool {
	switch expected {
	case "text":
		return actual == TagText || actual == TagTextRef
	case "bytes":
		return actual == TagBytes || actual == TagBytesRef
	default:
		tag, ok := parseTagName(expected)
		return ok && tag == actual
	}
}

func formatInstant(micros int64) string {
	return time.UnixMicro(micros).UTC().Format("2006-01-02T15:04:05.000000Z")
}
