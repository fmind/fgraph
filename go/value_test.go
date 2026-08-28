package fgraph

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestCanonicalJSONScalarAndEscapeMatrix(t *testing.T) {
	value := map[string]any{
		"nil": nil, "true": true, "false": false,
		"int": int(1), "int8": int8(2), "int16": int16(3), "int32": int32(4), "int64": int64(5),
		"uint": uint(6), "uint8": uint8(7), "uint16": uint16(8), "uint32": uint32(9), "uint64": uint64(10),
		"float32": float32(1.25), "float64": 2.5,
		"array":  []any{nil, true, "x"},
		"escape": "\"\\\b\f\n\r\t\x00\x1f",
	}
	encoded, err := canonicalJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	wantEscapes := `"escape":"\"\\\b\f\n\r\t\u0000\u001f"`
	if !strings.Contains(string(encoded), wantEscapes) {
		t.Fatalf("canonical escapes = %s", encoded)
	}
	if _, err := DecodeJSON(strings.NewReader(string(encoded))); err != nil {
		t.Fatalf("canonical output does not decode: %v", err)
	}
}

func TestDecodeJSONRejectsUnpairedUnicodeSurrogates(t *testing.T) {
	for _, raw := range []string{
		`"\ud800"`,
		`"\udfff"`,
		`{"\ud800":1}`,
	} {
		if _, err := DecodeJSON(strings.NewReader(raw)); !errors.Is(err, ErrType) {
			t.Errorf("DecodeJSON(%s) error = %v, want TypeError", raw, err)
		}
	}

	for raw, want := range map[string]string{
		`"\ud83d\ude00"`: "😀",
		`"\ufffd"`:       "�",
		`"�"`:            "�",
		`"\\ud800"`:      `\ud800`,
	} {
		decoded, err := DecodeJSON(strings.NewReader(raw))
		if err != nil || decoded != want {
			t.Errorf("DecodeJSON(%s) = %#v, %v; want %q", raw, decoded, err, want)
		}
	}
}

func TestCanonicalIntegralFloatBoundariesRemainStrictlyDecodable(t *testing.T) {
	positiveLimit := math.Ldexp(1, 63)
	negativeLimit := -positiveLimit
	cases := []struct {
		value          float64
		wantScientific bool
	}{
		{value: positiveLimit, wantScientific: true},
		{value: negativeLimit},
		{value: math.Nextafter(negativeLimit, math.Inf(-1)), wantScientific: true},
		{value: 1e20, wantScientific: true},
		{value: math.Nextafter(1e21, 0), wantScientific: true},
	}
	for _, test := range cases {
		encoded, err := canonicalJSON(test.value)
		if err != nil {
			t.Fatalf("canonicalJSON(%v) = %v", test.value, err)
		}
		hasExponent := bytes.Contains(encoded, []byte("e"))
		if hasExponent != test.wantScientific {
			t.Errorf("canonicalJSON(%v) = %s, scientific=%t", test.value, encoded, hasExponent)
		}
		if _, err := DecodeJSON(bytes.NewReader(encoded)); err != nil {
			t.Errorf("strict decode of %s = %v", encoded, err)
		}
	}
}

func TestJSONValidationFailures(t *testing.T) {
	valid := []any{
		nil, true, "text", int(1), int8(1), int16(1), int32(1), int64(1),
		uint(1), uint8(1), uint16(1), uint32(1), uint64(1), float32(1), float64(1),
		[]any{map[string]any{"x": 1}},
		[]float32{1},
		map[string]any{"x": []any{1}},
	}
	for _, value := range valid {
		if err := validateJSON(value); err != nil {
			t.Errorf("valid %T: %v", value, err)
		}
	}
	invalid := []any{
		uint64(math.MaxInt64) + 1,
		float32(float32(math.Inf(1))), math.NaN(), math.Inf(-1),
		[]any{math.NaN()},
		[]float32{float32(math.Inf(1))},
		map[string]any{"x": make(chan int)},
		make(chan int),
	}
	if uint64(^uint(0)) > math.MaxInt64 {
		invalid = append(invalid, ^uint(0))
		if _, err := scalarValue(JSON(^uint(0))); !errors.Is(err, ErrType) {
			t.Fatalf("out-of-range nested uint JSON wrapper error = %v", err)
		}
	}
	for _, value := range invalid {
		if err := validateJSON(value); !errors.Is(err, ErrType) {
			t.Errorf("invalid %T error = %v", value, err)
		}
		if _, err := canonicalJSON(value); !errors.Is(err, ErrType) {
			t.Errorf("canonical invalid %T error = %v", value, err)
		}
	}
}

func TestJSONNestingDepthAndCyclesAreBounded(t *testing.T) {
	nested := func(depth int) any {
		var value any = int64(1)
		for range depth {
			value = []any{value}
		}
		return value
	}

	if _, err := jsonStored(nested(MaxJSONDepth)); err != nil {
		t.Fatalf("maximum JSON nesting depth rejected: %v", err)
	}
	if _, err := jsonStored(nested(MaxJSONDepth + 1)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("over-depth in-memory JSON error = %v", err)
	}
	if _, err := DecodeJSON(strings.NewReader(strings.Repeat("[", MaxJSONDocumentDepth) + "1" + strings.Repeat("]", MaxJSONDocumentDepth))); err != nil {
		t.Fatalf("maximum wire-document nesting depth rejected: %v", err)
	}
	if _, err := DecodeJSON(strings.NewReader(strings.Repeat("[", MaxJSONDocumentDepth+1) + "1" + strings.Repeat("]", MaxJSONDocumentDepth+1))); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("over-depth wire-document JSON error = %v", err)
	}

	cycle := map[string]any{}
	cycle["self"] = cycle
	if _, err := jsonStored(cycle); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("cyclic JSON error = %v", err)
	}
}

func TestScalarAndWrapperFailureMatrix(t *testing.T) {
	valid := []any{
		uint8(1), uint16(1), uint32(1), RefTo("target"), Instant(0),
		BytesValue{1},
		VectorValue{1},
		JSONValue{Value: map[string]any{"x": 1}},
		time.Unix(0, 0),
		Object{Fields: []Field{{Name: "instant", Value: "2026-08-24T10:00:00Z"}}},
		Object{Fields: []Field{{Name: "bytes", Value: "aGVsbG8="}}},
		Object{Fields: []Field{{Name: "vector", Value: []any{int64(1), 0.5}}}},
	}
	for _, value := range valid {
		if _, err := scalarValue(value); err != nil {
			t.Errorf("valid scalar %T = %v", value, err)
		}
	}
	invalid := []any{
		Object{},
		Object{Fields: []Field{{Name: "a", Value: 1}, {Name: "b", Value: 2}}},
		Object{Fields: []Field{{Name: "instant", Value: "bad"}}},
		Object{Fields: []Field{{Name: "instant", Value: "2026-08-24T10:00:00,5Z"}}},
		Object{Fields: []Field{{Name: "instant", Value: true}}},
		Object{Fields: []Field{{Name: "bytes", Value: true}}},
		Object{Fields: []Field{{Name: "bytes", Value: "not-base64"}}},
		Object{Fields: []Field{{Name: "vector", Value: true}}},
		Object{Fields: []Field{{Name: "vector", Value: []any{"x"}}}},
		Object{Fields: []Field{{Name: "vector", Value: []any{math.Inf(1)}}}},
		Object{Fields: []Field{{Name: "vector", Value: []any{1e100}}}},
		Object{Fields: []Field{{Name: "tmp", Value: "t"}}},
		Object{Fields: []Field{{Name: "unknown", Value: 1}}},
		uint(math.MaxUint), uint64(math.MaxInt64) + 1, nil,
		struct{}{},
		math.NaN(), math.Inf(1), string([]byte{0xff}),
	}
	for _, value := range invalid {
		if _, err := scalarValue(value); err == nil {
			t.Errorf("invalid scalar %#v unexpectedly accepted", value)
		}
	}
}

func TestStorageThresholdsAndTags(t *testing.T) {
	shortText, shortTextErr := textStored(strings.Repeat("a", BlobThreshold))
	longText, longTextErr := textStored(strings.Repeat("a", BlobThreshold+1))
	if shortTextErr != nil || longTextErr != nil {
		t.Fatalf("text storage errors = %v/%v", shortTextErr, longTextErr)
	}
	if shortText.tag != TagText || longText.tag != TagTextRef || len(longText.hash) != 32 {
		t.Fatalf("text tags = %v/%v", shortText.tag, longText.tag)
	}
	shortBytes, shortBytesErr := bytesStored(make([]byte, BlobThreshold))
	longBytes, longBytesErr := bytesStored(make([]byte, BlobThreshold+1))
	if shortBytesErr != nil || longBytesErr != nil {
		t.Fatalf("bytes storage errors = %v/%v", shortBytesErr, longBytesErr)
	}
	if shortBytes.tag != TagBytes || longBytes.tag != TagBytesRef || len(longBytes.hash) != 32 {
		t.Fatalf("bytes tags = %v/%v", shortBytes.tag, longBytes.tag)
	}
	for _, call := range []func() error{
		func() error { _, err := textStored(strings.Repeat("x", MaxValueBytes+1)); return err },
		func() error { _, err := bytesStored(make([]byte, MaxValueBytes+1)); return err },
		func() error { _, err := vectorStored(make([]float32, MaxValueBytes/4+1)); return err },
		func() error { _, err := vectorStored([]float32{float32(math.NaN())}); return err },
		func() error { _, err := jsonStored(strings.Repeat("x", MaxValueBytes+1)); return err },
	} {
		if err := call(); err == nil {
			t.Fatal("oversize/nonfinite storage unexpectedly accepted")
		}
	}
	for tag, name := range tagNames {
		got, ok := parseTagName(name)
		if !ok || got != Tag(tag) {
			t.Fatalf("parseTagName(%q) = %d,%t", name, got, ok)
		}
	}
	if _, ok := parseTagName("unknown"); ok {
		t.Fatal("unknown tag accepted")
	}
	if !tagCompatible("text", TagTextRef) || !tagCompatible("bytes", TagBytesRef) || !tagCompatible("int", TagInt) || tagCompatible("int", TagText) {
		t.Fatal("logical tag compatibility mismatch")
	}
}

func TestNormalizeJSONNumbers(t *testing.T) {
	value := map[string]any{
		"integer": json.Number("1"),
		"float":   json.Number("1.5"),
		"array":   []any{json.Number("2"), map[string]any{"x": json.Number("3e0")}},
	}
	normalized, ok := normalizeJSONNumbers(value).(map[string]any)
	if !ok {
		t.Fatalf("normalized type = %T", normalizeJSONNumbers(value))
	}
	if normalized["integer"] != int64(1) || normalized["float"] != 1.5 {
		t.Fatalf("normalized = %#v", normalized)
	}
}
