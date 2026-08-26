package fgraph

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestConformance(t *testing.T) {
	root := filepath.Join("..", "conformance", "cases")
	paths := []string{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type().IsRegular() && filepath.Ext(path) == ".json" {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no conformance cases found")
	}
	sort.Strings(paths)
	for _, path := range paths {
		name, relErr := filepath.Rel(root, path)
		if relErr != nil {
			t.Fatal(relErr)
		}
		t.Run(filepath.ToSlash(name), func(t *testing.T) {
			// #nosec G304 -- every path is produced by walking the fixed repository conformance root above.
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			value, err := decodeConformanceJSON(data)
			if err != nil {
				t.Fatal(err)
			}
			caseMap, ok := objectMap(value)
			if !ok {
				t.Fatalf("case is %T", value)
			}
			steps, ok := caseMap["steps"].([]any)
			if !ok {
				t.Fatal("case steps are missing")
			}
			db, err := Open(":memory:", WithClock(func() int64 { return 1767225600000000 }))
			if err != nil {
				t.Fatal(err)
			}
			defer closeTest(t, db)
			runConformanceSteps(t, db, steps)
		})
	}
}

func decodeConformanceJSON(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("unexpected trailing conformance JSON value %v", trailing)
		}
		return nil, err
	}
	return normalizeConformanceNumbers(value), nil
}

func normalizeConformanceNumbers(value any) any {
	switch value := value.(type) {
	case json.Number:
		if !strings.ContainsAny(value.String(), ".eE") {
			if integer, err := value.Int64(); err == nil {
				return integer
			}
			// Keep an out-of-range integer distinct from a rounded float so the
			// implementation boundary can prove that it rejects the exact input.
			return value
		}
		floating, err := value.Float64()
		if err != nil {
			return value
		}
		return floating
	case []any:
		for index := range value {
			value[index] = normalizeConformanceNumbers(value[index])
		}
	case map[string]any:
		for key := range value {
			value[key] = normalizeConformanceNumbers(value[key])
		}
	}
	return value
}

func runConformanceSteps(t *testing.T, db *DB, steps []any) {
	t.Helper()
	ctx := context.Background()
	for index, raw := range steps {
		step, ok := objectMap(raw)
		if !ok {
			t.Fatalf("step %d is %T", index, raw)
		}
		var actual any
		var err error
		switch {
		case step["stats"] != nil:
			actual, err = db.Stats(ctx)
		case step["tx"] != nil:
			options := conformanceTxOptions(t, index, step["options"])
			actual, err = db.Transact(ctx, step["tx"], options...)
		case step["declare"] != nil:
			actual, err = runDeclareStep(ctx, db, step["declare"])
		case step["shape"] != nil:
			actual, err = runShapeStep(ctx, db, step["shape"])
		case step["receipt"] != nil:
			tx, ok := step["receipt"].(int64)
			if !ok {
				t.Fatalf("step %d receipt is %T; expected an integer transaction id", index, step["receipt"])
			}
			actual, err = db.Receipt(ctx, tx)
		case step["undo"] != nil:
			undo, undoOK := objectMap(step["undo"])
			if !undoOK {
				t.Fatalf("step %d undo is %T; expected an object", index, step["undo"])
			}
			target, targetOK := undo["target"].(int64)
			if !targetOK {
				t.Fatalf("step %d undo target is %T; expected an integer", index, undo["target"])
			}
			delete(undo, "target")
			actual, err = db.Undo(ctx, target, conformanceTxOptions(t, index, undo)...)
		case step["q"] != nil:
			args := map[string]any{}
			if rawArgs, exists := step["args"]; exists {
				var argsOK bool
				args, argsOK = objectMap(rawArgs)
				if !argsOK {
					t.Fatalf("step %d query args are %T", index, rawArgs)
				}
			}
			actual, err = db.QueryJSON(ctx, step["q"], args)
		case step["explain"] != nil:
			args := map[string]any{}
			if rawArgs, exists := step["args"]; exists {
				var argsOK bool
				args, argsOK = objectMap(rawArgs)
				if !argsOK {
					t.Fatalf("step %d explain args are %T", index, rawArgs)
				}
			}
			actual, err = db.ExplainJSON(ctx, step["explain"], args)
		case step["datoms"] != nil:
			actual, err = runDatomsStep(ctx, db, step["datoms"])
		case step["schema"] != nil:
			actual, err = runSchemaStep(ctx, db, step["schema"])
		case step["schema_manifest"] != nil:
			actual, err = db.SchemaManifest(ctx)
		case step["schema_check"] != nil:
			manifest := conformanceSchemaManifest(t, index, step["schema_check"])
			actual, err = db.CheckSchemaManifest(ctx, manifest)
		case step["schema_apply"] != nil:
			application, applicationOK := objectMap(step["schema_apply"])
			if !applicationOK {
				t.Fatalf("step %d schema_apply is %T; expected an object", index, step["schema_apply"])
			}
			manifest := conformanceSchemaManifest(t, index, application["manifest"])
			actual, err = db.ApplySchemaManifest(ctx, manifest, conformanceTxOptions(t, index, application)...)
		case step["validate"] != nil:
			actual, err = db.Validate(ctx, step["validate"])
		case step["entity"] != nil:
			actual, err = db.Entity(ctx, step["entity"])
		case step["history"] != nil:
			items, itemsOK := step["history"].([]any)
			if !itemsOK || len(items) < 1 || len(items) > 2 {
				t.Fatalf("step %d history is %T; expected one or two items", index, step["history"])
			}
			if len(items) == 1 {
				actual, err = db.History(ctx, items[0])
			} else {
				attr, attrOK := items[1].(string)
				if !attrOK {
					t.Fatalf("step %d history attribute is %T", index, items[1])
				}
				actual, err = db.History(ctx, items[0], attr)
			}
		case step["diff"] != nil:
			items, itemsOK := step["diff"].([]any)
			if !itemsOK || len(items) != 2 {
				t.Fatalf("step %d diff is %T; expected two transaction ids", index, step["diff"])
			}
			from, fromOK := items[0].(int64)
			to, toOK := items[1].(int64)
			if !fromOK || !toOK {
				t.Fatalf("step %d diff needs integer transaction ids", index)
			}
			actual, err = db.Diff(ctx, from, to)
		case step["why"] != nil:
			items, itemsOK := step["why"].([]any)
			if !itemsOK || len(items) < 1 || len(items) > 2 {
				t.Fatalf("step %d why is %T; expected one or two items", index, step["why"])
			}
			if len(items) == 1 {
				actual, err = db.Why(ctx, items[0])
			} else {
				attr, attrOK := items[1].(string)
				if !attrOK {
					t.Fatalf("step %d why attribute is %T", index, items[1])
				}
				actual, err = db.Why(ctx, items[0], attr)
			}
		case step["search"] != nil:
			actual, err = runSearchStep(ctx, db, step["search"])
		case step["attributes"] != nil:
			options, optionsOK := objectMap(step["attributes"])
			if !optionsOK {
				t.Fatalf("step %d attributes is %T; expected an object", index, step["attributes"])
			}
			prefix := ""
			if rawPrefix, exists := options["prefix"]; exists {
				var prefixOK bool
				prefix, prefixOK = rawPrefix.(string)
				if !prefixOK {
					t.Fatalf("step %d attributes prefix is %T; expected text", index, rawPrefix)
				}
			}
			includeSystem := false
			if rawSystem, exists := options["include_system"]; exists {
				var systemOK bool
				includeSystem, systemOK = rawSystem.(bool)
				if !systemOK {
					t.Fatalf("step %d attributes include_system is %T; expected a boolean", index, rawSystem)
				}
			}
			actual, err = db.Attributes(ctx, prefix, includeSystem)
		case step["at"] != nil:
			view, viewErr := db.ViewAt(ctx, step["at"])
			if viewErr != nil {
				err = viewErr
				break
			}
			inner, ok := step["steps"].([]any)
			if !ok {
				t.Fatalf("step %d at has no inner steps", index)
			}
			runConformanceSteps(t, view, inner)
			continue
		case step["facts"] != nil:
			actual, err = db.RawFacts(ctx, false)
			actual = normalizeRawBlobs(actual)
		default:
			t.Fatalf("step %d has unknown kind: %v", index, step)
		}
		if expectedError, ok := step["error"].(string); ok {
			if err == nil {
				t.Fatalf("step %d: expected %s, got nil", index, expectedError)
			}
			if name := ErrorName(err); name != expectedError {
				t.Fatalf("step %d: error name=%s want=%s: %v", index, name, expectedError, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("step %d: %v", index, err)
		}
		expected, hasExpectation := step["expect"]
		if !hasExpectation {
			continue
		}
		actual = jsonShape(t, actual)
		unorderedRows := false
		if rawQuery, ok := step["q"]; ok {
			query, queryOK := objectMap(rawQuery)
			if queryOK {
				order, hasOrder := query["order"].([]any)
				unorderedRows = !hasOrder || len(order) == 0
			}
		}
		if mismatch := subsetMismatch(expected, actual, "$", unorderedRows); mismatch != "" {
			pretty, marshalErr := json.MarshalIndent(actual, "", "  ")
			if marshalErr != nil {
				t.Fatalf("step %d: %s; cannot render actual: %v", index, mismatch, marshalErr)
			}
			t.Fatalf("step %d: %s\nactual: %s", index, mismatch, pretty)
		}
	}
}

func conformanceSchemaManifest(t *testing.T, index int, raw any) SchemaManifest {
	t.Helper()
	encoded, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("step %d schema manifest cannot be encoded: %v", index, err)
	}
	var manifest SchemaManifest
	if err := json.Unmarshal(encoded, &manifest); err != nil {
		t.Fatalf("step %d schema manifest cannot be decoded: %v", index, err)
	}
	return manifest
}

func conformanceTxOptions(t *testing.T, index int, raw any) []TxOption {
	t.Helper()
	if raw == nil {
		return nil
	}
	values, ok := objectMap(raw)
	if !ok {
		t.Fatalf("step %d transaction options are %T; expected an object", index, raw)
	}
	options := []TxOption{}
	if source, exists := values["source"]; exists {
		text, textOK := source.(string)
		if !textOK {
			t.Fatalf("step %d transaction source is %T; expected text", index, source)
		}
		options = append(options, WithSource(text))
	}
	if by, exists := values["by"]; exists {
		text, textOK := by.(string)
		if !textOK {
			t.Fatalf("step %d transaction by is %T; expected text", index, by)
		}
		options = append(options, WithBy(text))
	}
	if meta, exists := values["meta"]; exists {
		options = append(options, WithMeta(meta))
	}
	if facts, exists := values["tx"]; exists {
		options = append(options, WithTxFacts(facts))
	}
	if operationID, exists := values["operation_id"]; exists {
		text, textOK := operationID.(string)
		if !textOK {
			t.Fatalf("step %d transaction operation_id is %T; expected text", index, operationID)
		}
		options = append(options, WithOperationID(text))
	}
	if basis, exists := values["if_basis_tx"]; exists {
		tx, txOK := basis.(int64)
		if !txOK {
			t.Fatalf("step %d transaction if_basis_tx is %T; expected an integer", index, basis)
		}
		options = append(options, IfBasis(tx))
	}
	return options
}

func runDeclareStep(ctx context.Context, db *DB, raw any) (TxReport, error) {
	declaration, ok := objectMap(raw)
	if !ok {
		return TxReport{}, fmt.Errorf("declare is %T", raw)
	}
	attr, attrOK := declaration["attr"].(string)
	if !attrOK {
		return TxReport{}, fail(ErrType, "declare attr has type %T; use an attribute name", declaration["attr"])
	}
	options := []DeclareOption{}
	if ref, exists := declaration["ref"].(bool); exists && ref {
		options = append(options, Ref())
	} else if typeName, exists := declaration["type"].(string); exists {
		options = append(options, Type(typeName))
	}
	if value, exists := declaration["many"].(bool); exists {
		options = append(options, func(o *declareOptions) { o.many = &value })
	}
	if value, exists := declaration["unique"].(bool); exists {
		options = append(options, func(o *declareOptions) { o.unique = &value })
	}
	if value, exists := declaration["nohistory"].(bool); exists {
		options = append(options, NoHistory(value))
	}
	if value, exists := declaration["dims"].(int64); exists {
		options = append(options, Dims(value))
	}
	if value, exists := declaration["doc"].(string); exists {
		options = append(options, Doc(value))
	}
	if value, exists := declaration["vector_model"].(string); exists {
		options = append(options, VectorModel(value))
	}
	return db.Declare(ctx, attr, options...)
}

func runShapeStep(ctx context.Context, db *DB, raw any) (TxReport, error) {
	shape, ok := objectMap(raw)
	if !ok {
		return TxReport{}, fail(ErrType, "shape is %T; use an object", raw)
	}
	name, ok := shape["name"].(string)
	if !ok {
		return TxReport{}, fail(ErrType, "shape name is %T; use text", shape["name"])
	}
	definition := ShapeDefinition{}
	for key, destination := range map[string]*[]string{"required": &definition.Required, "allowed": &definition.Allowed} {
		if rawValues, exists := shape[key]; exists {
			values, ok := rawValues.([]any)
			if !ok {
				return TxReport{}, fail(ErrType, "shape %s is %T; use an array", key, rawValues)
			}
			for _, rawValue := range values {
				value, ok := rawValue.(string)
				if !ok {
					return TxReport{}, fail(ErrType, "shape %s item is %T; use attribute text", key, rawValue)
				}
				*destination = append(*destination, value)
			}
		}
	}
	if closed, exists := shape["closed"]; exists {
		value, ok := closed.(bool)
		if !ok {
			return TxReport{}, fail(ErrType, "shape closed is %T; use a boolean", closed)
		}
		definition.Closed = value
	}
	return db.DeclareShape(ctx, name, definition)
}

func runSchemaStep(ctx context.Context, db *DB, raw any) (SchemaSnapshot, error) {
	options, ok := objectMap(raw)
	if !ok {
		return SchemaSnapshot{}, fail(ErrType, "schema is %T; use an object", raw)
	}
	prefix := ""
	if rawPrefix, exists := options["prefix"]; exists {
		value, ok := rawPrefix.(string)
		if !ok {
			return SchemaSnapshot{}, fail(ErrType, "schema prefix is %T; use text", rawPrefix)
		}
		prefix = value
	}
	includeSystem := false
	if rawSystem, exists := options["include_system"]; exists {
		value, ok := rawSystem.(bool)
		if !ok {
			return SchemaSnapshot{}, fail(ErrType, "schema include_system is %T; use a boolean", rawSystem)
		}
		includeSystem = value
	}
	return db.Schema(ctx, prefix, includeSystem)
}

func runDatomsStep(ctx context.Context, db *DB, raw any) (DatomPage, error) {
	options, ok := objectMap(raw)
	if !ok {
		return DatomPage{}, fail(ErrType, "datoms is %T; use an object", raw)
	}
	request := DatomOptions{}
	if value, exists := options["index"].(string); exists {
		request.Index = value
	}
	if value, exists := options["source"].(string); exists {
		request.Source = value
	}
	if values, exists := options["components"].([]any); exists {
		request.Components = values
	}
	if value, exists := options["cursor"].(string); exists {
		request.Cursor = value
	}
	if value, exists := options["limit"].(int64); exists {
		request.Limit = int(value)
	}
	return db.Datoms(ctx, request)
}

func runSearchStep(ctx context.Context, db *DB, raw any) (SearchResult, error) {
	value, valueOK := objectMap(raw)
	if !valueOK {
		return SearchResult{}, fail(ErrType, "search step has type %T; use an object", raw)
	}
	options := SearchOpts{}
	if text, exists := value["text"]; exists {
		var textOK bool
		options.Text, textOK = text.(string)
		if !textOK {
			return SearchResult{}, fail(ErrType, "search text has type %T", text)
		}
	}
	if attr, exists := value["vector_attribute"]; exists {
		var attrOK bool
		options.VectorAttribute, attrOK = attr.(string)
		if !attrOK {
			return SearchResult{}, fail(ErrType, "search vector_attribute has type %T", attr)
		}
	}
	if rawAttributes, exists := value["text_attributes"]; exists {
		attributes, ok := rawAttributes.([]any)
		if !ok {
			return SearchResult{}, fail(ErrType, "search text_attributes has type %T", rawAttributes)
		}
		for _, rawAttribute := range attributes {
			attribute, ok := rawAttribute.(string)
			if !ok {
				return SearchResult{}, fail(ErrType, "search text attribute has type %T", rawAttribute)
			}
			options.TextAttributes = append(options.TextAttributes, attribute)
		}
	}
	options.K = int(asInt64(value["k"]))
	options.Expand = int(asInt64(value["expand"]))
	if vector, exists := value["vector"].([]any); exists {
		wrapped, err := wrappedValue("vector", vector)
		if err != nil {
			return SearchResult{}, err
		}
		var vectorOK bool
		options.Vector, vectorOK = wrapped.logical.([]float32)
		if !vectorOK {
			return SearchResult{}, fail(ErrType, "search vector decoded as %T", wrapped.logical)
		}
	}
	if filters, exists := value["filters"].([]any); exists {
		for _, filter := range filters {
			items, filterOK := filter.([]any)
			if !filterOK {
				return SearchResult{}, fail(ErrType, "search filter has type %T; use a clause array", filter)
			}
			options.Filters = append(options.Filters, items)
		}
	}
	return db.Search(ctx, options)
}

func normalizeRawBlobs(value any) any {
	rows, ok := value.([][]any)
	if !ok {
		return value
	}
	for _, row := range rows {
		if data, ok := row[3].([]byte); ok {
			row[3] = map[string]any{"hex": hex.EncodeToString(data)}
		}
	}
	return rows
}

func jsonShape(t *testing.T, value any) any {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var result any
	if err := decoder.Decode(&result); err != nil {
		t.Fatal(err)
	}
	return normalizeJSONNumbers(result)
}

func subsetMismatch(expected, actual any, path string, unorderedRows bool) string {
	switch expected := expected.(type) {
	case Object:
		actualMap, ok := objectMap(actual)
		if !ok {
			if typed, ok := actual.(map[string]any); ok {
				actualMap = typed
			} else {
				return fmt.Sprintf("%s: expected object, got %T", path, actual)
			}
		}
		allowExtra := false
		expectedKeys := 0
		for _, field := range expected.Fields {
			if field.Name == "..." && field.Value == true {
				allowExtra = true
				continue
			}
			expectedKeys++
			value, exists := actualMap[field.Name]
			if !exists {
				return fmt.Sprintf("%s.%s: missing", path, field.Name)
			}
			if mismatch := subsetMismatch(field.Value, value, path+"."+field.Name, unorderedRows); mismatch != "" {
				return mismatch
			}
		}
		if !allowExtra && len(actualMap) != expectedKeys {
			return fmt.Sprintf("%s: object key count=%d want=%d", path, len(actualMap), expectedKeys)
		}
		return ""
	case map[string]any:
		actualMap, ok := actual.(map[string]any)
		if !ok {
			return fmt.Sprintf("%s: expected object, got %T", path, actual)
		}
		allowExtra := false
		expectedKeys := 0
		for key, expectedValue := range expected {
			if key == "..." && expectedValue == true {
				allowExtra = true
				continue
			}
			expectedKeys++
			actualValue, exists := actualMap[key]
			if !exists {
				return fmt.Sprintf("%s.%s: missing", path, key)
			}
			if mismatch := subsetMismatch(expectedValue, actualValue, path+"."+key, unorderedRows); mismatch != "" {
				return mismatch
			}
		}
		if !allowExtra && len(actualMap) != expectedKeys {
			return fmt.Sprintf("%s: object key count=%d want=%d", path, len(actualMap), expectedKeys)
		}
		return ""
	case []any:
		actualItems, ok := actual.([]any)
		if !ok {
			return fmt.Sprintf("%s: expected array, got %T", path, actual)
		}
		if len(expected) != len(actualItems) {
			return fmt.Sprintf("%s: array length=%d want=%d", path, len(actualItems), len(expected))
		}
		if path != "$.rows" || !unorderedRows {
			for i := range expected {
				if mismatch := subsetMismatch(expected[i], actualItems[i], fmt.Sprintf("%s[%d]", path, i), unorderedRows); mismatch != "" {
					return mismatch
				}
			}
			return ""
		}
		remaining := append([]any(nil), actualItems...)
		for i, wanted := range expected {
			matched := -1
			for candidate, value := range remaining {
				if subsetMismatch(wanted, value, fmt.Sprintf("%s[%d]", path, i), false) == "" {
					matched = candidate
					break
				}
			}
			if matched < 0 {
				return fmt.Sprintf("%s[%d]: no unordered match for %v", path, i, wanted)
			}
			remaining = append(remaining[:matched], remaining[matched+1:]...)
		}
		return ""
	default:
		if comparison, numeric := orderedNumericCompare(expected, actual); numeric && comparison == 0 {
			return ""
		}
		if !reflect.DeepEqual(expected, actual) {
			return fmt.Sprintf("%s: got=%v (%T), want=%v (%T)", path, actual, actual, expected, expected)
		}
		return ""
	}
}
