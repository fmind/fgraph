package fgraph

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

type namedIdentity struct {
	name string
	id   int64
}

func (db *DB) Schema(ctx context.Context, prefix string, includeSystem bool) (SchemaSnapshot, error) {
	result := SchemaSnapshot{Attributes: []SchemaAttribute{}, Shapes: []ShapeInfo{}}
	readErr := db.withRead(ctx, func(runner sqlRunner) error {
		basis, basisErr := db.basisOn(ctx, runner)
		if basisErr != nil {
			return basisErr
		}
		if db.asOf != nil && *db.asOf < basis {
			basis = *db.asOf
		}
		result.BasisTx = basis
		identities, identitiesErr := db.schemaIdentities(ctx, runner, basis, prefix, includeSystem)
		if identitiesErr != nil {
			return identitiesErr
		}
		for _, identity := range identities {
			schema, schemaErr := db.schemaFor(ctx, runner, identity.id, []plannedFact{})
			if schemaErr != nil {
				return schemaErr
			}
			declared, declaredErr := db.declaredAttribute(ctx, runner, identity.id, identity.name)
			if declaredErr != nil {
				return declaredErr
			}
			observed, observedErr := db.schemaObservation(ctx, runner, identity.id, identity.name)
			if observedErr != nil {
				return observedErr
			}
			result.Attributes = append(result.Attributes, SchemaAttribute{
				Name: identity.name, Declared: declared,
				Effective: effectiveAttribute(schema, declared), Observed: observed,
			})
		}
		var shapesErr error
		result.Shapes, shapesErr = db.readShapes(ctx, runner)
		if shapesErr != nil {
			return shapesErr
		}
		digestAttributes := make([]any, 0, len(result.Attributes))
		for _, attribute := range result.Attributes {
			digestAttributes = append(digestAttributes, map[string]any{
				"name": attribute.Name, "declared": declaredAttributeWire(attribute.Declared),
				"effective": effectiveAttributeWire(attribute.Effective),
			})
		}
		digestShapes := []any{}
		for _, shape := range result.Shapes {
			digestShapes = append(digestShapes, map[string]any{
				"name": shape.Name, "required": stringValues(shape.Required), "allowed": stringValues(shape.Allowed), "closed": shape.Closed,
			})
		}
		encoded, encodeErr := canonicalJSON(map[string]any{"attributes": digestAttributes, "shapes": digestShapes})
		if encodeErr != nil {
			return encodeErr
		}
		digest := sha256.Sum256(encoded)
		result.Digest = "sha256:" + hex.EncodeToString(digest[:])
		return nil
	})
	return result, readErr
}

func declaredAttributeEmpty(value DeclaredAttribute) bool {
	return value.Type == nil && value.Many == nil && value.Unique == nil && value.NoHistory == nil &&
		value.Dims == nil && value.Doc == nil && value.VectorModel == nil
}

func (db *DB) normalizeSchemaManifest(manifest SchemaManifest) (SchemaManifest, error) {
	if manifest.FGraph != "schema/1" {
		return SchemaManifest{}, fail(ErrSchema, "schema manifest fgraph is %q; use schema/1", manifest.FGraph)
	}
	attributes := make([]SchemaManifestAttribute, 0, len(manifest.Attributes))
	seenAttributes := map[string]bool{}
	for _, attribute := range manifest.Attributes {
		if err := validateName(attribute.Name, true); err != nil {
			return SchemaManifest{}, wrap(ErrSchema, err, "schema manifest attribute %q is invalid", attribute.Name)
		}
		if seenAttributes[attribute.Name] {
			return SchemaManifest{}, fail(ErrSchema, "schema manifest repeats attribute %q", attribute.Name)
		}
		seenAttributes[attribute.Name] = true
		if attribute.Declared.Type != nil && !validTypeName(*attribute.Declared.Type) {
			return SchemaManifest{}, fail(ErrSchema, "schema manifest attribute %q has unsupported type %q", attribute.Name, *attribute.Declared.Type)
		}
		if attribute.Declared.Dims != nil && *attribute.Declared.Dims <= 0 {
			return SchemaManifest{}, fail(ErrSchema, "schema manifest attribute %q dims must be positive", attribute.Name)
		}
		if attribute.Declared.VectorModel != nil && strings.TrimSpace(*attribute.Declared.VectorModel) == "" {
			return SchemaManifest{}, fail(ErrSchema, "schema manifest attribute %q vector_model must be non-blank", attribute.Name)
		}
		if !declaredAttributeEmpty(attribute.Declared) {
			attributes = append(attributes, attribute)
		}
	}
	sort.Slice(attributes, func(left, right int) bool { return attributes[left].Name < attributes[right].Name })
	shapes := make([]ShapeInfo, 0, len(manifest.Shapes))
	seenShapes := map[string]bool{}
	for _, shape := range manifest.Shapes {
		name, ok := shape.Name.(string)
		if !ok {
			return SchemaManifest{}, fail(ErrSchema, "schema manifest shape name has type %T; use text", shape.Name)
		}
		if err := validateName(name, false); err != nil {
			return SchemaManifest{}, wrap(ErrSchema, err, "schema manifest shape %q is invalid", name)
		}
		if seenShapes[name] {
			return SchemaManifest{}, fail(ErrSchema, "schema manifest repeats shape %q", name)
		}
		seenShapes[name] = true
		required, err := normalizeShapeAttributes("required", shape.Required)
		if err != nil {
			return SchemaManifest{}, err
		}
		allowed, err := normalizeShapeAttributes("allowed", shape.Allowed)
		if err != nil {
			return SchemaManifest{}, err
		}
		if shape.Closed {
			allowed = sortedUniqueStrings(append(allowed, required...))
		}
		shapes = append(shapes, ShapeInfo{Name: name, Required: required, Allowed: allowed, Closed: shape.Closed})
	}
	sort.Slice(shapes, func(left, right int) bool {
		return fmt.Sprint(shapes[left].Name) < fmt.Sprint(shapes[right].Name)
	})
	attributeWire := make([]any, 0, len(attributes))
	for _, attribute := range attributes {
		attributeWire = append(attributeWire, map[string]any{
			"name": attribute.Name, "declared": declaredAttributeWire(attribute.Declared),
		})
	}
	shapeWire := make([]any, 0, len(shapes))
	for _, shape := range shapes {
		shapeWire = append(shapeWire, map[string]any{
			"name": shape.Name, "required": stringValues(shape.Required),
			"allowed": stringValues(shape.Allowed), "closed": shape.Closed,
		})
	}
	payload := map[string]any{"fgraph": "schema/1", "attributes": attributeWire, "shapes": shapeWire}
	encoded, err := canonicalJSON(payload)
	if err != nil {
		return SchemaManifest{}, err
	}
	digest := sha256.Sum256(encoded)
	return SchemaManifest{
		FGraph: "schema/1", Digest: "sha256:" + hex.EncodeToString(digest[:]), Attributes: attributes, Shapes: shapes,
	}, nil
}

func validTypeName(value string) bool {
	for _, candidate := range []string{"ref", "bool", "int", "float", "text", "instant", "bytes", "vector", "json"} {
		if value == candidate {
			return true
		}
	}
	return false
}

func (db *DB) SchemaManifest(ctx context.Context) (SchemaManifest, error) {
	var manifest SchemaManifest
	err := db.withRead(ctx, func(runner sqlRunner) error {
		var readErr error
		manifest, readErr = db.schemaManifestOn(ctx, runner)
		return readErr
	})
	return manifest, err
}

func (db *DB) schemaManifestOn(ctx context.Context, runner sqlRunner) (SchemaManifest, error) {
	basis, err := db.basisOn(ctx, runner)
	if err != nil {
		return SchemaManifest{}, err
	}
	if db.asOf != nil && *db.asOf < basis {
		basis = *db.asOf
	}
	identities, err := db.schemaIdentities(ctx, runner, basis, "", false)
	if err != nil {
		return SchemaManifest{}, err
	}
	manifest := SchemaManifest{FGraph: "schema/1", Attributes: []SchemaManifestAttribute{}, Shapes: []ShapeInfo{}}
	for _, identity := range identities {
		declared, declaredErr := db.declaredAttribute(ctx, runner, identity.id, identity.name)
		if declaredErr != nil {
			return SchemaManifest{}, declaredErr
		}
		if !declaredAttributeEmpty(declared) {
			manifest.Attributes = append(manifest.Attributes, SchemaManifestAttribute{Name: identity.name, Declared: declared})
		}
	}
	manifest.Shapes, err = db.readShapes(ctx, runner)
	if err != nil {
		return SchemaManifest{}, err
	}
	return db.normalizeSchemaManifest(manifest)
}

func manifestEntries(manifest SchemaManifest) (map[string]any, error) {
	result := map[string]any{}
	for _, attribute := range manifest.Attributes {
		result["attribute:"+attribute.Name] = declaredAttributeWire(attribute.Declared)
	}
	for _, shape := range manifest.Shapes {
		name, ok := shape.Name.(string)
		if !ok {
			return nil, fail(ErrSchema, "schema manifest shape name has type %T; use text", shape.Name)
		}
		result["shape:"+name] = map[string]any{
			"name": name, "required": stringValues(shape.Required),
			"allowed": stringValues(shape.Allowed), "closed": shape.Closed,
		}
	}
	return result, nil
}

func (db *DB) CheckSchemaManifest(ctx context.Context, manifest SchemaManifest) (SchemaManifestCheck, error) {
	desired, err := db.normalizeSchemaManifest(manifest)
	if err != nil {
		return SchemaManifestCheck{}, err
	}
	current, err := db.SchemaManifest(ctx)
	if err != nil {
		return SchemaManifestCheck{}, err
	}
	before, err := manifestEntries(current)
	if err != nil {
		return SchemaManifestCheck{}, err
	}
	after, err := manifestEntries(desired)
	if err != nil {
		return SchemaManifestCheck{}, err
	}
	keys := make([]string, 0, len(before)+len(after))
	seen := map[string]bool{}
	for key := range before {
		seen[key] = true
		keys = append(keys, key)
	}
	for key := range after {
		if !seen[key] {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	changes := []SchemaManifestChange{}
	for _, key := range keys {
		beforeJSON, beforeErr := canonicalJSON(before[key])
		if beforeErr != nil {
			return SchemaManifestCheck{}, beforeErr
		}
		afterJSON, afterErr := canonicalJSON(after[key])
		if afterErr != nil {
			return SchemaManifestCheck{}, afterErr
		}
		if string(beforeJSON) == string(afterJSON) {
			continue
		}
		parts := strings.SplitN(key, ":", 2)
		changes = append(changes, SchemaManifestChange{Kind: parts[0], Name: parts[1], Before: before[key], After: after[key]})
	}
	snapshot, err := db.Schema(ctx, "", false)
	if err != nil {
		return SchemaManifestCheck{}, err
	}
	return SchemaManifestCheck{
		BasisTx: snapshot.BasisTx, Valid: len(changes) == 0, CurrentDigest: current.Digest,
		DesiredDigest: desired.Digest, Changes: changes,
	}, nil
}

func declarationMap(attribute SchemaManifestAttribute) E {
	result := E{"id": attribute.Name}
	declaration := attribute.Declared
	if declaration.Type != nil {
		result[systemNames[8]] = *declaration.Type
	}
	if declaration.Many != nil {
		result[systemNames[5]] = *declaration.Many
	}
	if declaration.Unique != nil {
		result[systemNames[6]] = *declaration.Unique
	}
	if declaration.NoHistory != nil {
		result[systemNames[7]] = *declaration.NoHistory
	}
	if declaration.Dims != nil {
		result[systemNames[9]] = *declaration.Dims
	}
	if declaration.Doc != nil {
		result[systemNames[10]] = *declaration.Doc
	}
	if declaration.VectorModel != nil {
		result[systemNames[14]] = *declaration.VectorModel
	}
	return result
}

func schemaReplacementOperations(current, desired SchemaManifest) ([]any, error) {
	operations := []any{}
	attributeNames := map[string]bool{}
	for _, attribute := range append(current.Attributes, desired.Attributes...) {
		attributeNames[attribute.Name] = true
	}
	orderedAttributes := make([]string, 0, len(attributeNames))
	for name := range attributeNames {
		orderedAttributes = append(orderedAttributes, name)
	}
	sort.Strings(orderedAttributes)
	for _, name := range orderedAttributes {
		for _, schemaAttribute := range []int{5, 6, 7, 8, 9, 10, 14} {
			operations = append(operations, []any{"retract", name, systemNames[schemaAttribute]})
		}
	}
	for _, attribute := range desired.Attributes {
		operations = append(operations, declarationMap(attribute))
	}
	shapeNames := map[string]bool{}
	for _, shape := range append(current.Shapes, desired.Shapes...) {
		name, ok := shape.Name.(string)
		if !ok {
			return nil, fail(ErrSchema, "schema manifest shape name has type %T; use text", shape.Name)
		}
		shapeNames[name] = true
	}
	orderedShapes := make([]string, 0, len(shapeNames))
	for name := range shapeNames {
		orderedShapes = append(orderedShapes, name)
	}
	sort.Strings(orderedShapes)
	for _, name := range orderedShapes {
		for _, schemaAttribute := range []int{16, 17, 18} {
			operations = append(operations, []any{"retract", name, systemNames[schemaAttribute]})
		}
	}
	for _, shape := range desired.Shapes {
		definition := E{"id": shape.Name, systemNames[18]: shape.Closed}
		if len(shape.Required) > 0 {
			values := make([]any, len(shape.Required))
			for index, attribute := range shape.Required {
				values[index] = RefTo(attribute)
			}
			definition[systemNames[16]] = values
		}
		if len(shape.Allowed) > 0 {
			values := make([]any, len(shape.Allowed))
			for index, attribute := range shape.Allowed {
				values[index] = RefTo(attribute)
			}
			definition[systemNames[17]] = values
		}
		operations = append(operations, definition)
	}
	return operations, nil
}

func (db *DB) ApplySchemaManifest(ctx context.Context, manifest SchemaManifest, options ...TxOption) (TxReport, error) {
	desired, err := db.normalizeSchemaManifest(manifest)
	if err != nil {
		return TxReport{}, err
	}
	attributeWire := make([]any, 0, len(desired.Attributes))
	for _, attribute := range desired.Attributes {
		attributeWire = append(attributeWire, map[string]any{
			"name": attribute.Name, "declared": declaredAttributeWire(attribute.Declared),
		})
	}
	shapeWire := make([]any, 0, len(desired.Shapes))
	for _, shape := range desired.Shapes {
		name, ok := shape.Name.(string)
		if !ok {
			return TxReport{}, fail(ErrSchema, "schema manifest shape name has type %T; use text", shape.Name)
		}
		shapeWire = append(shapeWire, map[string]any{
			"name": name, "required": stringValues(shape.Required),
			"allowed": stringValues(shape.Allowed), "closed": shape.Closed,
		})
	}
	requestHash, err := canonicalLogicalRequestHash(map[string]any{
		"operation": "schema-apply",
		"manifest": map[string]any{
			"fgraph": desired.FGraph, "digest": desired.Digest,
			"attributes": attributeWire, "shapes": shapeWire,
		},
	})
	if err != nil {
		return TxReport{}, err
	}
	prepare := func(ctx context.Context, runner sqlRunner) (any, error) {
		// Full replacement discovery must share the writer transaction with
		// planning and commit so a concurrent declaration cannot survive it.
		current, currentErr := db.schemaManifestOn(ctx, runner)
		if currentErr != nil {
			return nil, currentErr
		}
		return schemaReplacementOperations(current, desired)
	}
	transactionOptions := append([]TxOption{}, options...)
	transactionOptions = append(
		transactionOptions,
		withRequestHashOverride(requestHash),
		func(config *txOptions) { config.prepareData = prepare },
	)
	return db.Transact(ctx, []any{}, transactionOptions...)
}

func stringValues(values []string) []any {
	result := make([]any, len(values))
	for index, value := range values {
		result[index] = value
	}
	return result
}

func declaredAttributeWire(declaration DeclaredAttribute) map[string]any {
	result := map[string]any{}
	if declaration.Type != nil {
		result["type"] = *declaration.Type
	}
	if declaration.Many != nil {
		result["many"] = *declaration.Many
	}
	if declaration.Unique != nil {
		result["unique"] = *declaration.Unique
	}
	if declaration.NoHistory != nil {
		result["nohistory"] = *declaration.NoHistory
	}
	if declaration.Dims != nil {
		result["dims"] = *declaration.Dims
	}
	if declaration.Doc != nil {
		result["doc"] = *declaration.Doc
	}
	if declaration.VectorModel != nil {
		result["vector_model"] = *declaration.VectorModel
	}
	return result
}

func effectiveAttribute(schema attributeSchema, declared DeclaredAttribute) EffectiveAttribute {
	effective := EffectiveAttribute{
		Many: schema.many, Unique: schema.unique, NoHistory: schema.deletesHistory(), Doc: declared.Doc,
	}
	if schema.typeName != "" {
		typeName := schema.typeName
		effective.Type = &typeName
	}
	if schema.dimsSet {
		dims := schema.dims
		effective.Dims = &dims
	}
	if schema.vectorModel != "" {
		model := schema.vectorModel
		effective.VectorModel = &model
	}
	return effective
}

func effectiveAttributeWire(effective EffectiveAttribute) map[string]any {
	return map[string]any{
		"type": pointerValue(effective.Type), "many": effective.Many,
		"unique": effective.Unique, "nohistory": effective.NoHistory,
		"dims": pointerValue(effective.Dims), "doc": pointerValue(effective.Doc),
		"vector_model": pointerValue(effective.VectorModel),
	}
}

func pointerValue[T any](value *T) any {
	if value == nil {
		return nil
	}
	return *value
}

func (db *DB) declaredAttribute(
	ctx context.Context,
	runner sqlRunner,
	id int64,
	name string,
) (declaration DeclaredAttribute, resultErr error) {
	visibility, visibilityArgs := db.visibility("f")
	args := append([]any{id}, visibilityArgs...)
	rows, err := runner.QueryContext(ctx, `SELECT f.a,f.v,f.t FROM fgraph_facts f
		WHERE f.e=? AND (f.a BETWEEN 5 AND 10 OR f.a=14) AND `+visibility+` ORDER BY f.id`, args...)
	if err != nil {
		return DeclaredAttribute{}, wrap(ErrFormat, err, "cannot read declarations for %q", name)
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "declaration rows")) }()
	declaration = DeclaredAttribute{}
	for rows.Next() {
		var attribute int64
		var raw any
		var tag Tag
		if err := rows.Scan(&attribute, &raw, &tag); err != nil {
			return DeclaredAttribute{}, finishRows(rows, wrap(ErrFormat, err, "cannot decode declaration for %q", name), "declaration rows")
		}
		logical, err := db.logicalValue(ctx, runner, raw, tag)
		if err != nil {
			return DeclaredAttribute{}, finishRows(rows, err, "declaration rows")
		}
		if err := applyDeclaredAttribute(&declaration, attribute, logical, name); err != nil {
			return DeclaredAttribute{}, finishRows(rows, err, "declaration rows")
		}
	}
	if err := rows.Err(); err != nil {
		return DeclaredAttribute{}, finishRows(rows, wrap(ErrFormat, err, "cannot finish declarations for %q", name), "declaration rows")
	}
	if err := rows.Close(); err != nil {
		return DeclaredAttribute{}, wrap(ErrFormat, err, "cannot close declaration rows for %q", name)
	}
	return declaration, nil
}

func applyDeclaredAttribute(declaration *DeclaredAttribute, attribute int64, value any, name string) error {
	switch attribute {
	case 5, 6, 7:
		boolean, ok := value.(bool)
		if !ok {
			return fail(ErrFormat, "declaration %d for %q has type %T; repair the database", attribute, name, value)
		}
		switch attribute {
		case 5:
			declaration.Many = &boolean
		case 6:
			declaration.Unique = &boolean
		case 7:
			declaration.NoHistory = &boolean
		}
	case 8, 10, 14:
		text, ok := value.(string)
		if !ok {
			return fail(ErrFormat, "declaration %d for %q has type %T; repair the database", attribute, name, value)
		}
		switch attribute {
		case 8:
			declaration.Type = &text
		case 10:
			declaration.Doc = &text
		case 14:
			declaration.VectorModel = &text
		}
	case 9:
		dimensions, ok := value.(int64)
		if !ok {
			return fail(ErrFormat, "dimensions declaration for %q has type %T; repair the database", name, value)
		}
		declaration.Dims = &dimensions
	}
	return nil
}

func (db *DB) schemaObservation(
	ctx context.Context,
	runner sqlRunner,
	id int64,
	name string,
) (observation AttributeObservation, resultErr error) {
	visibility, visibilityArgs := db.visibility("f")
	args := append([]any{id}, visibilityArgs...)
	rows, err := runner.QueryContext(ctx, `SELECT f.t,COUNT(*)
		FROM fgraph_facts f WHERE f.a=? AND `+visibility+` GROUP BY f.t ORDER BY f.t`, args...)
	if err != nil {
		return AttributeObservation{}, wrap(ErrFormat, err, "cannot inspect observed values for %q", name)
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "observed value rows")) }()
	observation = AttributeObservation{Types: []string{}}
	for rows.Next() {
		var tag Tag
		var facts int64
		if err := rows.Scan(&tag, &facts); err != nil {
			return AttributeObservation{}, finishRows(rows, wrap(ErrFormat, err, "cannot decode observed values for %q", name), "observed value rows")
		}
		if tag < TagRef || tag > TagJSON {
			return AttributeObservation{}, finishRows(rows, fail(ErrFormat, "attribute %q has unknown stored tag %d", name, tag), "observed value rows")
		}
		observation.Types = append(observation.Types, logicalTag(tag))
		observation.LiveFacts += facts
	}
	if err := rows.Err(); err != nil {
		return AttributeObservation{}, finishRows(rows, wrap(ErrFormat, err, "cannot finish observed values for %q", name), "observed value rows")
	}
	if err := rows.Close(); err != nil {
		return AttributeObservation{}, wrap(ErrFormat, err, "cannot close observed value rows for %q", name)
	}
	if err := runner.QueryRowContext(ctx, "SELECT COUNT(DISTINCT f.e) FROM fgraph_facts f WHERE f.a=? AND "+visibility, args...).Scan(&observation.Entities); err != nil {
		return AttributeObservation{}, wrap(ErrFormat, err, "cannot count observed entities for %q", name)
	}
	sort.Strings(observation.Types)
	return observation, nil
}

func (db *DB) schemaIdentities(
	ctx context.Context,
	runner sqlRunner,
	basis int64,
	prefix string,
	includeSystem bool,
) (result []namedIdentity, resultErr error) {
	rows, err := runner.QueryContext(ctx, "SELECT id,name FROM fgraph_ids WHERE name IS NOT NULL AND created_tx<=? ORDER BY name", basis)
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot list schema identities")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "schema identity rows")) }()
	result = []namedIdentity{}
	for rows.Next() {
		var identity namedIdentity
		if err := rows.Scan(&identity.id, &identity.name); err != nil {
			return nil, finishRows(rows, wrap(ErrFormat, err, "cannot decode schema identity"), "schema identity rows")
		}
		if attributePattern.MatchString(identity.name) && strings.HasPrefix(identity.name, prefix) && (includeSystem || !strings.HasPrefix(identity.name, "fgraph/")) {
			result = append(result, identity)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, finishRows(rows, wrap(ErrFormat, err, "cannot finish listing schema identities"), "schema identity rows")
	}
	if err := rows.Close(); err != nil {
		return nil, wrap(ErrFormat, err, "cannot close schema identity rows")
	}
	return result, nil
}

func (db *DB) readShapes(ctx context.Context, runner sqlRunner) (result []ShapeInfo, resultErr error) {
	visibility, args := db.visibility("f")
	rows, err := runner.QueryContext(ctx, "SELECT DISTINCT f.e FROM fgraph_facts f WHERE f.a IN (16,17,18) AND "+visibility+" ORDER BY f.e", args...)
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot list shapes")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "shape identity rows")) }()
	ids := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, finishRows(rows, wrap(ErrFormat, err, "cannot decode shape identity"), "shape identity rows")
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, finishRows(rows, wrap(ErrFormat, err, "cannot finish listing shapes"), "shape identity rows")
	}
	if err := rows.Close(); err != nil {
		return nil, wrap(ErrFormat, err, "cannot close shape identity rows")
	}
	result = make([]ShapeInfo, 0, len(ids))
	for _, id := range ids {
		shape, err := db.readShape(ctx, runner, id)
		if err != nil {
			return nil, err
		}
		result = append(result, shape)
	}
	return result, nil
}

func (db *DB) readShape(ctx context.Context, runner sqlRunner, id int64) (shape ShapeInfo, resultErr error) {
	shape = ShapeInfo{Name: db.displayEntity(id), Required: []string{}, Allowed: []string{}}
	visibility, args := db.visibility("f")
	queryArgs := append([]any{id}, args...)
	rows, err := runner.QueryContext(ctx, "SELECT f.a,f.v,f.t FROM fgraph_facts f WHERE f.e=? AND f.a IN (16,17,18) AND "+visibility+" ORDER BY f.a,f.id", queryArgs...)
	if err != nil {
		return ShapeInfo{}, wrap(ErrFormat, err, "cannot read shape %d", id)
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "shape rows")) }()
	refs := map[int64]string{}
	requiredIDs := []int64{}
	allowedIDs := []int64{}
	for rows.Next() {
		var attr, tag int64
		var value any
		if err := rows.Scan(&attr, &value, &tag); err != nil {
			return ShapeInfo{}, finishRows(rows, wrap(ErrFormat, err, "cannot decode shape %d", id), "shape rows")
		}
		if attr == 18 {
			shape.Closed = sqliteBool(value)
			continue
		}
		if Tag(tag) != TagRef {
			return ShapeInfo{}, finishRows(rows, fail(ErrFormat, "shape %d attribute %d is not a reference", id, attr), "shape rows")
		}
		refs[asInt64(value)] = ""
		if attr == 16 {
			requiredIDs = append(requiredIDs, asInt64(value))
		} else {
			allowedIDs = append(allowedIDs, asInt64(value))
		}
	}
	if err := rows.Err(); err != nil {
		return ShapeInfo{}, finishRows(rows, wrap(ErrFormat, err, "cannot finish reading shape %d", id), "shape rows")
	}
	if err := rows.Close(); err != nil {
		return ShapeInfo{}, wrap(ErrFormat, err, "cannot close shape rows")
	}
	for ref := range refs {
		var name sql.NullString
		if err := runner.QueryRowContext(ctx, "SELECT name FROM fgraph_ids WHERE id=?", ref).Scan(&name); err != nil || !name.Valid {
			return ShapeInfo{}, wrap(ErrFormat, err, "shape %d references unnamed attribute %d", id, ref)
		}
		refs[ref] = name.String
	}
	for _, ref := range requiredIDs {
		shape.Required = append(shape.Required, refs[ref])
	}
	for _, ref := range allowedIDs {
		shape.Allowed = append(shape.Allowed, refs[ref])
	}
	sort.Strings(shape.Required)
	sort.Strings(shape.Allowed)
	return shape, nil
}

// DeclareShape creates or replaces one named shape. The replacement is a
// single transaction so readers never observe a partially updated definition.
func (db *DB) DeclareShape(
	ctx context.Context,
	name string,
	definition ShapeDefinition,
	options ...TxOption,
) (TxReport, error) {
	if err := validateName(name, false); err != nil {
		return TxReport{}, err
	}
	required, err := normalizeShapeAttributes("required", definition.Required)
	if err != nil {
		return TxReport{}, err
	}
	allowed, err := normalizeShapeAttributes("allowed", definition.Allowed)
	if err != nil {
		return TxReport{}, err
	}
	if definition.Closed {
		// Required attributes must be valid under a closed shape even when the
		// caller does not repeat them in Allowed.
		allowed = sortedUniqueStrings(append(allowed, required...))
	}

	shape := E{"id": name, systemNames[18]: definition.Closed}
	if len(required) > 0 {
		values := make([]any, len(required))
		for index, attribute := range required {
			values[index] = RefTo(attribute)
		}
		shape[systemNames[16]] = values
	}
	if len(allowed) > 0 {
		values := make([]any, len(allowed))
		for index, attribute := range allowed {
			values[index] = RefTo(attribute)
		}
		shape[systemNames[17]] = values
	}
	return db.Transact(ctx, []any{
		[]any{"retract", name, systemNames[16]},
		[]any{"retract", name, systemNames[17]},
		[]any{"retract", name, systemNames[18]},
		shape,
	}, options...)
}

func normalizeShapeAttributes(label string, values []string) ([]string, error) {
	for _, attribute := range values {
		if err := validateName(attribute, true); err != nil {
			return nil, wrap(ErrSchema, err, "shape %s contains invalid attribute %q", label, attribute)
		}
	}
	return sortedUniqueStrings(values), nil
}

func sortedUniqueStrings(values []string) []string {
	unique := make(map[string]struct{}, len(values))
	for _, value := range values {
		unique[value] = struct{}{}
	}
	result := make([]string, 0, len(unique))
	for value := range unique {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

// Validate checks shaped entities without changing the database. With no
// selectors it checks every shaped entity; selectors keep validation bounded.
func (db *DB) Validate(ctx context.Context, selectors ...any) (ValidationReport, error) {
	report := ValidationReport{Violations: []ValidationViolation{}, Valid: true}
	readErr := db.withRead(ctx, func(runner sqlRunner) error {
		basis, err := db.basisOn(ctx, runner)
		if err != nil {
			return err
		}
		if db.asOf != nil && *db.asOf < basis {
			basis = *db.asOf
		}
		report.BasisTx = basis
		targets := map[int64]bool{}
		for _, selector := range selectors {
			id, found, resolveErr := db.resolveReadEntity(ctx, runner, selector)
			if resolveErr != nil {
				return resolveErr
			}
			if !found {
				return fail(ErrNotFound, "entity %v does not exist", selector)
			}
			targets[id] = true
		}
		var issuesErr error
		report.Violations, issuesErr = db.shapeIssues(ctx, runner, targets)
		report.Valid = len(report.Violations) == 0
		return issuesErr
	})
	return report, readErr
}

func (db *DB) shapeIssues(ctx context.Context, runner sqlRunner, targets map[int64]bool) (violations []ValidationViolation, resultErr error) {
	visibility, args := db.visibility("f")
	rows, err := runner.QueryContext(ctx, "SELECT f.e,f.v FROM fgraph_facts f WHERE f.a=15 AND f.t=0 AND "+visibility+" ORDER BY f.e,f.id", args...)
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot read shape memberships")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "shape membership rows")) }()
	type membership struct{ entity, shape int64 }
	memberships := []membership{}
	for rows.Next() {
		var item membership
		if err := rows.Scan(&item.entity, &item.shape); err != nil {
			return nil, finishRows(rows, wrap(ErrFormat, err, "cannot decode shape membership"), "shape membership rows")
		}
		if len(targets) == 0 || targets[item.entity] {
			memberships = append(memberships, item)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, finishRows(rows, wrap(ErrFormat, err, "cannot finish reading shape memberships"), "shape membership rows")
	}
	if err := rows.Close(); err != nil {
		return nil, wrap(ErrFormat, err, "cannot close shape membership rows")
	}
	violations = []ValidationViolation{}
	for _, membership := range memberships {
		shape, err := db.readShape(ctx, runner, membership.shape)
		if err != nil {
			return nil, err
		}
		entity, err := db.pullEntity(ctx, runner, membership.entity, 1, map[int64]bool{})
		if err != nil {
			return nil, err
		}
		allowed := map[string]bool{}
		for _, attr := range shape.Allowed {
			allowed[attr] = true
		}
		if shape.Closed {
			for _, required := range shape.Required {
				if !allowed[required] {
					violations = append(violations, ValidationViolation{
						Code: "shape_definition", Entity: db.displayEntity(membership.entity), Shape: shape.Name,
						Attribute: required, Message: "closed shape does not allow one of its required attributes",
					})
				}
			}
		}
		for _, required := range shape.Required {
			if _, found := entity[required]; !found {
				violations = append(violations, ValidationViolation{
					Code: "required", Entity: db.displayEntity(membership.entity), Shape: shape.Name,
					Attribute: required, Message: "required attribute is missing",
				})
			}
		}
		if shape.Closed {
			attributes := make([]string, 0, len(entity))
			for attr := range entity {
				attributes = append(attributes, attr)
			}
			sort.Strings(attributes)
			for _, attr := range attributes {
				if !strings.HasPrefix(attr, "fgraph/") && !allowed[attr] {
					violations = append(violations, ValidationViolation{
						Code: "allowed", Entity: db.displayEntity(membership.entity), Shape: shape.Name,
						Attribute: attr, Message: "attribute is not allowed by the closed shape",
					})
				}
			}
		}
	}
	return violations, nil
}

func (db *DB) validateTouchedShapes(ctx context.Context, runner sqlRunner, plan *transactionPlan) error {
	targets := map[int64]bool{}
	changedShapes := map[int64]bool{}
	for _, assertion := range plan.assertions {
		targets[assertion.e] = true
		if assertion.a >= 16 && assertion.a <= 18 {
			changedShapes[assertion.e] = true
		}
	}
	for _, request := range plan.retracts {
		if request.missing {
			continue
		}
		targets[request.e] = true
		if request.a == nil || (*request.a >= 16 && *request.a <= 18) {
			changedShapes[request.e] = true
		}
	}
	for shape := range changedShapes {
		members, err := changedShapeMembers(ctx, runner, shape)
		if err != nil {
			return err
		}
		for _, entity := range members {
			targets[entity] = true
		}
	}
	violations, err := db.shapeIssues(ctx, runner, targets)
	if err != nil {
		return err
	}
	if len(violations) > 0 {
		violation := violations[0]
		return fail(ErrSchema, "entity %v violates shape %v: %s (%s)", violation.Entity, violation.Shape, violation.Message, violation.Attribute)
	}
	return nil
}

func changedShapeMembers(ctx context.Context, runner sqlRunner, shape int64) (members []int64, resultErr error) {
	rows, err := runner.QueryContext(ctx, "SELECT e FROM fgraph_facts WHERE a=15 AND t=0 AND v=? AND rx IS NULL ORDER BY e", shape)
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot find members of changed shape %d", shape)
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "changed shape member rows")) }()
	for rows.Next() {
		var entity int64
		if err := rows.Scan(&entity); err != nil {
			return nil, wrap(ErrFormat, err, "cannot decode changed shape member")
		}
		members = append(members, entity)
	}
	if err := rows.Err(); err != nil {
		return nil, wrap(ErrFormat, err, "cannot finish changed shape members")
	}
	return members, nil
}
