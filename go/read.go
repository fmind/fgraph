package fgraph

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode/utf8"
)

func (db *DB) withRead(ctx context.Context, fn func(sqlRunner) error) (resultErr error) {
	if err := db.checkUsable(false); err != nil {
		return err
	}
	if db.exec != nil {
		return fn(db.exec)
	}
	db.store.mu.Lock()
	defer db.store.mu.Unlock()
	if err := db.checkUsable(false); err != nil {
		return err
	}
	conn, err := db.store.sql.Conn(ctx)
	if err != nil {
		return wrap(ErrFormat, err, "cannot acquire SQLite reader connection")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(conn.Close(), "SQLite reader connection")) }()
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return wrap(ErrFormat, err, "cannot begin coherent SQLite read snapshot")
	}
	defer func() {
		_, rollbackErr := conn.ExecContext(context.Background(), "ROLLBACK")
		if rollbackErr != nil {
			resultErr = joinErrors(resultErr, wrap(ErrFormat, rollbackErr, "cannot close coherent SQLite read snapshot"))
		}
	}()
	runner := newPreparedRunner(conn)
	defer func() {
		resultErr = joinErrors(resultErr, wrapClose(runner.Close(), "prepared read statements"))
	}()
	if err := db.store.refreshNames(ctx, runner); err != nil {
		return err
	}
	return fn(runner)
}

func (db *DB) displayEntity(id int64) any {
	if name, found := db.store.idNames[id]; found {
		return name
	}
	// Small fault-injection stores in tests may only populate the forward map.
	if db.store.idNames == nil {
		for name, candidate := range db.store.names {
			if candidate == id {
				return name
			}
		}
	}
	// Stable UUID selectors belong to portable event/snapshot records. Regular
	// database reads deliberately expose unnamed identities as file-local ids.
	return id
}

func (db *DB) displayEntityAllocated(id int64, alloc *allocator) any {
	if alloc != nil {
		for name, candidate := range alloc.ids {
			if candidate == id {
				return name
			}
		}
	}
	return db.displayEntity(id)
}

func (db *DB) renderLogical(value any, tag Tag) any {
	return db.renderLogicalAllocated(value, tag, nil)
}

func (db *DB) renderLogicalAllocated(value any, tag Tag, alloc *allocator) any {
	switch tag {
	case TagRef:
		return map[string]any{"ref": db.displayEntityAllocated(asInt64(value), alloc)}
	case TagInstant:
		return map[string]any{"instant": formatInstant(asInt64(value))}
	case TagBytes, TagBytesRef:
		data := value.([]byte) //nolint:errcheck // logicalValue validates physical bytes before rendering.
		return map[string]any{"bytes": base64.StdEncoding.EncodeToString(data)}
	case TagVector:
		// Promote stored float32 values before JSON encoding so every runtime
		// exposes the same exact binary32 value rather than Go's shorter float32 text.
		return map[string]any{"vector": float32JSON(value.([]float32))} //nolint:errcheck // logicalValue validates the stored vector.
	case TagJSON:
		return map[string]any{"json": value}
	default:
		return value
	}
}

func (db *DB) logicalValue(ctx context.Context, runner sqlRunner, raw any, tag Tag) (any, error) {
	switch tag {
	case TagBool:
		value, ok := raw.(int64)
		if !ok || (value != 0 && value != 1) {
			return nil, fail(ErrFormat, "bool fact has physical value %v; run doctor and restore a valid backup", raw)
		}
		return value != 0, nil
	case TagInt:
		value, ok := raw.(int64)
		if !ok {
			return nil, fail(ErrFormat, "int fact has SQLite type %T; run doctor and restore a valid backup", raw)
		}
		return value, nil
	case TagInstant:
		value, ok := raw.(int64)
		if !ok || value < minInstantMicros || value > maxInstantMicros {
			return nil, fail(ErrFormat, "instant fact has physical value %v; run doctor and restore a valid backup", raw)
		}
		return value, nil
	case TagRef:
		value, ok := raw.(int64)
		if !ok || value <= 0 {
			return nil, fail(ErrFormat, "ref fact has physical value %v; run doctor and restore a valid backup", raw)
		}
		return value, nil
	case TagFloat:
		value, ok := raw.(float64)
		if !ok || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fail(ErrFormat, "float fact has physical value %v; run doctor and restore a valid backup", raw)
		}
		return value, nil
	case TagText:
		value, ok := raw.(string)
		if !ok || !utf8.ValidString(value) || len(value) > BlobThreshold {
			return nil, fail(ErrFormat, "text fact has physical value %v; run doctor and restore a valid backup", raw)
		}
		return value, nil
	case TagBytes:
		data, ok := raw.([]byte)
		if !ok || len(data) > BlobThreshold {
			return nil, fail(ErrFormat, "bytes fact has SQLite type %T; restore a valid fgraph file", raw)
		}
		return append([]byte(nil), data...), nil
	case TagTextRef, TagBytesRef, TagVector:
		hash, ok := raw.([]byte)
		if !ok || len(hash) != sha256.Size {
			return nil, fail(ErrFormat, "indirect fact has SQLite type %T; restore a valid fgraph file", raw)
		}
		var data any
		if err := runner.QueryRowContext(ctx, "SELECT data FROM fgraph_blobs WHERE hash=?", hash).Scan(&data); err != nil {
			return nil, wrap(ErrFormat, err, "blob %x is missing; run doctor and restore from backup", hash)
		}
		return db.logicalIndirectValue(raw, tag, data)
	case TagJSON:
		text, ok := raw.(string)
		if !ok || len(text) > MaxValueBytes || !utf8.ValidString(text) {
			return nil, fail(ErrFormat, "JSON fact has SQLite type %T; restore a valid backup", raw)
		}
		value, err := decodeInternalJSON(strings.NewReader(text))
		if err != nil {
			return nil, wrap(ErrFormat, err, "JSON fact is not valid canonical JSON")
		}
		canonical, err := canonicalJSON(value)
		if err != nil || !bytes.Equal(canonical, []byte(text)) {
			return nil, wrap(ErrFormat, err, "JSON fact is not canonical JSON; run doctor and restore a valid backup")
		}
		plain, plainErr := plainJSON(value)
		if plainErr != nil {
			return nil, wrap(ErrFormat, plainErr, "JSON fact exceeds the supported nesting depth")
		}
		return plain, nil
	}
	return nil, fail(ErrFormat, "fact has unknown tag %d; migrate or restore the file", tag)
}

func (db *DB) logicalIndirectValue(raw any, tag Tag, data any) (any, error) {
	hash, ok := raw.([]byte)
	if !ok || len(hash) != sha256.Size {
		return nil, fail(ErrFormat, "indirect fact has SQLite type %T; restore a valid fgraph file", raw)
	}
	if !validIndirectBlob(tag, hash, data) {
		return nil, fail(ErrFormat, "blob %x does not match its content-addressed hash or physical domain; run doctor and restore a valid backup", hash)
	}
	switch tag {
	case TagTextRef:
		return data.(string), nil //nolint:errcheck // validIndirectBlob proves the physical domain.
	case TagBytesRef:
		return append([]byte(nil), data.([]byte)...), nil //nolint:errcheck // validIndirectBlob proves the physical domain.
	case TagVector:
		value := data.([]byte) //nolint:errcheck // validIndirectBlob proves the physical domain.
		vector := make([]float32, len(value)/4)
		for index := range vector {
			vector[index] = math.Float32frombits(binary.LittleEndian.Uint32(value[index*4:]))
		}
		return vector, nil
	default:
		return nil, fail(ErrFormat, "fact has non-indirect tag %d for blob %x; restore a valid database", tag, hash)
	}
}

func (db *DB) renderRaw(ctx context.Context, runner sqlRunner, raw rawFact, overrideRx *int64) (Fact, error) {
	logical, err := db.logicalValue(ctx, runner, raw.v, raw.t)
	if err != nil {
		return Fact{}, err
	}
	return db.renderRawLogical(ctx, runner, raw, logical, overrideRx)
}

func (db *DB) renderRawLogical(ctx context.Context, runner sqlRunner, raw rawFact, logical any, overrideRx *int64) (Fact, error) {
	var attr string
	if err := runner.QueryRowContext(ctx, "SELECT name FROM fgraph_ids WHERE id=?", raw.a).Scan(&attr); err != nil {
		return Fact{}, wrap(ErrFormat, err, "attribute id %d has no name; restore a valid fgraph file", raw.a)
	}
	rx := overrideRx
	if rx == nil && raw.rx.Valid {
		value := raw.rx.Int64
		rx = &value
	}
	return Fact{
		ID: raw.id, E: db.displayEntity(raw.e), A: attr,
		V: db.renderLogical(logical, raw.t), Tag: raw.t, Tx: raw.tx, Rx: rx,
	}, nil
}

func (db *DB) renderViewRaw(ctx context.Context, runner sqlRunner, raw rawFact) (Fact, error) {
	// A historical view must not reveal that a fact will be retracted later.
	if db.asOf != nil && raw.rx.Valid && raw.rx.Int64 > *db.asOf {
		raw.rx = sql.NullInt64{}
	}
	return db.renderRaw(ctx, runner, raw, nil)
}

func (db *DB) visibility(alias string) (string, []any) {
	if db.asOf == nil {
		return alias + ".rx IS NULL", nil
	}
	return alias + ".tx <= ? AND (" + alias + ".rx IS NULL OR " + alias + ".rx > ?)", []any{*db.asOf, *db.asOf}
}

func (db *DB) resolveReadEntity(ctx context.Context, runner sqlRunner, ref any) (int64, bool, error) {
	switch ref := ref.(type) {
	case string:
		var id int64
		query := "SELECT id FROM fgraph_ids WHERE name=?"
		args := []any{ref}
		if db.asOf != nil {
			query += " AND created_tx<=?"
			args = append(args, *db.asOf)
		}
		if err := runner.QueryRowContext(ctx, query, args...).Scan(&id); err == nil {
			return id, true, nil
		} else if err != sql.ErrNoRows {
			return 0, false, wrap(ErrFormat, err, "cannot resolve entity name %q", ref)
		}
		return 0, false, nil
	case int:
		return db.resolveNumericEntity(ctx, runner, int64(ref))
	case int64:
		return db.resolveNumericEntity(ctx, runner, ref)
	case float64:
		if integer, ok := exactInt64Float(ref); ok && integer >= 1 {
			return db.resolveNumericEntity(ctx, runner, integer)
		}
		return 0, false, fail(ErrType, "entity id %v is invalid; use a positive integer", ref)
	case []any:
		alloc := &allocator{runner: runner, store: db.store, ids: map[string]int64{}}
		return db.resolveLookup(ctx, runner, &transactionPlan{allocator: alloc, tempids: map[string]int64{}}, ref, false)
	default:
		if fields, ok := objectFields(ref); ok && len(fields) == 1 && fields[0].Name == "eid" {
			text, ok := fields[0].Value.(string)
			if !ok {
				return 0, false, fail(ErrType, "eid selector must contain a canonical UUID string")
			}
			gid, err := parseUUID(text)
			if err != nil {
				return 0, false, err
			}
			query := "SELECT id FROM fgraph_ids WHERE gid=?"
			args := []any{gid[:]}
			if db.asOf != nil {
				query += " AND created_tx<=?"
				args = append(args, *db.asOf)
			}
			var id int64
			if err := runner.QueryRowContext(ctx, query, args...).Scan(&id); err == nil {
				return id, true, nil
			} else if err != sql.ErrNoRows {
				return 0, false, wrap(ErrFormat, err, "cannot resolve eid %q", text)
			}
			return 0, false, nil
		}
		return 0, false, fail(ErrType, "entity reference has type %T; use a name, positive int64, or unique lookup", ref)
	}
}

func (db *DB) resolveNumericEntity(ctx context.Context, runner sqlRunner, id int64) (int64, bool, error) {
	if id < 1 {
		return 0, false, nil
	}
	query := "SELECT EXISTS(SELECT 1 FROM fgraph_ids WHERE id=?"
	args := []any{id}
	if db.asOf != nil {
		query += " AND created_tx<=?"
		args = append(args, *db.asOf)
	}
	query += ")"
	var exists int
	if err := runner.QueryRowContext(ctx, query, args...).Scan(&exists); err != nil {
		return 0, false, wrap(ErrFormat, err, "cannot resolve numeric entity %d", id)
	}
	return id, exists != 0, nil
}

func (db *DB) Entity(ctx context.Context, ref any, depth ...int) (map[string]any, error) {
	level := 1
	if len(depth) > 0 {
		level = depth[0]
	}
	if level < 0 {
		return nil, fail(ErrType, "entity depth %d is invalid; use zero or a positive integer", level)
	}
	var result map[string]any
	readErr := db.withRead(ctx, func(runner sqlRunner) error {
		id, found, err := db.resolveReadEntity(ctx, runner, ref)
		if err != nil {
			return err
		}
		if !found {
			return fail(ErrNotFound, "entity %v does not exist; use a known name or id", ref)
		}
		result, err = db.pullEntity(ctx, runner, id, level, map[int64]bool{})
		return err
	})
	return result, readErr
}

func (db *DB) pullEntity(
	ctx context.Context,
	runner sqlRunner,
	id int64,
	depth int,
	seen map[int64]bool,
	spendWork ...func() error,
) (result map[string]any, resultErr error) {
	visibility, args := db.visibility("f")
	queryArgs := append([]any{id}, args...)
	rows, err := runner.QueryContext(ctx, `SELECT f.id,f.e,f.a,f.v,f.t,f.tx,f.rx,i.name
		FROM fgraph_facts f JOIN fgraph_ids i ON i.id=f.a
		WHERE f.e=? AND `+visibility+` ORDER BY f.a,f.id`, queryArgs...)
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot read entity %d", id)
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "entity fact rows")) }()
	result = map[string]any{}
	seen[id] = true
	defer delete(seen, id)
	for rows.Next() {
		if len(spendWork) > 0 && spendWork[0] != nil {
			if err := spendWork[0](); err != nil {
				return nil, err
			}
		}
		var raw rawFact
		var attr string
		if err := rows.Scan(&raw.id, &raw.e, &raw.a, &raw.v, &raw.t, &raw.tx, &raw.rx, &attr); err != nil {
			return nil, wrap(ErrFormat, err, "cannot decode entity %d", id)
		}
		logical, err := db.logicalValue(ctx, runner, raw.v, raw.t)
		if err != nil {
			return nil, err
		}
		var rendered any
		if raw.t == TagRef && depth > 1 && !seen[asInt64(logical)] {
			rendered, err = db.pullEntity(ctx, runner, asInt64(logical), depth-1, seen, spendWork...)
			if err != nil {
				return nil, err
			}
		} else {
			rendered = db.renderLogical(logical, raw.t)
		}
		schema, err := db.schemaFor(ctx, runner, raw.a, nil)
		if err != nil {
			return nil, err
		}
		if schema.many {
			current, exists := result[attr]
			if !exists {
				result[attr] = []any{rendered}
			} else {
				values, ok := current.([]any)
				if !ok {
					return nil, fail(ErrFormat, "many attribute %q has non-array rendered state; restore a valid database", attr)
				}
				result[attr] = append(values, rendered)
			}
		} else {
			result[attr] = rendered
		}
	}
	if err := rows.Err(); err != nil {
		return nil, wrap(ErrFormat, err, "cannot finish reading entity %d", id)
	}
	if len(result) == 0 {
		var exists int
		query := "SELECT EXISTS(SELECT 1 FROM fgraph_ids WHERE id=?"
		args := []any{id}
		if db.asOf != nil {
			query += " AND created_tx<=?"
			args = append(args, *db.asOf)
		}
		query += ")"
		if err := runner.QueryRowContext(ctx, query, args...).Scan(&exists); err != nil {
			return nil, wrap(ErrFormat, err, "cannot check entity %d", id)
		}
		if exists == 0 {
			return nil, nil
		}
	}
	return result, nil
}

func (db *DB) RawFacts(ctx context.Context, includeGenesis bool) ([][]any, error) {
	result := [][]any{}
	err := db.withRead(ctx, func(runner sqlRunner) (resultErr error) {
		query := "SELECT id,e,a,v,t,tx,rx FROM fgraph_facts"
		if !includeGenesis {
			query += fmt.Sprintf(" WHERE id>%d", GenesisFactCount)
		}
		query += " ORDER BY id"
		rows, err := runner.QueryContext(ctx, query)
		if err != nil {
			return wrap(ErrFormat, err, "cannot read physical fact rows")
		}
		defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "physical fact rows")) }()
		for rows.Next() {
			var id, e, a, tag, tx int64
			var value any
			var rx sql.NullInt64
			if err := rows.Scan(&id, &e, &a, &value, &tag, &tx, &rx); err != nil {
				return wrap(ErrFormat, err, "cannot decode physical fact row")
			}
			var rxValue any
			if rx.Valid {
				rxValue = rx.Int64
			}
			result = append(result, []any{id, e, a, value, tag, tx, rxValue})
		}
		if err := rows.Err(); err != nil {
			return wrap(ErrFormat, err, "cannot finish reading physical fact rows")
		}
		return nil
	})
	return result, err
}

func (db *DB) Stats(ctx context.Context) (Stats, error) {
	result := Stats{ApplicationID: ApplicationID, FormatVersion: FormatVersion}
	readErr := db.withRead(ctx, func(runner sqlRunner) error {
		basis, basisErr := db.basisOn(ctx, runner)
		if basisErr != nil {
			return basisErr
		}
		if db.asOf != nil && *db.asOf < basis {
			basis = *db.asOf
		}
		visibility, visibilityArgs := db.visibility("f")
		queries := []struct {
			dest  *int64
			query string
			args  []any
		}{
			{query: "SELECT COUNT(*) FROM fgraph_ids identity WHERE created_tx<=? AND NOT EXISTS (SELECT 1 FROM fgraph_events event WHERE event.tx=identity.id)", dest: &result.Entities, args: []any{basis}},
			{query: "SELECT COUNT(*) FROM fgraph_facts WHERE tx<=?", dest: &result.Facts, args: []any{basis}},
			{query: "SELECT COUNT(*) FROM fgraph_facts f WHERE " + visibility, dest: &result.LiveFacts, args: visibilityArgs},
			{query: "SELECT COUNT(*) FROM fgraph_events WHERE tx<=?", dest: &result.Transactions, args: []any{basis}},
			{query: "SELECT COUNT(DISTINCT v) FROM fgraph_facts WHERE t IN (7,8,9) AND tx<=?", dest: &result.Blobs, args: []any{basis}},
		}
		for _, query := range queries {
			if err := runner.QueryRowContext(ctx, query.query, query.args...).Scan(query.dest); err != nil {
				return wrap(ErrFormat, err, "cannot compute database statistics")
			}
		}
		rows, err := runner.QueryContext(ctx, "SELECT name FROM fgraph_ids WHERE name IS NOT NULL AND created_tx<=?", basis)
		if err != nil {
			return wrap(ErrFormat, err, "cannot count attribute names")
		}
		result.Attributes, err = countAttributeRows(rows)
		if err != nil {
			return err
		}
		result.Size = db.store.fileSize()
		return nil
	})
	return result, readErr
}

// Attributes returns the effective schema and observed logical types for
// application attributes. System attributes are opt-in so discovery remains
// useful for normal application modeling.
func (db *DB) Attributes(ctx context.Context, prefix string, includeSystem bool) ([]AttributeInfo, error) {
	result := []AttributeInfo{}
	err := db.withRead(ctx, func(runner sqlRunner) (resultErr error) {
		basis, err := db.basisOn(ctx, runner)
		if err != nil {
			return err
		}
		if db.asOf != nil && *db.asOf < basis {
			basis = *db.asOf
		}
		rows, err := runner.QueryContext(ctx, "SELECT id,name FROM fgraph_ids WHERE name IS NOT NULL AND created_tx<=? ORDER BY name", basis)
		if err != nil {
			return wrap(ErrFormat, err, "cannot list attribute identities")
		}
		defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "attribute identity rows")) }()
		for rows.Next() {
			var id int64
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				return wrap(ErrFormat, err, "cannot decode attribute identity")
			}
			if !attributePattern.MatchString(name) || (!includeSystem && strings.HasPrefix(name, "fgraph/")) || !strings.HasPrefix(name, prefix) {
				continue
			}
			info, err := db.attributeInfo(ctx, runner, id, name)
			if err != nil {
				return err
			}
			result = append(result, info)
		}
		if err := rows.Err(); err != nil {
			return wrap(ErrFormat, err, "cannot finish listing attribute identities")
		}
		return nil
	})
	return result, err
}

func (db *DB) attributeInfo(ctx context.Context, runner sqlRunner, id int64, name string) (AttributeInfo, error) {
	schema, schemaErr := db.schemaFor(ctx, runner, id, nil)
	if schemaErr != nil {
		return AttributeInfo{}, schemaErr
	}
	info := AttributeInfo{
		Name: name, Types: []string{}, Many: schema.many, Unique: schema.unique,
		NoHistory: schema.deletesHistory(),
	}
	if schema.dimsSet {
		dims := schema.dims
		info.Dims = &dims
	}
	if schema.vectorModel != "" {
		model := schema.vectorModel
		info.VectorModel = &model
	}
	types := map[string]struct{}{}
	if schema.typeName != "" {
		types[schema.typeName] = struct{}{}
	}
	observedTypes, facts, observedErr := db.observedAttributeTypes(ctx, runner, id, name)
	if observedErr != nil {
		return AttributeInfo{}, observedErr
	}
	info.Facts = facts
	for typeName := range observedTypes {
		types[typeName] = struct{}{}
	}
	for typeName := range types {
		info.Types = append(info.Types, typeName)
	}
	sort.Strings(info.Types)
	doc, found, docErr := db.attributeDoc(ctx, runner, id)
	if docErr != nil {
		return AttributeInfo{}, docErr
	}
	if found {
		info.Doc = &doc
	}
	return info, nil
}

func (db *DB) observedAttributeTypes(
	ctx context.Context,
	runner sqlRunner,
	id int64,
	name string,
) (types map[string]struct{}, facts int64, resultErr error) {
	types = map[string]struct{}{}
	visibility, visibilityArgs := db.visibility("f")
	args := append([]any{id}, visibilityArgs...)
	rows, queryErr := runner.QueryContext(ctx, "SELECT f.t,COUNT(*) FROM fgraph_facts f WHERE f.a=? AND "+visibility+" GROUP BY f.t", args...)
	if queryErr != nil {
		return nil, 0, wrap(ErrFormat, queryErr, "cannot inspect observed types for %q", name)
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "observed attribute type rows")) }()
	for rows.Next() {
		var tag int64
		var count int64
		if scanErr := rows.Scan(&tag, &count); scanErr != nil {
			return nil, 0, wrap(ErrFormat, scanErr, "cannot decode observed type for %q", name)
		}
		if tag < int64(TagRef) || tag > int64(TagJSON) {
			return nil, 0, fail(ErrFormat, "attribute %q has unknown stored tag %d", name, tag)
		}
		types[logicalTag(Tag(tag))] = struct{}{}
		facts += count
	}
	if iterationErr := rows.Err(); iterationErr != nil {
		return nil, 0, wrap(ErrFormat, iterationErr, "cannot finish reading observed types for %q", name)
	}
	return types, facts, nil
}

func (db *DB) attributeDoc(ctx context.Context, runner sqlRunner, id int64) (string, bool, error) {
	visibility, visibilityArgs := db.visibility("f")
	args := append([]any{id}, visibilityArgs...)
	var raw any
	var tag int64
	err := runner.QueryRowContext(ctx, "SELECT f.v,f.t FROM fgraph_facts f WHERE f.e=? AND f.a=10 AND "+visibility+" ORDER BY f.id DESC LIMIT 1", args...).Scan(&raw, &tag)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, wrap(ErrFormat, err, "cannot read attribute documentation for id %d", id)
	}
	logical, err := db.logicalValue(ctx, runner, raw, Tag(tag))
	if err != nil {
		return "", false, err
	}
	doc, ok := logical.(string)
	if !ok {
		return "", false, fail(ErrFormat, "attribute documentation for id %d has logical type %T", id, logical)
	}
	return doc, true, nil
}

func countAttributeRows(rows *sql.Rows) (count int64, resultErr error) {
	defer func() {
		resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "attribute name rows"))
	}()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return 0, wrap(ErrFormat, err, "cannot decode attribute name")
		}
		if attributePattern.MatchString(name) {
			count++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, wrap(ErrFormat, err, "cannot finish counting attribute names")
	}
	return count, nil
}
