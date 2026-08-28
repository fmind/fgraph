package fgraph

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"strings"
)

// EventRecords returns portable event/1 records after since and through an
// optional inclusive local transaction boundary. Historical views clamp the
// boundary to their pinned basis.
func (db *DB) EventRecords(ctx context.Context, since int64, through ...int64) ([]map[string]any, error) {
	if since < GenesisTx {
		return nil, fail(ErrType, "event cursor %d is invalid; use a transaction id at least %d", since, GenesisTx)
	}
	if len(through) > 1 {
		return nil, fail(ErrType, "event records accept at most one through transaction")
	}
	end := int64(0)
	if len(through) == 1 {
		end = through[0]
		if end < GenesisTx {
			return nil, fail(ErrType, "event through %d is invalid; use a transaction id at least %d", end, GenesisTx)
		}
	} else {
		basis, err := db.latestTx(ctx)
		if err != nil {
			return nil, err
		}
		end = basis
	}
	if db.asOf != nil && end > *db.asOf {
		end = *db.asOf
	}
	transactions := []int64{}
	err := db.withRead(ctx, func(runner sqlRunner) (resultErr error) {
		rows, err := runner.QueryContext(ctx, "SELECT tx FROM fgraph_events WHERE tx>? AND tx<=? ORDER BY tx", since, end)
		if err != nil {
			return wrap(ErrFormat, err, "cannot read event records after %d through %d", since, end)
		}
		defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "event record transaction rows")) }()
		for rows.Next() {
			var tx int64
			if err := rows.Scan(&tx); err != nil {
				return wrap(ErrFormat, err, "cannot decode an event transaction after %d", since)
			}
			transactions = append(transactions, tx)
		}
		if err := rows.Err(); err != nil {
			return wrap(ErrFormat, err, "cannot finish event records after %d", since)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	records := make([]map[string]any, 0, len(transactions))
	for _, tx := range transactions {
		record, err := db.eventRecordForTx(ctx, tx)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

// Tail writes portable event/1 NDJSON records after since.
func (db *DB) Tail(ctx context.Context, writer io.Writer, since int64) error {
	if writer == nil {
		return fail(ErrType, "tail writer is nil; provide an io.Writer")
	}
	if since < GenesisTx {
		return fail(ErrType, "event cursor %d is invalid; use a transaction id at least %d", since, GenesisTx)
	}
	return db.withRead(ctx, func(runner sqlRunner) (resultErr error) {
		basis, err := db.basisOn(ctx, runner)
		if err != nil {
			return err
		}
		if db.asOf != nil && *db.asOf < basis {
			basis = *db.asOf
		}
		rows, err := runner.QueryContext(ctx, "SELECT tx FROM fgraph_events WHERE tx>? AND tx<=? ORDER BY tx", since, basis)
		if err != nil {
			return wrap(ErrFormat, err, "cannot read event records after %d through %d", since, basis)
		}
		defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "tail transaction rows")) }()
		for rows.Next() {
			var tx int64
			if err := rows.Scan(&tx); err != nil {
				return wrap(ErrFormat, err, "cannot decode an event transaction after %d", since)
			}
			record, err := db.eventRecordForTxOn(ctx, runner, tx)
			if err != nil {
				return err
			}
			line, err := canonicalJSON(record)
			if err != nil {
				return err
			}
			if err := writeFull(writer, append(line, '\n')); err != nil {
				return wrap(ErrFormat, err, "cannot write event/1 record")
			}
		}
		if err := rows.Err(); err != nil {
			return wrap(ErrFormat, err, "cannot finish event records after %d", since)
		}
		return nil
	})
}

func (db *DB) eventRecordForTx(ctx context.Context, tx int64) (map[string]any, error) {
	if db.asOf != nil && tx > *db.asOf {
		return nil, fail(ErrNotFound, "transaction %d is beyond historical horizon %d", tx, *db.asOf)
	}
	var record map[string]any
	err := db.withRead(ctx, func(runner sqlRunner) error {
		var err error
		record, err = db.eventRecordForTxOn(ctx, runner, tx)
		return err
	})
	return record, err
}

func (db *DB) eventRecordForTxOn(ctx context.Context, runner sqlRunner, tx int64) (map[string]any, error) {
	var at int64
	if err := runner.QueryRowContext(ctx, "SELECT v FROM fgraph_facts WHERE e=? AND a=1 AND tx=e AND rx IS NULL", tx).Scan(&at); err != nil {
		return nil, wrap(ErrNotFound, err, "transaction %d does not exist; use a committed transaction id", tx)
	}
	record, err := db.exportTransaction(ctx, runner, tx, at)
	if err != nil {
		return nil, err
	}
	return db.validateEventRecord(ctx, runner, tx, record)
}

func (db *DB) validateEventRecord(ctx context.Context, runner sqlRunner, tx int64, record map[string]any) (map[string]any, error) {
	var stored []byte
	var eventData sql.NullString
	if err := runner.QueryRowContext(ctx, "SELECT event_hash,event_data FROM fgraph_events WHERE tx=?", tx).Scan(&stored, &eventData); err != nil {
		return nil, wrap(ErrFormat, err, "transaction %d has no durable event receipt", tx)
	}
	if len(stored) != 32 {
		return nil, fail(ErrFormat, "transaction %d event hash is malformed", tx)
	}
	if eventData.Valid {
		decoded, err := decodeStoredEventData(eventData.String, stored)
		if err != nil {
			return nil, wrap(ErrFormat, err, "transaction %d has invalid canonical event data", tx)
		}
		if decoded["event"] != record["event"] || decoded["at"] != record["at"] {
			return nil, fail(ErrFormat, "transaction %d event data does not match its durable identity or timestamp", tx)
		}
		return decoded, nil
	}
	return map[string]any{
		"fgraph": "event/1", "event": record["event"], "at": record["at"],
		"redacted": true, "event_hash": hex.EncodeToString(stored),
	}, nil
}

func decodeStoredEventData(data string, storedHash []byte) (map[string]any, error) {
	if len(data) > maxPortableLineBytes {
		return nil, fail(ErrTooLarge, "stored canonical event is %d bytes; maximum is %d", len(data), maxPortableLineBytes)
	}
	decoded, err := DecodeJSON(strings.NewReader(data))
	if err != nil {
		return nil, err
	}
	record, ok := objectMap(decoded)
	if !ok || record["fgraph"] != "event/1" {
		return nil, fail(ErrFormat, "stored event data is not an event/1 object")
	}
	canonical, err := canonicalJSON(record)
	if err != nil {
		return nil, err
	}
	if data != string(canonical) {
		return nil, fail(ErrFormat, "stored event data is not canonical JSON")
	}
	digest := sha256.Sum256(canonical)
	if len(storedHash) != sha256.Size || !bytes.Equal(digest[:], storedHash) {
		return nil, fail(ErrFormat, "stored event data does not match its SHA-256 receipt")
	}
	return record, nil
}

func (db *DB) exportTransaction(ctx context.Context, runner sqlRunner, tx, at int64) (result map[string]any, resultErr error) {
	var gid []byte
	if err := runner.QueryRowContext(ctx, "SELECT gid FROM fgraph_ids WHERE id=?", tx).Scan(&gid); err != nil || len(gid) != 16 {
		return nil, wrap(ErrFormat, err, "transaction %d has no event UUID", tx)
	}
	var eventUUID [16]byte
	copy(eventUUID[:], gid)
	line := map[string]any{
		"fgraph": "event/1", "event": formatUUID(eventUUID), "at": at,
		"created": []any{}, "asserted": []any{}, "retracted": []any{},
	}
	createdRows, createdQueryErr := runner.QueryContext(ctx, "SELECT name,gid FROM fgraph_ids WHERE created_tx=? AND id!=? ORDER BY id", tx, tx)
	if createdQueryErr != nil {
		return nil, wrap(ErrFormat, createdQueryErr, "cannot read identities created by event %d", tx)
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(createdRows.Close(), "created identity rows")) }()
	created := []any{}
	for createdRows.Next() {
		var name sql.NullString
		var identity []byte
		if scanErr := createdRows.Scan(&name, &identity); scanErr != nil {
			return nil, joinErrors(
				wrap(ErrFormat, scanErr, "cannot decode event %d created identity", tx),
				wrapClose(createdRows.Close(), "created identity rows"),
			)
		}
		if name.Valid {
			created = append(created, name.String)
		} else if len(identity) == 16 {
			var uuid [16]byte
			copy(uuid[:], identity)
			created = append(created, map[string]any{"eid": formatUUID(uuid)})
		}
	}
	if rowsErr := createdRows.Err(); rowsErr != nil {
		return nil, joinErrors(
			wrap(ErrFormat, rowsErr, "cannot finish event %d created identities", tx),
			wrapClose(createdRows.Close(), "created identity rows"),
		)
	}
	if closeErr := createdRows.Close(); closeErr != nil {
		return nil, wrap(ErrFormat, closeErr, "cannot close event %d created identities", tx)
	}
	line["created"] = created
	rows, queryErr := runner.QueryContext(ctx, "SELECT id,e,a,v,t,tx,rx FROM fgraph_facts WHERE tx=? ORDER BY id", tx)
	if queryErr != nil {
		return nil, wrap(ErrFormat, queryErr, "cannot read assertions for event transaction %d", tx)
	}
	asserted, scanErr := scanRawFacts(rows)
	if scanErr != nil {
		return nil, scanErr
	}
	assertedTuples := []any{}
	txFacts := []any{}
	for _, fact := range asserted {
		logical, logicalErr := db.logicalValue(ctx, runner, fact.v, fact.t)
		if logicalErr != nil {
			return nil, logicalErr
		}
		if fact.e == tx {
			switch fact.a {
			case 1:
				continue
			case 2:
				line["by"] = logical
				continue
			case 3:
				line["source"] = logical
				continue
			case 4:
				line["meta"] = logical
				continue
			case importedAtAttrID:
				// Imported-at is local receipt metadata. Portable event identity
				// remains anchored to the source clock and never exposes the local
				// rebasing detail as an event/1 field.
				line["at"] = logical
				continue
			default:
				attr, attrErr := db.attributeName(ctx, runner, fact.a)
				if attrErr != nil {
					return nil, attrErr
				}
				value, valueErr := db.eventValue(ctx, runner, logical, fact.t)
				if valueErr != nil {
					return nil, valueErr
				}
				txFacts = append(txFacts, []any{attr, value, logicalTag(fact.t)})
				continue
			}
		}
		tuple, tupleErr := db.exportTuple(ctx, runner, fact, logical)
		if tupleErr != nil {
			return nil, tupleErr
		}
		assertedTuples = append(assertedTuples, tuple)
	}
	rows, queryErr = runner.QueryContext(ctx, "SELECT id,e,a,v,t,tx,rx FROM fgraph_facts WHERE rx=? ORDER BY id", tx)
	if queryErr != nil {
		return nil, wrap(ErrFormat, queryErr, "cannot read retractions for event transaction %d", tx)
	}
	retracted, err := scanRawFacts(rows)
	if err != nil {
		return nil, err
	}
	retractedTuples := []any{}
	for _, fact := range retracted {
		logical, err := db.logicalValue(ctx, runner, fact.v, fact.t)
		if err != nil {
			return nil, err
		}
		tuple, err := db.exportTuple(ctx, runner, fact, logical)
		if err != nil {
			return nil, err
		}
		retractedTuples = append(retractedTuples, tuple)
	}
	line["asserted"], line["retracted"] = assertedTuples, retractedTuples
	if len(txFacts) > 0 {
		line["tx_facts"] = txFacts
	}
	return line, nil
}

func (db *DB) exportTuple(ctx context.Context, runner sqlRunner, fact rawFact, logical any) ([]any, error) {
	attr, err := db.attributeName(ctx, runner, fact.a)
	if err != nil {
		return nil, err
	}
	entity, err := db.identitySelector(ctx, runner, fact.e)
	if err != nil {
		return nil, err
	}
	value, err := db.eventValue(ctx, runner, logical, fact.t)
	if err != nil {
		return nil, err
	}
	return []any{entity, attr, value, logicalTag(fact.t)}, nil
}

func (db *DB) eventValue(ctx context.Context, runner sqlRunner, logical any, tag Tag) (any, error) {
	if tag != TagRef {
		return db.renderLogical(logical, tag), nil
	}
	target, err := db.identitySelector(ctx, runner, asInt64(logical))
	if err != nil {
		return nil, err
	}
	return map[string]any{"ref": target}, nil
}

func (db *DB) identitySelector(ctx context.Context, runner sqlRunner, id int64) (any, error) {
	var name sql.NullString
	var gid []byte
	if err := runner.QueryRowContext(ctx, "SELECT name,gid FROM fgraph_ids WHERE id=?", id).Scan(&name, &gid); err != nil {
		return nil, wrap(ErrFormat, err, "identity %d is missing from the format-v2 registry", id)
	}
	if name.Valid {
		return name.String, nil
	}
	if len(gid) != 16 {
		return nil, fail(ErrFormat, "identity %d has no canonical UUID", id)
	}
	var uuid [16]byte
	copy(uuid[:], gid)
	return map[string]any{"eid": formatUUID(uuid)}, nil
}

func (db *DB) attributeName(ctx context.Context, runner sqlRunner, id int64) (string, error) {
	var attr string
	if err := runner.QueryRowContext(ctx, "SELECT name FROM fgraph_ids WHERE id=?", id).Scan(&attr); err != nil {
		return "", wrap(ErrFormat, err, "attribute id %d has no name", id)
	}
	return attr, nil
}

func logicalTag(tag Tag) string {
	switch tag {
	case TagTextRef:
		return "text"
	case TagBytesRef:
		return "bytes"
	default:
		if tag >= TagRef && tag <= TagJSON {
			return tagNames[tag]
		}
		return "unknown"
	}
}

func ErrorKind(err error) error {
	for _, kind := range []error{ErrNotFound, ErrConflict, ErrSchema, ErrType, ErrQuery, ErrFormat, ErrReadOnly, ErrTooLarge, ErrUnsupported} {
		if ErrorName(err) == kind.Error() {
			return kind
		}
	}
	return ErrFormat
}

func taggedInput(value any, tag string) (any, error) {
	switch typed := value.(type) {
	case RefValue:
		if tag == "ref" {
			return typed, nil
		}
	case InstantValue:
		if tag == "instant" {
			return typed, nil
		}
	case BytesValue:
		if tag == "bytes" {
			return typed, nil
		}
	case VectorValue:
		if tag == "vector" {
			return typed, nil
		}
	case JSONValue:
		if tag == "json" {
			return typed, nil
		}
	}
	logical := value
	if fields, ok := objectFields(value); ok && len(fields) == 1 {
		wrapper := fields[0].Name
		switch wrapper {
		case "ref", "instant", "bytes", "vector", "json":
			if wrapper != tag {
				return nil, fail(ErrType, "portable %s value uses %q wrapper; use a matching logical tag", tag, wrapper)
			}
			logical = fields[0].Value
		}
	}
	switch tag {
	case "ref":
		return RefTo(logical), nil
	case "bool":
		if value, ok := logical.(bool); ok {
			return value, nil
		}
	case "int":
		if value, ok := logical.(int64); ok {
			return value, nil
		}
	case "float":
		switch value := logical.(type) {
		case float64:
			return value, nil
		case int64:
			return float64(value), nil
		}
	case "text":
		if value, ok := logical.(string); ok {
			return value, nil
		}
	case "instant":
		switch value := logical.(type) {
		case string:
			return Object{Fields: []Field{{Name: "instant", Value: value}}}, nil
		case int64:
			return Instant(value), nil
		}
	case "bytes":
		if value, ok := logical.(string); ok {
			return Object{Fields: []Field{{Name: "bytes", Value: value}}}, nil
		}
	case "vector":
		if value, ok := logical.([]any); ok {
			return Object{Fields: []Field{{Name: "vector", Value: value}}}, nil
		}
	case "json":
		return JSON(logical), nil
	default:
		return nil, fail(ErrType, "portable logical tag %q is invalid; use ref, bool, int, float, text, instant, bytes, vector, or json", tag)
	}
	return nil, fail(ErrType, "portable %s value has type %T; use matching logical tags", tag, logical)
}
