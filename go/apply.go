package fgraph

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"strings"
)

const (
	maxPortableLineBytes = 8*MaxValueBytes + 64*1024
	// A receipt embeds one maximum-size event and repeats its created selectors
	// so restore can allocate local ids even when event data is redacted later.
	maxPortableSnapshotLineBytes = 2*maxPortableLineBytes + 64*1024
)

// Apply idempotently applies a portable event/1 NDJSON stream. The whole
// stream commits or rolls back as one SQLite write transaction.
func (db *DB) Apply(ctx context.Context, reader io.Reader) (reports []TxReport, resultErr error) {
	resultErr = db.applyStream(ctx, reader, func(report TxReport) {
		reports = append(reports, report)
	})
	if resultErr != nil {
		return nil, resultErr
	}
	return reports, nil
}

func (db *DB) ApplySummary(ctx context.Context, reader io.Reader) (ApplySummary, error) {
	summary := ApplySummary{}
	err := db.applyStream(ctx, reader, func(report TxReport) {
		summary.Events++
		switch report.Status {
		case "applied":
			summary.Applied++
		case "already_applied":
			summary.AlreadyApplied++
		case "noop":
			summary.Noop++
		}
	})
	if err != nil {
		return ApplySummary{}, err
	}
	basis, err := db.latestTx(ctx)
	if err != nil {
		return ApplySummary{}, err
	}
	summary.BasisTx = basis
	return summary, nil
}

func (db *DB) applyStream(ctx context.Context, reader io.Reader, recordReport func(TxReport)) (resultErr error) {
	if reader == nil {
		return fail(ErrType, "apply reader is nil; provide event/1 NDJSON through an io.Reader")
	}
	if err := db.checkUsable(true); err != nil {
		return err
	}
	resultErr = db.atomicPortableWrite(ctx, "apply", func(runner sqlRunner, _ *sql.Conn) error {
		view := &DB{store: db.store, exec: runner}
		buffered := bufio.NewReader(reader)
		for lineNumber := 1; ; lineNumber++ {
			line, readErr := readPortableLine(buffered)
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				if errors.Is(readErr, ErrTooLarge) {
					return readErr
				}
				return wrap(ErrFormat, readErr, "cannot read event stream")
			}
			if len(bytes.TrimSpace(line)) > 0 {
				report, err := view.applyEventLine(ctx, runner, line, lineNumber)
				if err != nil {
					return err
				}
				recordReport(report)
				db.store.dataVersion = -1
				if err := db.store.refreshNames(ctx, runner); err != nil {
					return err
				}
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
		}
		return nil
	})
	return resultErr
}

func readPortableLine(reader *bufio.Reader) ([]byte, error) {
	return readPortableLineLimit(reader, maxPortableLineBytes)
}

func readPortableLineLimit(reader *bufio.Reader, maxBytes int) ([]byte, error) {
	line := make([]byte, 0, min(reader.Size(), maxBytes))
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(fragment) > 0 && fragment[len(fragment)-1] == '\n' {
			fragment = fragment[:len(fragment)-1]
		}
		if len(fragment) > maxBytes-len(line) {
			return nil, fail(ErrTooLarge, "portable NDJSON payload exceeds %d bytes; split or reduce the encoded record", maxBytes)
		}
		line = append(line, fragment...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return line, err
	}
}

func (db *DB) applyEventLine(ctx context.Context, runner sqlRunner, raw []byte, lineNumber int) (TxReport, error) {
	decoded, decodeErr := DecodeJSON(bytes.NewReader(raw))
	if decodeErr != nil {
		return TxReport{}, wrap(ErrType, decodeErr, "event line %d is invalid JSON", lineNumber)
	}
	record, ok := objectMap(decoded)
	if !ok || record["fgraph"] != "event/1" {
		return TxReport{}, fail(ErrType, "event line %d must be an fgraph event/1 object", lineNumber)
	}
	allowed := map[string]bool{
		"fgraph": true, "event": true, "at": true, "created": true,
		"by": true, "source": true, "meta": true, "tx_facts": true,
		"asserted": true, "retracted": true,
	}
	unknown := []string{}
	for name := range record {
		if !allowed[name] {
			unknown = append(unknown, name)
		}
	}
	if len(unknown) > 0 {
		return TxReport{}, fail(ErrType, "event line %d has unknown fields %s", lineNumber, strings.Join(sortedStrings(unknown), ", "))
	}
	eventID, ok := record["event"].(string)
	if !ok {
		return TxReport{}, fail(ErrType, "event line %d has no UUID event id", lineNumber)
	}
	eventUUID, uuidErr := parseUUID(eventID)
	if uuidErr != nil || eventUUID[8]&0xc0 != 0x80 || eventUUID[6]>>4 < 1 || eventUUID[6]>>4 > 5 {
		return TxReport{}, fail(ErrType, "event line %d event id %q is not a canonical RFC UUID version 1 through 5", lineNumber, eventID)
	}
	_, eventHash, canonicalErr := canonicalEventData(record)
	if canonicalErr != nil {
		return TxReport{}, wrap(ErrType, canonicalErr, "event line %d cannot be canonicalized", lineNumber)
	}
	var existingID int64
	existingErr := runner.QueryRowContext(ctx, "SELECT id FROM fgraph_ids WHERE gid=?", eventUUID[:]).Scan(&existingID)
	if existingErr == nil {
		var stored []byte
		storedErr := runner.QueryRowContext(ctx, "SELECT event_hash FROM fgraph_events WHERE tx=?", existingID).Scan(&stored)
		if storedErr != nil || !bytes.Equal(stored, eventHash[:]) {
			return TxReport{}, fail(ErrConflict, "event %s collides with another identity or payload", eventID)
		}
		report, receiptErr := db.eventReceipt(ctx, runner, existingID)
		if receiptErr != nil {
			return TxReport{}, receiptErr
		}
		return alreadyAppliedReport(report), nil
	}
	if !errors.Is(existingErr, sql.ErrNoRows) {
		return TxReport{}, wrap(ErrFormat, existingErr, "cannot inspect event %s", eventID)
	}
	at, ok := record["at"].(int64)
	if !ok {
		return TxReport{}, fail(ErrType, "event line %d needs integer at", lineNumber)
	}
	if instantErr := validateInstantMicros(at); instantErr != nil {
		return TxReport{}, wrap(ErrType, instantErr, "event line %d has invalid at", lineNumber)
	}
	created, createdOK := record["created"].([]any)
	asserted, assertedOK := record["asserted"].([]any)
	retracted, retractedOK := record["retracted"].([]any)
	if !createdOK || !assertedOK || !retractedOK {
		return TxReport{}, fail(ErrType, "event line %d needs created/asserted/retracted arrays", lineNumber)
	}
	operations := make([]any, 0, len(asserted)+len(retracted))
	appendFacts := func(kind string, tuples []any) error {
		for _, rawTuple := range tuples {
			tuple, ok := rawTuple.([]any)
			if !ok || len(tuple) != 4 {
				return fail(ErrType, "event line %d %s tuple must be [selector,attribute,value,tag]", lineNumber, kind)
			}
			attribute, attrOK := tuple[1].(string)
			tag, tagOK := tuple[3].(string)
			if !attrOK || !tagOK {
				return fail(ErrType, "event line %d %s tuple needs text attribute and tag", lineNumber, kind)
			}
			entity, err := db.applySelector(ctx, runner, tuple[0])
			if err != nil {
				return wrap(ErrorKind(err), err, "event line %d has invalid %s selector", lineNumber, kind)
			}
			value, err := db.applyWireValue(ctx, runner, tuple[2], tag)
			if err != nil {
				return wrap(ErrorKind(err), err, "event line %d has invalid %s value", lineNumber, kind)
			}
			operations = append(operations, []any{kind, entity, attribute, value})
		}
		return nil
	}
	// Retractions precede assertions so cardinality-one replacements preserve
	// the source event's net transition while still using the normal validator.
	if err := appendFacts("retract", retracted); err != nil {
		return TxReport{}, err
	}
	if err := appendFacts("assert", asserted); err != nil {
		return TxReport{}, err
	}
	txFacts := []any{}
	if raw, exists := record["tx_facts"]; exists {
		tuples, ok := raw.([]any)
		if !ok {
			return TxReport{}, fail(ErrType, "event line %d tx_facts must be an array", lineNumber)
		}
		for _, rawTuple := range tuples {
			tuple, ok := rawTuple.([]any)
			if !ok || len(tuple) != 3 {
				return TxReport{}, fail(ErrType, "event line %d tx fact must be [attribute,value,tag]", lineNumber)
			}
			attribute, attrOK := tuple[0].(string)
			tag, tagOK := tuple[2].(string)
			if !attrOK || !tagOK {
				return TxReport{}, fail(ErrType, "event line %d tx fact needs text attribute and tag", lineNumber)
			}
			value, err := db.applyWireValue(ctx, runner, tuple[1], tag)
			if err != nil {
				return TxReport{}, wrap(ErrorKind(err), err, "event line %d has invalid tx fact value", lineNumber)
			}
			txFacts = append(txFacts, []any{attribute, value, tag})
		}
	}
	txFacts = append(txFacts, []any{systemNames[importedAtAttrID], Instant(at), "instant"})
	config := txOptions{
		force: true, eventID: &eventID, eventHash: &eventHash,
		originAt: &at, preallocated: created, txFacts: txFacts, txFactsSet: true,
	}
	if raw, exists := record["by"]; exists {
		value, ok := raw.(string)
		if !ok {
			return TxReport{}, fail(ErrType, "event line %d by must be text", lineNumber)
		}
		config.by = &value
	}
	if raw, exists := record["source"]; exists {
		value, ok := raw.(string)
		if !ok {
			return TxReport{}, fail(ErrType, "event line %d source must be text", lineNumber)
		}
		config.source = &value
	}
	if meta, exists := record["meta"]; exists {
		config.meta, config.metaSet = meta, true
	}
	report, err := db.transactOn(ctx, runner, operations, config)
	if err != nil {
		return TxReport{}, wrap(ErrorKind(err), err, "event line %d event %s failed", lineNumber, eventID)
	}
	return report, nil
}

func sortedStrings(values []string) []string {
	result := append([]string(nil), values...)
	for left := 0; left < len(result); left++ {
		for right := left + 1; right < len(result); right++ {
			if result[right] < result[left] {
				result[left], result[right] = result[right], result[left]
			}
		}
	}
	return result
}

func (db *DB) applySelector(ctx context.Context, runner sqlRunner, value any) (any, error) {
	if name, ok := value.(string); ok {
		if err := validateName(name, false); err != nil {
			return nil, err
		}
		return name, nil
	}
	eid, uuid, err := selectorUUID(value)
	if err != nil {
		return nil, err
	}
	var id int64
	if queryErr := runner.QueryRowContext(ctx, "SELECT id FROM fgraph_ids WHERE gid=?", uuid[:]).Scan(&id); queryErr == nil {
		return id, nil
	} else if !errors.Is(queryErr, sql.ErrNoRows) {
		return nil, wrap(ErrFormat, queryErr, "cannot resolve event identity %s", eid)
	}
	return Tmp("event:" + eid), nil
}

func (db *DB) applyWireValue(ctx context.Context, runner sqlRunner, value any, tag string) (any, error) {
	if tag != "ref" {
		return taggedInput(value, tag)
	}
	fields, ok := objectFields(value)
	if !ok || len(fields) != 1 || fields[0].Name != "ref" {
		return nil, fail(ErrType, "event ref value must use {\"ref\":selector}")
	}
	target, err := db.applySelector(ctx, runner, fields[0].Value)
	if err != nil {
		return nil, err
	}
	return RefTo(target), nil
}

func selectorEID(value any) (string, error) {
	eid, _, err := selectorUUID(value)
	return eid, err
}

func selectorUUID(value any) (string, [16]byte, error) {
	fields, ok := objectFields(value)
	if !ok || len(fields) != 1 || fields[0].Name != "eid" {
		return "", [16]byte{}, fail(ErrType, "identity selector must be a name or {\"eid\":canonical-uuid}")
	}
	eid, ok := fields[0].Value.(string)
	if !ok {
		return "", [16]byte{}, fail(ErrType, "identity eid selector must contain a UUID string")
	}
	uuid, err := parseUUID(eid)
	if err != nil {
		return "", [16]byte{}, err
	}
	return eid, uuid, nil
}

func (db *DB) preallocateEventIdentities(ctx context.Context, runner sqlRunner, plan *transactionPlan, selectors []any) error {
	for _, selector := range selectors {
		if name, ok := selector.(string); ok {
			if _, _, err := plan.allocator.name(ctx, name, false, true); err != nil {
				return err
			}
			continue
		}
		eid, uuid, err := selectorUUID(selector)
		if err != nil {
			return err
		}
		var existing int64
		if queryErr := runner.QueryRowContext(ctx, "SELECT id FROM fgraph_ids WHERE gid=?", uuid[:]).Scan(&existing); queryErr == nil {
			continue
		} else if !errors.Is(queryErr, sql.ErrNoRows) {
			return wrap(ErrFormat, queryErr, "cannot inspect preallocated identity %s", eid)
		}
		token := "event:" + eid
		if _, duplicate := plan.tempids[token]; duplicate {
			return fail(ErrConflict, "event repeats created identity %s", eid)
		}
		id, err := plan.allocator.anonymous()
		if err != nil {
			return err
		}
		plan.tempids[token] = id
		// Retain even an identity with no facts, and let finalize install the
		// source global identity instead of deriving a replacement UUIDv5.
		plan.allocator.ids[token] = id
		plan.allocator.gids[id] = eid
	}
	return nil
}

func (db *DB) atomicPortableWrite(
	ctx context.Context,
	name string,
	operation func(sqlRunner, *sql.Conn) error,
) (resultErr error) {
	if db.exec != nil {
		savepoint := "fgraph_" + name
		if _, err := db.exec.ExecContext(ctx, "SAVEPOINT "+savepoint); err != nil {
			return wrap(ErrFormat, err, "cannot begin atomic %s savepoint", name)
		}
		if err := operation(db.exec, nil); err != nil {
			_, rollbackErr := db.exec.ExecContext(context.Background(), "ROLLBACK TO "+savepoint)
			_, releaseErr := db.exec.ExecContext(context.Background(), "RELEASE "+savepoint)
			db.store.dataVersion = -1
			refreshErr := db.store.refreshNames(context.Background(), db.exec)
			var cleanupErr error
			if rollbackErr != nil {
				cleanupErr = wrap(ErrFormat, rollbackErr, "cannot roll back %s", name)
			}
			if releaseErr != nil {
				cleanupErr = joinErrors(cleanupErr, wrap(ErrFormat, releaseErr, "cannot release rolled-back %s", name))
			}
			return joinErrors(err, joinErrors(cleanupErr, refreshErr))
		}
		if _, err := db.exec.ExecContext(ctx, "RELEASE "+savepoint); err != nil {
			return wrap(ErrFormat, err, "cannot release atomic %s savepoint", name)
		}
		db.store.dataVersion = -1
		return nil
	}
	db.store.mu.Lock()
	defer db.store.mu.Unlock()
	conn, err := db.store.sql.Conn(ctx)
	if err != nil {
		return wrap(ErrFormat, err, "cannot acquire SQLite writer for %s", name)
	}
	committed := false
	started := false
	defer func() {
		if started && !committed {
			resultErr = joinErrors(resultErr, rollbackSQLite(conn, name))
		}
		resultErr = joinErrors(resultErr, wrapClose(conn.Close(), name+" database connection"))
	}()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return wrap(ErrConflict, err, "cannot acquire the single-writer lock for %s", name)
	}
	started = true
	runner := newPreparedRunner(conn)
	if err := operation(runner, conn); err != nil {
		closeErr := wrapClose(runner.Close(), name+" prepared statements")
		db.store.dataVersion = -1
		return joinErrors(err, closeErr)
	}
	if err := runner.Close(); err != nil {
		db.store.dataVersion = -1
		return wrap(ErrFormat, err, "cannot close prepared %s statements", name)
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return wrap(ErrFormat, err, "cannot commit %s", name)
	}
	committed = true
	started = false
	db.store.dataVersion = -1
	return nil
}

func decodeLowerHex(value any, bytes int, description string) ([]byte, error) {
	text, ok := value.(string)
	if !ok || len(text) != bytes*2 || text != strings.ToLower(text) {
		return nil, fail(ErrType, "%s must be %d-byte lowercase hex", description, bytes)
	}
	decoded, err := hex.DecodeString(text)
	if err != nil {
		return nil, fail(ErrType, "%s must be %d-byte lowercase hex", description, bytes)
	}
	return decoded, nil
}
