package fgraph

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"strings"
)

// Snapshot writes a portable retained-state snapshot/1 stream. Unlike Backup,
// this format is canonical NDJSON and can be restored by every runtime.
func (db *DB) Snapshot(ctx context.Context, writer io.Writer) error {
	if writer == nil {
		return fail(ErrType, "snapshot writer is nil; provide an io.Writer")
	}
	return db.withRead(ctx, func(runner sqlRunner) error {
		basis, basisErr := db.basisOn(ctx, runner)
		if basisErr != nil {
			return basisErr
		}
		if db.asOf != nil && *db.asOf < basis {
			basis = *db.asOf
		}
		var createdAt int64
		if createdAtErr := runner.QueryRowContext(ctx, "SELECT value FROM fgraph_meta WHERE key='created_at'").Scan(&createdAt); createdAtErr != nil {
			return wrap(ErrFormat, createdAtErr, "snapshot cannot read fgraph_meta.created_at")
		}
		basisEvent, eventErr := db.eventIDForTx(ctx, runner, basis)
		if eventErr != nil {
			return eventErr
		}
		digest := sha256.New()
		writeBody := func(record any) error {
			encoded, encodeErr := canonicalJSON(record)
			if encodeErr != nil {
				return encodeErr
			}
			if err := writeFull(writer, append(encoded, '\n')); err != nil {
				return wrap(ErrFormat, err, "cannot write portable snapshot")
			}
			_, _ = digest.Write(encoded)
			_, _ = digest.Write([]byte{'\n'})
			return nil
		}
		if writeErr := writeBody(map[string]any{
			"fgraph": "snapshot/1", "format": int64(FormatVersion),
			"created_at": createdAt, "basis": basisEvent,
		}); writeErr != nil {
			return writeErr
		}
		receiptCount, receiptErr := db.snapshotReceipts(ctx, runner, basis, writeBody)
		if receiptErr != nil {
			return receiptErr
		}
		factCount, factErr := db.snapshotFacts(ctx, runner, basis, writeBody)
		if factErr != nil {
			return factErr
		}
		footer, footerErr := canonicalJSON(map[string]any{
			"fgraph": "end", "sha256": hex.EncodeToString(digest.Sum(nil)),
			"receipts": receiptCount, "facts": factCount,
		})
		if footerErr != nil {
			return footerErr
		}
		if err := writeFull(writer, append(footer, '\n')); err != nil {
			return wrap(ErrFormat, err, "cannot write portable snapshot footer")
		}
		return nil
	})
}

func writeFull(writer io.Writer, value []byte) error {
	written, err := writer.Write(value)
	if err == nil && written != len(value) {
		return io.ErrShortWrite
	}
	return err
}

func (db *DB) eventIDForTx(ctx context.Context, runner sqlRunner, tx int64) (string, error) {
	var gid []byte
	if err := runner.QueryRowContext(ctx, "SELECT gid FROM fgraph_ids WHERE id=?", tx).Scan(&gid); err != nil || len(gid) != 16 {
		return "", wrap(ErrFormat, err, "transaction %d is missing its event identity", tx)
	}
	var uuid [16]byte
	copy(uuid[:], gid)
	return formatUUID(uuid), nil
}

func (db *DB) snapshotReceipts(
	ctx context.Context,
	runner sqlRunner,
	basis int64,
	write func(any) error,
) (count int64, resultErr error) {
	rows, err := runner.QueryContext(ctx, `SELECT ev.tx,ev.event_hash,ev.event_data,ev.operation_id,ev.request_hash,i.gid
		FROM fgraph_events ev JOIN fgraph_ids i ON i.id=ev.tx
		WHERE ev.tx>? AND ev.tx<=? ORDER BY ev.tx`, GenesisTx, basis)
	if err != nil {
		return 0, wrap(ErrFormat, err, "cannot enumerate snapshot receipts")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "snapshot receipt rows")) }()
	for rows.Next() {
		var tx int64
		var eventHash, requestHash, gid []byte
		var eventData, operationID sql.NullString
		if err := rows.Scan(&tx, &eventHash, &eventData, &operationID, &requestHash, &gid); err != nil {
			return 0, wrap(ErrFormat, err, "cannot decode snapshot receipt")
		}
		if len(eventHash) != sha256.Size || len(gid) != 16 || (requestHash != nil && len(requestHash) != sha256.Size) {
			return 0, fail(ErrFormat, "transaction %d has a malformed durable receipt", tx)
		}
		var eventUUID [16]byte
		copy(eventUUID[:], gid)
		var at int64
		if err := runner.QueryRowContext(ctx, "SELECT v FROM fgraph_facts WHERE e=? AND a=1 AND tx=?", tx, tx).Scan(&at); err != nil {
			return 0, wrap(ErrFormat, err, "cannot read snapshot receipt %d timestamp", tx)
		}
		originAt := at
		var imported int64
		if err := runner.QueryRowContext(ctx, "SELECT v FROM fgraph_facts WHERE e=? AND a=? AND tx=?", tx, importedAtAttrID, tx).Scan(&imported); err == nil {
			originAt = imported
		} else if err != sql.ErrNoRows {
			return 0, wrap(ErrFormat, err, "cannot read snapshot receipt %d origin timestamp", tx)
		}
		created, err := db.createdSelectors(ctx, runner, tx)
		if err != nil {
			return 0, err
		}
		var operation any
		var request any
		var portableData any
		if eventData.Valid {
			portableData, err = decodeStoredEventData(eventData.String, eventHash)
			if err != nil {
				return 0, wrap(ErrFormat, err, "cannot snapshot transaction %d event data", tx)
			}
		}
		if operationID.Valid {
			operation = operationID.String
			request = hex.EncodeToString(requestHash)
		}
		receipt := map[string]any{
			"event": formatUUID(eventUUID), "at": at, "origin_at": originAt,
			"event_hash": hex.EncodeToString(eventHash), "event_data": portableData, "operation_id": operation,
			"request_hash": request, "created": created,
		}
		if err := write(map[string]any{"receipt": receipt}); err != nil {
			return 0, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, wrap(ErrFormat, err, "cannot finish snapshot receipts")
	}
	return count, nil
}

func (db *DB) createdSelectors(ctx context.Context, runner sqlRunner, tx int64) (created []any, resultErr error) {
	rows, err := runner.QueryContext(ctx, "SELECT id FROM fgraph_ids WHERE created_tx=? AND id<>? ORDER BY id", tx, tx)
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot enumerate identities created by transaction %d", tx)
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "snapshot created identity rows")) }()
	created = []any{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, wrap(ErrFormat, err, "cannot decode created identity for transaction %d", tx)
		}
		selector, err := db.identitySelector(ctx, runner, id)
		if err != nil {
			return nil, err
		}
		created = append(created, selector)
	}
	if err := rows.Err(); err != nil {
		return nil, wrap(ErrFormat, err, "cannot finish created identities for transaction %d", tx)
	}
	return created, nil
}

func (db *DB) snapshotFacts(
	ctx context.Context,
	runner sqlRunner,
	basis int64,
	write func(any) error,
) (count int64, resultErr error) {
	rows, err := runner.QueryContext(ctx, `SELECT id,e,a,v,t,tx,rx FROM fgraph_facts
		WHERE tx>? AND tx<=? ORDER BY id`, GenesisTx, basis)
	if err != nil {
		return 0, wrap(ErrFormat, err, "cannot enumerate snapshot facts")
	}
	facts, err := scanRawFacts(rows)
	if err != nil {
		return 0, err
	}
	for _, fact := range facts {
		logical, err := db.logicalValue(ctx, runner, fact.v, fact.t)
		if err != nil {
			return 0, err
		}
		tuple, err := db.exportTuple(ctx, runner, fact, logical)
		if err != nil {
			return 0, err
		}
		assertEvent, err := db.eventIDForTx(ctx, runner, fact.tx)
		if err != nil {
			return 0, err
		}
		var retractEvent any
		if fact.rx.Valid && fact.rx.Int64 <= basis {
			retractEvent, err = db.eventIDForTx(ctx, runner, fact.rx.Int64)
			if err != nil {
				return 0, err
			}
		}
		tuple = append(tuple, assertEvent, retractEvent)
		if err := write(map[string]any{"fact": tuple}); err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

type snapshotRestoreState struct {
	identities map[string]int64
	events     map[string]int64
	receipts   map[int64]snapshotReceipt
	lastEvent  string
	next       int64
	receiptN   int64
	factN      int64
	phaseFacts bool
}

type snapshotReceipt struct {
	event    string
	at       int64
	originAt int64
}

// Restore atomically installs a portable retained-state snapshot into a
// pristine database. Apply is the merge primitive for non-pristine stores.
func (db *DB) Restore(ctx context.Context, reader io.Reader) error {
	if reader == nil {
		return fail(ErrType, "restore reader is nil; provide snapshot/1 NDJSON through an io.Reader")
	}
	if err := db.checkUsable(true); err != nil {
		return err
	}
	return db.atomicPortableWrite(ctx, "restore", func(runner sqlRunner, conn *sql.Conn) error {
		if conn == nil {
			return fail(ErrUnsupported, "restore is unavailable inside a speculative transaction")
		}
		basis, err := db.basisOn(ctx, runner)
		if err != nil {
			return err
		}
		if basis != GenesisTx {
			return fail(ErrConflict, "restore requires a pristine database; use apply for an ordered event stream")
		}
		return db.restoreSnapshotStream(ctx, runner, conn, reader)
	})
}

func (db *DB) restoreSnapshotStream(ctx context.Context, runner sqlRunner, conn *sql.Conn, reader io.Reader) error {
	state, stateErr := db.newSnapshotRestoreState(ctx, runner)
	if stateErr != nil {
		return stateErr
	}
	digest := sha256.New()
	buffered := bufio.NewReader(reader)
	lineNumber := 0
	headerSeen, footerSeen := false, false
	var headerBasis string
	for {
		raw, readErr := readPortableLine(buffered)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return wrap(ErrFormat, readErr, "cannot read snapshot stream")
		}
		if len(bytes.TrimSpace(raw)) > 0 {
			lineNumber++
			if footerSeen {
				return fail(ErrType, "snapshot line %d appears after the footer", lineNumber)
			}
			decoded, decodeErr := DecodeJSON(bytes.NewReader(raw))
			if decodeErr != nil {
				return wrap(ErrType, decodeErr, "snapshot line %d is invalid JSON", lineNumber)
			}
			record, ok := objectMap(decoded)
			if !ok {
				return fail(ErrType, "snapshot line %d must be an object", lineNumber)
			}
			if record["fgraph"] == "end" {
				if !headerSeen {
					return fail(ErrType, "snapshot footer appeared before its header")
				}
				if err := validateSnapshotFooter(record, digest, state); err != nil {
					return err
				}
				footerSeen = true
			} else {
				canonical, canonicalErr := canonicalJSON(record)
				if canonicalErr != nil {
					return canonicalErr
				}
				_, _ = digest.Write(canonical)
				_, _ = digest.Write([]byte{'\n'})
				if !headerSeen {
					var headerErr error
					headerBasis, headerErr = db.restoreSnapshotHeader(ctx, runner, record)
					if headerErr != nil {
						return headerErr
					}
					headerSeen = true
				} else if _, receipt := record["receipt"]; receipt {
					if state.phaseFacts {
						return fail(ErrType, "snapshot receipts must precede fact records")
					}
					if err := db.restoreSnapshotReceipt(ctx, runner, state, record); err != nil {
						return err
					}
				} else if _, fact := record["fact"]; fact {
					state.phaseFacts = true
					if err := db.restoreSnapshotFact(ctx, runner, state, record); err != nil {
						return err
					}
				} else {
					return fail(ErrType, "snapshot line %d is neither a receipt nor a fact", lineNumber)
				}
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
	}
	if !headerSeen || !footerSeen {
		return fail(ErrType, "snapshot is truncated; header and footer are required")
	}
	expectedBasis := genesisEventID
	if state.receiptN > 0 {
		expectedBasis = state.lastEvent
	}
	if headerBasis != expectedBasis {
		return fail(ErrConflict, "snapshot header basis does not match its final transaction receipt")
	}
	if _, updateErr := runner.ExecContext(ctx, "UPDATE fgraph_meta SET value=? WHERE key='next_id'", state.next); updateErr != nil {
		return wrap(ErrFormat, updateErr, "cannot finalize restored identity allocator")
	}
	db.store.dataVersion = -1
	if refreshErr := db.store.refreshNames(ctx, runner); refreshErr != nil {
		return refreshErr
	}
	if receiptTimeErr := db.validateRestoredReceiptTimes(ctx, runner, state); receiptTimeErr != nil {
		return receiptTimeErr
	}
	report, fatal, err := db.doctorReport(ctx, conn)
	if err != nil {
		return err
	}
	if len(fatal) > 0 || !report.OK {
		return fail(ErrFormat, "restored snapshot violates format invariants: %s", strings.Join(report.Problems, "; "))
	}
	return nil
}

func (db *DB) newSnapshotRestoreState(ctx context.Context, runner sqlRunner) (state *snapshotRestoreState, resultErr error) {
	state = &snapshotRestoreState{
		identities: map[string]int64{}, events: map[string]int64{genesisEventID: GenesisTx},
		receipts: map[int64]snapshotReceipt{}, next: FirstUserID,
	}
	rows, err := runner.QueryContext(ctx, "SELECT id,name,gid FROM fgraph_ids ORDER BY id")
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot initialize snapshot identity registry")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "snapshot identity registry rows")) }()
	for rows.Next() {
		var id int64
		var name sql.NullString
		var gid []byte
		if err := rows.Scan(&id, &name, &gid); err != nil {
			return nil, wrap(ErrFormat, err, "cannot decode snapshot identity registry")
		}
		var selector any
		if name.Valid {
			selector = name.String
		} else {
			var uuid [16]byte
			copy(uuid[:], gid)
			selector = map[string]any{"eid": formatUUID(uuid)}
		}
		key, err := snapshotSelectorKey(selector)
		if err != nil {
			return nil, err
		}
		state.identities[key] = id
	}
	if err := rows.Err(); err != nil {
		return nil, wrap(ErrFormat, err, "cannot finish snapshot identity registry")
	}
	return state, nil
}

func snapshotSelectorKey(selector any) (string, error) {
	if name, ok := selector.(string); ok {
		if err := validateName(name, false); err != nil {
			return "", err
		}
		return "name:" + name, nil
	}
	eid, err := selectorEID(selector)
	if err != nil {
		return "", err
	}
	return "eid:" + eid, nil
}

func exactKeys(record map[string]any, names ...string) bool {
	if len(record) != len(names) {
		return false
	}
	for _, name := range names {
		if _, exists := record[name]; !exists {
			return false
		}
	}
	return true
}

func (db *DB) restoreSnapshotHeader(ctx context.Context, runner sqlRunner, record map[string]any) (string, error) {
	if !exactKeys(record, "fgraph", "format", "created_at", "basis") || record["fgraph"] != "snapshot/1" || record["format"] != int64(FormatVersion) {
		return "", fail(ErrType, "snapshot header is invalid or targets another format version")
	}
	createdAt, ok := record["created_at"].(int64)
	if !ok {
		return "", fail(ErrType, "snapshot created_at must be integer microseconds")
	}
	if err := validateInstantMicros(createdAt); err != nil {
		return "", err
	}
	basis, ok := record["basis"].(string)
	if !ok {
		return "", fail(ErrType, "snapshot basis must be an event UUID")
	}
	if _, err := parseUUID(basis); err != nil {
		return "", err
	}
	genesisData, genesisHash, err := genesisEventData(createdAt)
	if err != nil {
		return "", err
	}
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{"UPDATE fgraph_meta SET value=? WHERE key='created_at'", []any{createdAt}},
		{"UPDATE fgraph_facts SET v=? WHERE e=? AND a=1 AND tx=?", []any{createdAt, GenesisTx, GenesisTx}},
		{"UPDATE fgraph_events SET event_hash=?,event_data=? WHERE tx=?", []any{genesisHash[:], genesisData, GenesisTx}},
	} {
		if _, err := runner.ExecContext(ctx, statement.query, statement.args...); err != nil {
			return "", wrap(ErrFormat, err, "cannot restore snapshot genesis metadata")
		}
	}
	return basis, nil
}

func (db *DB) restoreSnapshotReceipt(ctx context.Context, runner sqlRunner, state *snapshotRestoreState, wrapper map[string]any) error {
	if !exactKeys(wrapper, "receipt") {
		return fail(ErrType, "snapshot receipt wrapper must contain exactly receipt")
	}
	receipt, receiptOK := objectMap(wrapper["receipt"])
	if !receiptOK || !exactKeys(receipt, "event", "at", "origin_at", "event_hash", "event_data", "operation_id", "request_hash", "created") {
		return fail(ErrType, "snapshot receipt is malformed")
	}
	eventID, eventOK := receipt["event"].(string)
	if !eventOK {
		return fail(ErrType, "snapshot receipt event must be a UUID")
	}
	eventUUID, uuidErr := parseUUID(eventID)
	if uuidErr != nil {
		return uuidErr
	}
	if _, duplicate := state.events[eventID]; duplicate {
		return fail(ErrConflict, "snapshot repeats event %s", eventID)
	}
	at, atOK := receipt["at"].(int64)
	originAt, originOK := receipt["origin_at"].(int64)
	if !atOK || !originOK {
		return fail(ErrType, "snapshot receipt at and origin_at must be integer microseconds")
	}
	if atErr := validateInstantMicros(at); atErr != nil {
		return atErr
	}
	if originErr := validateInstantMicros(originAt); originErr != nil {
		return originErr
	}
	eventHash, hashErr := decodeLowerHex(receipt["event_hash"], sha256.Size, "snapshot event_hash")
	if hashErr != nil {
		return hashErr
	}
	var eventData any
	var eventRecord map[string]any
	if receipt["event_data"] != nil {
		record, recordOK := objectMap(receipt["event_data"])
		if !recordOK {
			return fail(ErrType, "snapshot event_data must be an event object or null")
		}
		canonical, digest, canonicalErr := canonicalEventData(record)
		if canonicalErr != nil {
			return canonicalErr
		}
		if !bytes.Equal(digest[:], eventHash) {
			return fail(ErrConflict, "snapshot event %s data does not match event_hash", eventID)
		}
		eventRecord = record
		eventData = canonical
	}
	var operationID any
	var requestHash any
	if receipt["operation_id"] != nil || receipt["request_hash"] != nil {
		operation, operationOK := receipt["operation_id"].(string)
		if !operationOK {
			return fail(ErrType, "snapshot operation receipt is malformed")
		}
		if err := validateOperationID(operation); err != nil {
			return err
		}
		request, requestErr := decodeLowerHex(receipt["request_hash"], sha256.Size, "snapshot request_hash")
		if requestErr != nil {
			return requestErr
		}
		operationID, requestHash = operation, request
	}
	created, createdOK := receipt["created"].([]any)
	if !createdOK {
		return fail(ErrType, "snapshot receipt created must be an array")
	}
	if eventRecord != nil {
		if eventRecord["event"] != eventID || eventRecord["at"] != originAt {
			return fail(ErrConflict, "snapshot event %s data disagrees with its receipt identity or origin timestamp", eventID)
		}
		payloadCreated, payloadOK := eventRecord["created"].([]any)
		if !payloadOK {
			return fail(ErrType, "snapshot event %s data has no created array", eventID)
		}
		payloadJSON, payloadErr := canonicalJSON(payloadCreated)
		receiptJSON, receiptErr := canonicalJSON(created)
		if payloadErr != nil || receiptErr != nil || !bytes.Equal(payloadJSON, receiptJSON) {
			return fail(ErrConflict, "snapshot event %s created identities disagree with its receipt", eventID)
		}
	}
	type reservedIdentity struct {
		selector any
		id       int64
	}
	reserved := make([]reservedIdentity, 0, len(created))
	for _, selector := range created {
		key, err := snapshotSelectorKey(selector)
		if err != nil {
			return err
		}
		if _, exists := state.identities[key]; exists {
			return fail(ErrConflict, "snapshot repeats identity %s", key)
		}
		state.identities[key] = state.next
		reserved = append(reserved, reservedIdentity{selector: selector, id: state.next})
		state.next++
	}
	tx := state.next
	state.next++
	for _, identity := range reserved {
		if name, ok := identity.selector.(string); ok {
			if _, err := runner.ExecContext(ctx, "INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (?,?,NULL,?)", identity.id, name, tx); err != nil {
				return wrap(ErrConflict, err, "cannot restore named identity %q", name)
			}
			continue
		}
		eid, gid, selectorErr := selectorUUID(identity.selector)
		if selectorErr != nil {
			return selectorErr
		}
		if _, err := runner.ExecContext(ctx, "INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (?,NULL,?,?)", identity.id, gid[:], tx); err != nil {
			return wrap(ErrConflict, err, "cannot restore identity %s", eid)
		}
	}
	if _, err := runner.ExecContext(ctx, "INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (?,NULL,?,?)", tx, eventUUID[:], tx); err != nil {
		return wrap(ErrConflict, err, "cannot restore event identity %s", eventID)
	}
	if _, err := runner.ExecContext(ctx, "INSERT INTO fgraph_events(tx,event_hash,event_data,operation_id,request_hash) VALUES (?,?,?,?,?)", tx, eventHash, eventData, operationID, requestHash); err != nil {
		return wrap(ErrConflict, err, "cannot restore event receipt %s", eventID)
	}
	state.identities["eid:"+eventID] = tx
	state.events[eventID] = tx
	state.receipts[tx] = snapshotReceipt{event: eventID, at: at, originAt: originAt}
	state.lastEvent = eventID
	state.receiptN++
	return nil
}

func (db *DB) restoreSnapshotFact(ctx context.Context, runner sqlRunner, state *snapshotRestoreState, wrapper map[string]any) error {
	if !exactKeys(wrapper, "fact") {
		return fail(ErrType, "snapshot fact wrapper must contain exactly fact")
	}
	tuple, ok := wrapper["fact"].([]any)
	if !ok || len(tuple) != 6 {
		return fail(ErrType, "snapshot fact must be [e,a,v,tag,assert-event,retract-event]")
	}
	entity, err := state.resolveSelector(tuple[0])
	if err != nil {
		return err
	}
	attribute, err := state.resolveSelector(tuple[1])
	if err != nil {
		return err
	}
	attributeName, ok := tuple[1].(string)
	if !ok {
		return fail(ErrType, "snapshot fact attribute must be a named selector")
	}
	tag, ok := tuple[3].(string)
	if !ok {
		return fail(ErrType, "snapshot fact tag must be text")
	}
	assertEvent, ok := tuple[4].(string)
	if !ok {
		return fail(ErrType, "snapshot assertion event must be a UUID")
	}
	tx, exists := state.events[assertEvent]
	if !exists || tx == GenesisTx {
		return fail(ErrNotFound, "snapshot fact assertion event %q is unknown", assertEvent)
	}
	value, err := db.snapshotStoredValue(tuple[2], tag, state)
	if err != nil {
		return err
	}
	id, err := db.insertFact(ctx, runner, plannedFact{e: entity, a: attribute, attr: attributeName, value: value}, tx)
	if err != nil {
		return err
	}
	if tuple[5] != nil {
		retractEvent, ok := tuple[5].(string)
		if !ok {
			return fail(ErrType, "snapshot retraction event must be null or UUID")
		}
		rx, exists := state.events[retractEvent]
		if !exists || rx <= tx {
			return fail(ErrConflict, "snapshot retraction event is unknown or not later")
		}
		if _, err := runner.ExecContext(ctx, "UPDATE fgraph_facts SET rx=? WHERE id=?", rx, id); err != nil {
			return wrap(ErrFormat, err, "cannot restore fact %d retraction", id)
		}
		if value.tag == TagText || value.tag == TagTextRef {
			if _, err := runner.ExecContext(ctx, "DELETE FROM fgraph_fts WHERE rowid=?", id); err != nil {
				return wrap(ErrFormat, err, "cannot remove restored historical fact %d from FTS", id)
			}
		}
	}
	state.factN++
	return nil
}

func (state *snapshotRestoreState) resolveSelector(selector any) (int64, error) {
	key, err := snapshotSelectorKey(selector)
	if err != nil {
		return 0, err
	}
	id, exists := state.identities[key]
	if !exists {
		return 0, fail(ErrNotFound, "snapshot fact references unknown identity %s", key)
	}
	return id, nil
}

func (db *DB) snapshotStoredValue(value any, tag string, state *snapshotRestoreState) (storedValue, error) {
	if tag == "ref" {
		fields, ok := objectFields(value)
		if !ok || len(fields) != 1 || fields[0].Name != "ref" {
			return storedValue{}, fail(ErrType, "snapshot ref fact must use {\"ref\":selector}")
		}
		ref, err := state.resolveSelector(fields[0].Value)
		if err != nil {
			return storedValue{}, err
		}
		return storedValue{logical: ref, storage: ref, tag: TagRef}, nil
	}
	input, err := taggedInput(value, tag)
	if err != nil {
		return storedValue{}, err
	}
	stored, err := scalarValue(input)
	if err != nil {
		return storedValue{}, err
	}
	if logicalTag(stored.tag) != tag {
		return storedValue{}, fail(ErrType, "snapshot fact value does not match logical tag %q", tag)
	}
	return stored, nil
}

func validateSnapshotFooter(record map[string]any, digest hash.Hash, state *snapshotRestoreState) error {
	if !exactKeys(record, "fgraph", "sha256", "receipts", "facts") || record["fgraph"] != "end" {
		return fail(ErrType, "snapshot footer is malformed")
	}
	footerHash, footerHashOK := record["sha256"].(string)
	if !footerHashOK || len(footerHash) != sha256.Size*2 {
		return fail(ErrType, "snapshot footer sha256 must be %d-byte lowercase hex", sha256.Size)
	}
	want, err := decodeLowerHex(footerHash, len(footerHash)/2, "snapshot footer sha256")
	if err != nil {
		return err
	}
	if !bytes.Equal(want, digest.Sum(nil)) {
		return fail(ErrConflict, "snapshot digest does not match its body; reject the truncated or modified stream")
	}
	receipts, receiptsOK := record["receipts"].(int64)
	facts, factsOK := record["facts"].(int64)
	if !receiptsOK || !factsOK || receipts != state.receiptN || facts != state.factN {
		return fail(ErrType, "snapshot footer counts do not match its body")
	}
	return nil
}

func (db *DB) validateRestoredReceiptTimes(ctx context.Context, runner sqlRunner, state *snapshotRestoreState) error {
	for tx, receipt := range state.receipts {
		var at int64
		if err := runner.QueryRowContext(ctx, "SELECT v FROM fgraph_facts WHERE e=? AND a=1 AND tx=?", tx, tx).Scan(&at); err != nil || at != receipt.at {
			return fail(ErrConflict, "snapshot receipt %s timestamp does not match its retained facts", receipt.event)
		}
		origin := at
		var imported int64
		if err := runner.QueryRowContext(ctx, "SELECT v FROM fgraph_facts WHERE e=? AND a=? AND tx=?", tx, importedAtAttrID, tx).Scan(&imported); err == nil {
			origin = imported
		} else if err != sql.ErrNoRows {
			return wrap(ErrFormat, err, "cannot validate restored receipt %s origin", receipt.event)
		}
		if origin != receipt.originAt {
			return fail(ErrConflict, "snapshot receipt %s origin timestamp does not match its retained facts", receipt.event)
		}
	}
	return nil
}

func (state *snapshotRestoreState) String() string {
	return fmt.Sprintf("receipts=%d facts=%d next=%d", state.receiptN, state.factN, state.next)
}
