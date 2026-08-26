package fgraph

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	googleuuid "github.com/google/uuid"
)

const genesisEventID = "00000000-0000-4000-8000-000000000040"

func randomUUIDString() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", wrap(ErrFormat, err, "cannot generate event UUID; check the operating-system random source")
	}
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return formatUUID(id), nil
}

func deterministicEventID(seed string, tx int64) string {
	digest := sha256.Sum256([]byte("fgraph-event/1\x00" + seed + "\x00" + strconv.FormatInt(tx, 10)))
	var id [16]byte
	copy(id[:], digest[:16])
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return formatUUID(id)
}

func (s *store) nextEventID(tx int64) (string, error) {
	if s.eventSeed != nil {
		return deterministicEventID(*s.eventSeed, tx), nil
	}
	return s.eventIDs()
}

func parseUUID(value string) ([16]byte, error) {
	var result [16]byte
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' || value != strings.ToLower(value) {
		return result, fail(ErrType, "event id %q is not a canonical lowercase UUID", value)
	}
	compact := strings.ReplaceAll(value, "-", "")
	decoded, err := hex.DecodeString(compact)
	if err != nil || len(decoded) != len(result) {
		return result, wrap(ErrType, err, "event id %q is not a canonical lowercase UUID", value)
	}
	copy(result[:], decoded)
	return result, nil
}

func formatUUID(id [16]byte) string {
	raw := hex.EncodeToString(id[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", raw[:8], raw[8:12], raw[12:16], raw[16:20], raw[20:])
}

func anonymousUUID(event [16]byte, ordinal uint64) [16]byte {
	var name [8]byte
	binary.BigEndian.PutUint64(name[:], ordinal)
	// RFC UUIDv5 fixes SHA-1 as its interoperable name-hash algorithm; this is
	// identity derivation, not a collision-resistance security boundary.
	return [16]byte(googleuuid.NewSHA1(googleuuid.UUID(event), name[:]))
}

func genesisEventRecord(at int64) map[string]any {
	created := make([]any, 0, len(systemNames)-1)
	for id := 1; id < len(systemNames); id++ {
		created = append(created, systemNames[id])
	}
	return map[string]any{
		"fgraph":    "event/1",
		"event":     genesisEventID,
		"at":        at,
		"created":   created,
		"asserted":  []any{},
		"retracted": []any{},
	}
}

func canonicalEventData(record map[string]any) (string, [sha256.Size]byte, error) {
	encoded, err := canonicalJSON(record)
	if err != nil {
		return "", [sha256.Size]byte{}, wrap(ErrFormat, err, "cannot encode portable event record")
	}
	if len(encoded) > maxPortableLineBytes {
		return "", [sha256.Size]byte{}, fail(ErrTooLarge, "canonical event is %d bytes; keep it at or below %d bytes so tail and apply remain bounded", len(encoded), maxPortableLineBytes)
	}
	return string(encoded), sha256.Sum256(encoded), nil
}

func genesisEventData(at int64) (string, [sha256.Size]byte, error) {
	return canonicalEventData(genesisEventRecord(at))
}

func canonicalRequestHash(data any, options txOptions) ([sha256.Size]byte, error) {
	wireOptions := map[string]any{}
	if options.by != nil {
		wireOptions["by"] = *options.by
	}
	if options.source != nil {
		wireOptions["source"] = *options.source
	}
	if options.metaSet {
		wireOptions["meta"] = wireValue(options.meta)
	}
	if options.txFactsSet {
		wireOptions["tx"] = wireValue(options.txFacts)
	}
	return canonicalLogicalRequestHash(map[string]any{"data": wireValue(data), "options": wireOptions})
}

func canonicalLogicalRequestHash(request any) ([sha256.Size]byte, error) {
	encoded, err := canonicalJSON(request)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

func withRequestHashOverride(hash [sha256.Size]byte) TxOption {
	return func(options *txOptions) {
		options.requestHash = append([]byte(nil), hash[:]...)
	}
}

func withRequestHashBase(base map[string]any) TxOption {
	return func(options *txOptions) {
		options.requestHashBase = base
	}
}

func validateOperationID(value string) error {
	if len(value) == 0 || len(value) > 512 || !utf8.ValidString(value) {
		return fail(ErrType, "operation id must be valid UTF-8 between 1 and 512 bytes")
	}
	for _, char := range value {
		if unicode.IsControl(char) {
			return fail(ErrType, "operation id contains a control character; use printable text")
		}
	}
	return nil
}

func (db *DB) basisOn(ctx context.Context, runner sqlRunner) (int64, error) {
	var basis int64
	if err := runner.QueryRowContext(ctx, "SELECT COALESCE(MAX(tx),0) FROM fgraph_events").Scan(&basis); err != nil {
		return 0, wrap(ErrFormat, err, "cannot read the current event basis")
	}
	return basis, nil
}

func (db *DB) operationReceipt(
	ctx context.Context,
	runner sqlRunner,
	operationID string,
	requestHash [sha256.Size]byte,
) (TxReport, bool, error) {
	var tx int64
	var stored []byte
	err := runner.QueryRowContext(ctx, "SELECT tx,request_hash FROM fgraph_events WHERE operation_id=?", operationID).Scan(&tx, &stored)
	if errors.Is(err, sql.ErrNoRows) {
		return TxReport{}, false, nil
	}
	if err != nil {
		return TxReport{}, false, wrap(ErrFormat, err, "cannot inspect operation id %q", operationID)
	}
	if !bytes.Equal(stored, requestHash[:]) {
		return TxReport{}, false, fail(ErrConflict, "operation id %q was already used for different input", operationID)
	}
	report, err := db.eventReceipt(ctx, runner, tx)
	if err != nil {
		return TxReport{}, false, err
	}
	return alreadyAppliedReport(report), true, nil
}

func alreadyAppliedReport(report TxReport) TxReport {
	report.Status = "already_applied"
	// A retry receipt identifies the original durable event without replaying
	// its changes as if they were applied again.
	report.IDs = map[string]int64{}
	report.Asserted = []Fact{}
	report.Retracted = []Fact{}
	return report
}

func (db *DB) eventReceipt(ctx context.Context, runner sqlRunner, tx int64) (TxReport, error) {
	var at int64
	if err := runner.QueryRowContext(ctx, "SELECT v FROM fgraph_facts WHERE e=? AND a=1 AND tx=?", tx, tx).Scan(&at); err != nil {
		return TxReport{}, wrap(ErrFormat, err, "cannot read event %d timestamp", tx)
	}
	var gid []byte
	if err := runner.QueryRowContext(ctx, "SELECT gid FROM fgraph_ids WHERE id=?", tx).Scan(&gid); err != nil || len(gid) != 16 {
		return TxReport{}, wrap(ErrFormat, err, "cannot read event %d UUID", tx)
	}
	var uuid [16]byte
	copy(uuid[:], gid)
	var basis int64
	if err := runner.QueryRowContext(ctx, "SELECT COALESCE(MAX(tx),0) FROM fgraph_events WHERE tx<?", tx).Scan(&basis); err != nil {
		return TxReport{}, wrap(ErrFormat, err, "cannot read event %d basis", tx)
	}
	if basis == 0 {
		basis = tx
	}
	report := TxReport{
		Tx: tx, At: at, EventID: formatUUID(uuid), BasisTx: basis,
		IDs: map[string]int64{}, Asserted: []Fact{}, Retracted: []Fact{},
	}
	for _, selection := range []struct {
		dest  *[]Fact
		query string
		rx    bool
	}{
		{dest: &report.Asserted, query: "SELECT id,e,a,v,t,tx,rx FROM fgraph_facts WHERE tx=? ORDER BY id"},
		{dest: &report.Retracted, query: "SELECT id,e,a,v,t,tx,rx FROM fgraph_facts WHERE rx=? ORDER BY id", rx: true},
	} {
		rows, err := runner.QueryContext(ctx, selection.query, tx)
		if err != nil {
			return TxReport{}, wrap(ErrFormat, err, "cannot read event %d facts", tx)
		}
		raw, err := scanRawFacts(rows)
		if err != nil {
			return TxReport{}, err
		}
		for _, fact := range raw {
			var override *int64
			if selection.rx {
				override = &tx
			}
			rendered, err := db.renderRaw(ctx, runner, fact, override)
			if err != nil {
				return TxReport{}, err
			}
			*selection.dest = append(*selection.dest, rendered)
		}
	}
	return report, nil
}
