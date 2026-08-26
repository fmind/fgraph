package fgraph

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
)

// Receipt returns durable event metadata without replaying the transaction.
// On a historical view, tx and read_basis_tx are both bounded by that view.
func (db *DB) Receipt(ctx context.Context, tx int64) (EventReceipt, error) {
	var receipt EventReceipt
	err := db.withRead(ctx, func(runner sqlRunner) error {
		readBasis := int64(0)
		if db.asOf != nil {
			readBasis = *db.asOf
		} else {
			var err error
			readBasis, err = db.basisOn(ctx, runner)
			if err != nil {
				return err
			}
		}
		if tx > readBasis {
			return fail(ErrNotFound, "transaction %d is after this view's basis %d", tx, readBasis)
		}
		var eventHash, requestHash, gid []byte
		var operationID sql.NullString
		var basis int64
		err := runner.QueryRowContext(ctx, `SELECT ev.event_hash,ev.operation_id,ev.request_hash,i.gid,
			COALESCE((SELECT MAX(prior.tx) FROM fgraph_events prior WHERE prior.tx<ev.tx),ev.tx)
			FROM fgraph_events ev JOIN fgraph_ids i ON i.id=ev.tx WHERE ev.tx=?`, tx).
			Scan(&eventHash, &operationID, &requestHash, &gid, &basis)
		if errors.Is(err, sql.ErrNoRows) {
			return fail(ErrNotFound, "transaction %d was not found; use a transaction id returned by transact", tx)
		}
		if err != nil {
			return wrap(ErrFormat, err, "cannot read transaction %d receipt", tx)
		}
		if len(eventHash) != 32 || len(gid) != 16 || (requestHash != nil && len(requestHash) != 32) {
			return fail(ErrFormat, "transaction %d has a malformed event receipt; restore a valid format-v2 snapshot", tx)
		}
		var eventUUID [16]byte
		copy(eventUUID[:], gid)
		receipt = EventReceipt{
			ReadBasisTx: readBasis,
			BasisTx:     basis,
			Tx:          tx,
			Event:       formatUUID(eventUUID),
			EventHash:   "sha256:" + hex.EncodeToString(eventHash),
			Facts:       []Fact{},
		}
		if operationID.Valid {
			value := operationID.String
			receipt.OperationID = &value
		}
		if requestHash != nil {
			value := "sha256:" + hex.EncodeToString(requestHash)
			receipt.RequestHash = &value
		}

		rows, err := runner.QueryContext(ctx, "SELECT id,e,a,v,t,tx,rx FROM fgraph_facts WHERE e=? AND tx=e ORDER BY id", tx)
		if err != nil {
			return wrap(ErrFormat, err, "cannot read transaction %d metadata", tx)
		}
		facts, err := scanRawFacts(rows)
		if err != nil {
			return err
		}
		atFound := false
		for _, fact := range facts {
			logical, err := db.logicalValue(ctx, runner, fact.v, fact.t)
			if err != nil {
				return err
			}
			switch fact.a {
			case 1:
				receipt.At = asInt64(logical)
				atFound = true
			case 2:
				value, ok := logical.(string)
				if !ok {
					return fail(ErrFormat, "transaction %d fgraph/by is not text", tx)
				}
				receipt.By = &value
			case 3:
				value, ok := logical.(string)
				if !ok {
					return fail(ErrFormat, "transaction %d fgraph/source is not text", tx)
				}
				receipt.Source = &value
			case 4:
				value := logical
				receipt.Meta = &value
			case importedAtAttrID:
				value := asInt64(logical)
				receipt.ImportedAt = &value
			default:
				rendered, err := db.renderRaw(ctx, runner, fact, nil)
				if err != nil {
					return err
				}
				receipt.Facts = append(receipt.Facts, rendered)
			}
		}
		if !atFound {
			return fail(ErrFormat, "transaction %d has no timestamp receipt; restore a valid format-v2 snapshot", tx)
		}
		return nil
	})
	return receipt, err
}
