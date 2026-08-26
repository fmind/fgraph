package fgraph

import (
	"context"
	"database/sql"
	"time"
)

func (db *DB) atTx(tx int64) *DB {
	return &DB{store: db.store, asOf: &tx, exec: db.exec}
}

// At returns a validated read-only view at a transaction or instant.
func (db *DB) At(ctx context.Context, value any) (*DB, error) {
	return db.viewAt(ctx, value)
}

func (db *DB) AtInstant(ctx context.Context, micros int64) (*DB, error) {
	if err := validateInstantMicros(micros); err != nil {
		return nil, err
	}
	var tx int64
	err := db.withRead(ctx, func(runner sqlRunner) error {
		query := `SELECT e FROM fgraph_facts WHERE a=1 AND tx=e AND t=5 AND v<=?`
		args := []any{micros}
		if db.asOf != nil {
			query += " AND e<=?"
			args = append(args, *db.asOf)
		}
		query += " ORDER BY v DESC,e DESC LIMIT 1"
		err := runner.QueryRowContext(ctx, query, args...).Scan(&tx)
		if err == sql.ErrNoRows {
			return fail(ErrNotFound, "no transaction exists at or before %s; choose a later instant", formatInstant(micros))
		}
		if err != nil {
			return wrap(ErrFormat, err, "cannot resolve transaction at %s", formatInstant(micros))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return db.atTx(tx), nil
}

func (db *DB) ViewAt(ctx context.Context, value any) (*DB, error) {
	return db.viewAt(ctx, value)
}

func (db *DB) viewAt(ctx context.Context, value any) (*DB, error) {
	switch value := value.(type) {
	case int:
		return db.viewAtInteger(ctx, int64(value))
	case int64:
		return db.viewAtInteger(ctx, value)
	case InstantValue:
		return db.AtInstant(ctx, value.Micros)
	case string:
		instant, err := parseRFC3339(value)
		if err != nil {
			return nil, fail(ErrType, "at value %q is neither a transaction id nor RFC 3339 instant; correct it", value)
		}
		return db.AtInstant(ctx, instant.UTC().UnixMicro())
	default:
		return nil, fail(ErrType, "at value has type %T; use a transaction id or RFC 3339 instant", value)
	}
}

func (db *DB) viewAtInteger(ctx context.Context, value int64) (*DB, error) {
	var transaction bool
	err := db.withRead(ctx, func(runner sqlRunner) error {
		var exists int
		if err := runner.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM fgraph_facts WHERE e=? AND a=1 AND tx=e)", value).Scan(&exists); err != nil {
			return wrap(ErrFormat, err, "cannot distinguish transaction %d from an instant", value)
		}
		transaction = exists != 0
		return nil
	})
	if err != nil {
		return nil, err
	}
	if transaction {
		if db.asOf != nil && value > *db.asOf {
			return db.atTx(*db.asOf), nil
		}
		return db.atTx(value), nil
	}
	return db.AtInstant(ctx, value)
}

type txInfo struct {
	full     map[string]any
	by       string
	source   string
	at       int64
	presence factPresence
}

func (db *DB) transactionInfo(ctx context.Context, runner sqlRunner, tx int64) (info txInfo, resultErr error) {
	rows, err := runner.QueryContext(ctx, `SELECT f.v,f.t,i.name
		FROM fgraph_facts f JOIN fgraph_ids i ON i.id=f.a WHERE f.e=? AND f.rx IS NULL ORDER BY f.id`, tx)
	if err != nil {
		return txInfo{}, wrap(ErrFormat, err, "cannot read provenance transaction %d", tx)
	}
	defer func() {
		resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "provenance transaction rows"))
	}()
	info = txInfo{full: map[string]any{}}
	for rows.Next() {
		var raw any
		var tag Tag
		var attr string
		if err := rows.Scan(&raw, &tag, &attr); err != nil {
			return txInfo{}, wrap(ErrFormat, err, "cannot decode provenance transaction %d", tx)
		}
		logical, err := db.logicalValue(ctx, runner, raw, tag)
		if err != nil {
			return txInfo{}, err
		}
		rendered := db.renderLogical(logical, tag)
		info.full[attr] = rendered
		switch attr {
		case "fgraph/at":
			info.at = asInt64(logical)
			info.presence |= factAtPresent
		case "fgraph/by":
			by, ok := logical.(string)
			if !ok {
				return txInfo{}, fail(ErrFormat, "transaction %d fgraph/by has type %T; repair its receipt", tx, logical)
			}
			info.by = by
			info.presence |= factByPresent
		case "fgraph/source":
			source, ok := logical.(string)
			if !ok {
				return txInfo{}, fail(ErrFormat, "transaction %d fgraph/source has type %T; repair its receipt", tx, logical)
			}
			info.source = source
			info.presence |= factSourcePresent
		}
	}
	if err := rows.Err(); err != nil {
		return txInfo{}, wrap(ErrFormat, err, "cannot finish reading provenance transaction %d", tx)
	}
	return info, nil
}

func (db *DB) History(ctx context.Context, ref any, attr ...string) ([]Fact, error) {
	result := []Fact{}
	err := db.withRead(ctx, func(runner sqlRunner) error {
		entity, found, err := db.resolveReadEntity(ctx, runner, ref)
		if err != nil {
			return err
		}
		if !found {
			return fail(ErrNotFound, "entity %v does not exist; use a known name or id", ref)
		}
		query := "SELECT id,e,a,v,t,tx,rx FROM fgraph_facts WHERE e=?"
		args := []any{entity}
		if db.asOf != nil {
			query += " AND tx<=?"
			args = append(args, *db.asOf)
		}
		if len(attr) > 0 {
			attrID, ok := db.store.names[attr[0]]
			if !ok {
				return fail(ErrNotFound, "attribute %q does not exist; use a known attribute", attr[0])
			}
			query += " AND a=?"
			args = append(args, attrID)
		}
		query += " ORDER BY tx,id"
		rows, err := runner.QueryContext(ctx, query, args...)
		if err != nil {
			return wrap(ErrFormat, err, "cannot read history for %v", ref)
		}
		raw, err := scanRawFacts(rows)
		if err != nil {
			return err
		}
		cache := map[int64]txInfo{}
		for _, fact := range raw {
			rendered, err := db.renderViewRaw(ctx, runner, fact)
			if err != nil {
				return err
			}
			info, ok := cache[fact.tx]
			if !ok {
				info, err = db.transactionInfo(ctx, runner, fact.tx)
				if err != nil {
					return err
				}
				cache[fact.tx] = info
			}
			rendered.At, rendered.By, rendered.Source = info.at, info.by, info.source
			rendered.presence |= info.presence & (factAtPresent | factByPresent | factSourcePresent)
			if rendered.Rx != nil {
				rxInfo, ok := cache[fact.rx.Int64]
				if !ok {
					rxInfo, err = db.transactionInfo(ctx, runner, fact.rx.Int64)
					if err != nil {
						return err
					}
					cache[fact.rx.Int64] = rxInfo
				}
				rendered.RxAt, rendered.RxBy, rendered.RxSource = rxInfo.at, rxInfo.by, rxInfo.source
				if rxInfo.presence&factAtPresent != 0 {
					rendered.presence |= factRxAtPresent
				}
				if rxInfo.presence&factByPresent != 0 {
					rendered.presence |= factRxByPresent
				}
				if rxInfo.presence&factSourcePresent != 0 {
					rendered.presence |= factRxSourcePresent
				}
			}
			result = append(result, rendered)
		}
		return nil
	})
	return result, err
}

func (db *DB) Diff(ctx context.Context, from, to int64) (Diff, error) {
	if from > to {
		return Diff{}, fail(ErrType, "diff start %d is after end %d; swap the transaction ids", from, to)
	}
	result := Diff{Asserted: []Fact{}, Retracted: []Fact{}}
	if db.asOf != nil {
		if from >= *db.asOf {
			return result, nil
		}
		if to > *db.asOf {
			to = *db.asOf
		}
	}
	err := db.withRead(ctx, func(runner sqlRunner) error {
		asserted, err := db.rangeFacts(ctx, runner, "tx", from, to)
		if err != nil {
			return err
		}
		retracted, err := db.rangeFacts(ctx, runner, "rx", from, to)
		if err != nil {
			return err
		}
		result.Asserted, result.Retracted = asserted, retracted
		return nil
	})
	return result, err
}

func (db *DB) rangeFacts(ctx context.Context, runner sqlRunner, column string, from, to int64) ([]Fact, error) {
	query := "SELECT id,e,a,v,t,tx,rx FROM fgraph_facts WHERE " + column + ">? AND " + column + "<=? ORDER BY " + column + ",id"
	rows, err := runner.QueryContext(ctx, query, from, to)
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot compute temporal range %d..%d", from, to)
	}
	raw, err := scanRawFacts(rows)
	if err != nil {
		return nil, err
	}
	result := make([]Fact, 0, len(raw))
	for _, fact := range raw {
		rendered, err := db.renderViewRaw(ctx, runner, fact)
		if err != nil {
			return nil, err
		}
		result = append(result, rendered)
	}
	return result, nil
}

func (db *DB) Changes(ctx context.Context, since int64, until ...int64) (Diff, error) {
	to := int64(^uint64(0) >> 1)
	if len(until) > 0 {
		to = until[0]
	}
	return db.Diff(ctx, since, to)
}

func (db *DB) Why(ctx context.Context, ref any, attr ...string) ([]Fact, error) {
	result := []Fact{}
	err := db.withRead(ctx, func(runner sqlRunner) error {
		entity, found, err := db.resolveReadEntity(ctx, runner, ref)
		if err != nil {
			return err
		}
		if !found {
			return fail(ErrNotFound, "entity %v does not exist; use a known name or id", ref)
		}
		visibility, visibilityArgs := db.visibility("d")
		query := "SELECT d.id,d.e,d.a,d.v,d.t,d.tx,d.rx FROM fgraph_facts d WHERE d.e=? AND " + visibility
		args := append([]any{entity}, visibilityArgs...)
		if len(attr) > 0 {
			attrID, ok := db.store.names[attr[0]]
			if !ok {
				return fail(ErrNotFound, "attribute %q does not exist; use a known attribute", attr[0])
			}
			query += " AND d.a=?"
			args = append(args, attrID)
		}
		query += " ORDER BY id"
		rows, err := runner.QueryContext(ctx, query, args...)
		if err != nil {
			return wrap(ErrFormat, err, "cannot read current facts for %v", ref)
		}
		raw, err := scanRawFacts(rows)
		if err != nil {
			return err
		}
		cache := map[int64]txInfo{}
		for _, fact := range raw {
			rendered, err := db.renderViewRaw(ctx, runner, fact)
			if err != nil {
				return err
			}
			info, ok := cache[fact.tx]
			if !ok {
				info, err = db.transactionInfo(ctx, runner, fact.tx)
				if err != nil {
					return err
				}
				cache[fact.tx] = info
			}
			rendered.Provenance = info.full
			result = append(result, rendered)
		}
		return nil
	})
	return result, err
}

func (db *DB) Speculate(ctx context.Context, callback func(*DB) error) (resultErr error) {
	if err := db.checkUsable(true); err != nil {
		return err
	}
	if callback == nil {
		return fail(ErrType, "speculation callback is nil; provide a callback to transact and query")
	}
	if db.exec != nil {
		return fail(ErrUnsupported, "nested speculation is unsupported; use one outer speculation scope")
	}
	db.store.mu.Lock()
	defer db.store.mu.Unlock()
	conn, err := db.store.sql.Conn(ctx)
	if err != nil {
		return wrap(ErrFormat, err, "cannot acquire SQLite connection for speculation")
	}
	defer func() {
		resultErr = joinErrors(resultErr, wrapClose(conn.Close(), "speculation database connection"))
	}()
	if _, err := conn.ExecContext(ctx, "SAVEPOINT fgraph_speculate"); err != nil {
		return wrap(ErrFormat, err, "cannot begin speculation savepoint")
	}
	view := &DB{store: db.store, exec: conn}
	callbackErr := callback(view)
	_, rollbackErr := conn.ExecContext(context.Background(), "ROLLBACK TO fgraph_speculate")
	_, releaseErr := conn.ExecContext(context.Background(), "RELEASE fgraph_speculate")
	db.store.dataVersion = -1
	if callbackErr != nil {
		return callbackErr
	}
	if rollbackErr != nil {
		return wrap(ErrFormat, rollbackErr, "cannot roll back speculation")
	}
	if releaseErr != nil {
		return wrap(ErrFormat, releaseErr, "cannot release speculation savepoint")
	}
	return nil
}

func (db *DB) Undo(ctx context.Context, target int64, options ...TxOption) (TxReport, error) {
	if target > 0 && target <= GenesisTx {
		return TxReport{}, fail(ErrUnsupported, "system transaction %d cannot be undone; genesis is immutable", target)
	}
	prepare := func(ctx context.Context, runner sqlRunner) (any, error) {
		// Discovery must share the writer transaction with planning and commit so
		// undo never retracts an identical value asserted after the target.
		return db.undoOperations(ctx, runner, target)
	}
	transactionOptions := append([]TxOption{}, options...)
	transactionOptions = append(
		transactionOptions,
		withRequestHashBase(map[string]any{"operation": "undo", "tx": target}),
		func(config *txOptions) { config.prepareData = prepare },
		WithTxFacts(E{"fgraph/undoes": RefTo(target)}),
	)
	return db.Transact(ctx, []any{}, transactionOptions...)
}

func (db *DB) undoOperations(ctx context.Context, runner sqlRunner, target int64) ([]any, error) {
	var operations []any
	var exists int
	if err := runner.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM fgraph_facts WHERE e=? AND a=1 AND tx=e)", target).Scan(&exists); err != nil {
		return nil, wrap(ErrFormat, err, "cannot inspect transaction %d", target)
	}
	if exists == 0 {
		return nil, fail(ErrNotFound, "transaction %d does not exist; use a tx id from history or a receipt", target)
	}
	rows, err := runner.QueryContext(ctx, `SELECT id,e,a,v,t,tx,rx FROM fgraph_facts
			WHERE (tx=? AND e<>? AND rx IS NULL) OR rx=? ORDER BY id`, target, target, target)
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot plan undo for transaction %d", target)
	}
	facts, err := scanRawFacts(rows)
	if err != nil {
		return nil, err
	}
	for _, fact := range facts {
		var attr string
		if err := runner.QueryRowContext(ctx, "SELECT name FROM fgraph_ids WHERE id=?", fact.a).Scan(&attr); err != nil {
			return nil, wrap(ErrFormat, err, "cannot resolve undo attribute %d", fact.a)
		}
		logical, err := db.logicalValue(ctx, runner, fact.v, fact.t)
		if err != nil {
			return nil, err
		}
		input, inputErr := inputValue(logical, fact.t)
		if inputErr != nil {
			return nil, inputErr
		}
		if fact.tx == target && !fact.rx.Valid {
			operations = append(operations, []any{"retract", fact.e, attr, input})
		} else if fact.rx.Valid && fact.rx.Int64 == target {
			operations = append(operations, []any{"assert", fact.e, attr, input})
		}
	}
	return operations, nil
}

func inputValue(logical any, tag Tag) (any, error) {
	switch tag {
	case TagRef:
		return RefTo(logical), nil
	case TagInstant:
		return Instant(asInt64(logical)), nil
	case TagBytes, TagBytesRef:
		value, ok := logical.([]byte)
		if !ok {
			return nil, fail(ErrFormat, "stored bytes value decoded as %T; repair the fact", logical)
		}
		return Bytes(value), nil
	case TagVector:
		value, ok := logical.([]float32)
		if !ok {
			return nil, fail(ErrFormat, "stored vector value decoded as %T; repair the fact", logical)
		}
		return Vector(value), nil
	case TagJSON:
		return JSON(logical), nil
	default:
		return logical, nil
	}
}

func (db *DB) Follow(ctx context.Context, options FollowOptions) <-chan FollowEvent {
	output := make(chan FollowEvent)
	interval := options.Interval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	go func() {
		defer close(output)
		if db.asOf != nil {
			select {
			case output <- FollowEvent{Err: fail(ErrUnsupported, "follow is unavailable on a historical view; follow the current database instead")}:
			case <-ctx.Done():
			}
			return
		}
		since := options.Since
		if since == 0 {
			since = GenesisTx
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			if ctx.Err() != nil {
				return
			}
			transactions, err := db.transactionIDsAfter(ctx, since)
			if err != nil {
				if ctx.Err() == nil {
					select {
					case output <- FollowEvent{Err: err}:
					case <-ctx.Done():
					}
				}
				return
			}
			for _, tx := range transactions {
				record, err := db.eventRecordForTx(ctx, tx)
				if err != nil {
					if ctx.Err() == nil {
						select {
						case output <- FollowEvent{Err: err}:
						case <-ctx.Done():
						}
					}
					return
				}
				select {
				case output <- FollowEvent{Tx: tx, Record: record}:
				case <-ctx.Done():
					return
				}
				since = tx
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()
	return output
}

func (db *DB) transactionIDsAfter(ctx context.Context, since int64) ([]int64, error) {
	transactions := []int64{}
	err := db.withRead(ctx, func(runner sqlRunner) (resultErr error) {
		rows, err := runner.QueryContext(ctx, `SELECT tx FROM fgraph_events WHERE tx>? ORDER BY tx`, since)
		if err != nil {
			return wrap(ErrFormat, err, "cannot read committed transactions after %d", since)
		}
		defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "follow transaction rows")) }()
		for rows.Next() {
			var tx int64
			if err := rows.Scan(&tx); err != nil {
				return wrap(ErrFormat, err, "cannot decode a committed transaction after %d", since)
			}
			transactions = append(transactions, tx)
		}
		if err := rows.Err(); err != nil {
			return wrap(ErrFormat, err, "cannot finish reading committed transactions after %d", since)
		}
		return nil
	})
	return transactions, err
}

func (db *DB) latestTx(ctx context.Context) (int64, error) {
	latest := int64(GenesisTx)
	err := db.withRead(ctx, func(runner sqlRunner) error {
		if err := runner.QueryRowContext(ctx, "SELECT COALESCE(MAX(tx),64) FROM fgraph_events").Scan(&latest); err != nil {
			return wrap(ErrFormat, err, "cannot resolve the latest transaction")
		}
		return nil
	})
	return latest, err
}
