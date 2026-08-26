package fgraph

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"
)

type attributeSchema struct {
	typeName     string
	vectorModel  string
	dims         int64
	many         bool
	unique       bool
	nohistory    bool
	nohistorySet bool
	dimsSet      bool
}

func (schema attributeSchema) deletesHistory() bool {
	// A dimensions declaration is itself enough to establish a vector schema,
	// even when the caller leaves fgraph/type to inference.
	return schema.nohistory || (!schema.nohistorySet && (schema.typeName == "vector" || schema.dimsSet))
}

type plannedFact struct {
	attr     string
	value    storedValue
	e        int64
	a        int64
	order    int
	txTarget bool
	cas      bool
}

type retractRequest struct {
	a       *int64
	value   *storedValue
	attr    string
	e       int64
	order   int
	missing bool
	cas     bool
}

type transactionPlan struct {
	tempids    map[string]int64
	allocator  *allocator
	casTargets map[string]bool
	schemas    map[int64]attributeSchema
	assertions []plannedFact
	retracts   []retractRequest
	order      int
}

func isSchemaAttribute(attribute int64) bool {
	return attribute >= 5 && attribute <= 10 || attribute == 14
}

func appendAssertion(plan *transactionPlan, fact plannedFact) {
	plan.assertions = append(plan.assertions, fact)
	if isSchemaAttribute(fact.a) && plan.schemas != nil {
		delete(plan.schemas, fact.e)
	}
}

func appendAssertions(plan *transactionPlan, facts ...plannedFact) {
	for _, fact := range facts {
		appendAssertion(plan, fact)
	}
}

type preparedMapField struct {
	values []storedValue
	schema attributeSchema
	attrID int64
}

type rawFact struct {
	v  any
	rx sql.NullInt64
	id int64
	e  int64
	a  int64
	t  Tag
	tx int64
}

func (db *DB) Transact(ctx context.Context, data any, options ...TxOption) (result TxReport, resultErr error) {
	if err := db.checkUsable(true); err != nil {
		return TxReport{}, err
	}
	config := txOptions{}
	for _, option := range options {
		option(&config)
	}
	if db.exec != nil {
		return db.transactOn(ctx, db.exec, data, config)
	}
	db.store.mu.Lock()
	defer db.store.mu.Unlock()
	if err := db.checkUsable(true); err != nil {
		return TxReport{}, err
	}
	conn, connErr := db.store.sql.Conn(ctx)
	if connErr != nil {
		return TxReport{}, wrap(ErrFormat, connErr, "cannot acquire SQLite writer connection")
	}
	committed := false
	defer func() {
		closeErr := wrapClose(conn.Close(), "transaction database connection")
		if !committed {
			resultErr = joinErrors(resultErr, closeErr)
		}
	}()
	if _, beginErr := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); beginErr != nil {
		return TxReport{}, wrap(ErrConflict, beginErr, "cannot acquire the single-writer lock; retry after the other writer completes")
	}
	runner := newPreparedRunner(conn)
	report, err := db.transactOn(ctx, runner, data, config)
	closeErr := wrapClose(runner.Close(), "prepared transaction statements")
	if err != nil {
		err = joinErrors(err, closeErr)
		err = joinErrors(err, rollbackSQLite(conn, "failed transaction"))
		db.store.dataVersion = -1
		return TxReport{}, err
	}
	if closeErr != nil {
		db.store.dataVersion = -1
		return TxReport{}, joinErrors(closeErr, rollbackSQLite(conn, "transaction after statement-close failure"))
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		commitErr := wrap(ErrFormat, err, "cannot commit transaction; retry after checking disk space")
		db.store.dataVersion = -1
		return TxReport{}, joinErrors(commitErr, rollbackSQLite(conn, "transaction after commit failure"))
	}
	committed = true
	return report, nil
}

func rollbackSQLite(conn *sql.Conn, description string) error {
	if _, err := conn.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		return wrap(ErrFormat, err, "cannot roll back %s", description)
	}
	return nil
}

// Add is the fluent alias used by ingestion-oriented callers.
func (db *DB) Add(ctx context.Context, data any, options ...TxOption) (TxReport, error) {
	return db.Transact(ctx, data, options...)
}

func (db *DB) transactOn(ctx context.Context, runner sqlRunner, data any, config txOptions) (TxReport, error) {
	basis, basisErr := db.basisOn(ctx, runner)
	if basisErr != nil {
		return TxReport{}, basisErr
	}
	if config.requestHash != nil && len(config.requestHash) != sha256.Size {
		return TxReport{}, fail(ErrType, "internal request hash override must be a 32-byte SHA-256 digest")
	}
	if config.requestHash != nil && config.requestHashBase != nil {
		return TxReport{}, fail(ErrType, "internal request hash override and base are mutually exclusive")
	}
	var requestHash [32]byte
	if config.operationID != nil {
		if err := validateOperationID(*config.operationID); err != nil {
			return TxReport{}, err
		}
		if config.requestHash != nil {
			copy(requestHash[:], config.requestHash)
		} else if config.requestHashBase != nil {
			request := make(map[string]any, len(config.requestHashBase)+1)
			for key, value := range config.requestHashBase {
				request[key] = value
			}
			if config.by != nil {
				request["by"] = *config.by
			}
			var hashErr error
			requestHash, hashErr = canonicalLogicalRequestHash(request)
			if hashErr != nil {
				return TxReport{}, hashErr
			}
		} else {
			var hashErr error
			requestHash, hashErr = canonicalRequestHash(data, config)
			if hashErr != nil {
				return TxReport{}, hashErr
			}
		}
		prior, found, priorErr := db.operationReceipt(ctx, runner, *config.operationID, requestHash)
		if priorErr != nil {
			return TxReport{}, priorErr
		}
		if found {
			return prior, nil
		}
	}
	if config.ifBasisTx != nil && *config.ifBasisTx != basis {
		return TxReport{}, fail(ErrConflict, "basis is %d, not requested %d; refresh state and retry", basis, *config.ifBasisTx)
	}
	if err := db.store.refreshNames(ctx, runner); err != nil {
		return TxReport{}, err
	}
	if config.prepareData != nil {
		prepared, prepareErr := config.prepareData(ctx, runner)
		if prepareErr != nil {
			return TxReport{}, prepareErr
		}
		data = prepared
	}
	if config.declaration != nil {
		if err := db.validateDeclarationOn(ctx, runner, config.declarationAttr, *config.declaration); err != nil {
			return TxReport{}, err
		}
	}
	alloc, allocErr := newAllocator(ctx, runner, db.store)
	if allocErr != nil {
		return TxReport{}, allocErr
	}
	plan := &transactionPlan{tempids: map[string]int64{}, allocator: alloc, schemas: map[int64]attributeSchema{}}
	if err := db.preallocateEventIdentities(ctx, runner, plan, config.preallocated); err != nil {
		return TxReport{}, err
	}
	if planErr := db.planData(ctx, runner, plan, data); planErr != nil {
		return TxReport{}, planErr
	}
	txFacts := []plannedFact{}
	if config.txFactsSet {
		var txFactsErr error
		txFacts, txFactsErr = db.planTxFacts(ctx, runner, plan, 0, config.txFacts)
		if txFactsErr != nil {
			return TxReport{}, txFactsErr
		}
	}
	if declarationErr := db.validatePlannedDeclarations(ctx, runner, plan); declarationErr != nil {
		return TxReport{}, declarationErr
	}
	if casErr := validateCASIsolation(plan); casErr != nil {
		return TxReport{}, casErr
	}
	assertions, retractions, err := db.diffPlan(ctx, runner, plan)
	if err != nil {
		return TxReport{}, err
	}
	if compactErr := compactPendingAllocations(ctx, runner, plan, assertions, txFacts); compactErr != nil {
		return TxReport{}, compactErr
	}
	hasMetadata := config.by != nil || config.source != nil || config.metaSet || len(txFacts) > 0 || config.operationID != nil
	hasIdentity := len(alloc.pending) > 0
	if len(assertions) == 0 && len(retractions) == 0 && !config.force && !hasMetadata && !hasIdentity {
		if flushErr := alloc.flush(ctx); flushErr != nil {
			return TxReport{}, flushErr
		}
		return TxReport{Status: "noop", BasisTx: basis, IDs: reportIDs(alloc, plan), Asserted: []Fact{}, Retracted: []Fact{}}, nil
	}
	var byValue, sourceValue, metaValue *storedValue
	if config.by != nil {
		value, valueErr := textStored(*config.by)
		if valueErr != nil {
			return TxReport{}, valueErr
		}
		byValue = &value
	}
	if config.source != nil {
		value, valueErr := textStored(*config.source)
		if valueErr != nil {
			return TxReport{}, valueErr
		}
		sourceValue = &value
	}
	if config.metaSet {
		value, valueErr := jsonStored(config.meta)
		if valueErr != nil {
			return TxReport{}, valueErr
		}
		metaValue = &value
	}
	tx, err := alloc.tx()
	if err != nil {
		return TxReport{}, err
	}
	for index := range assertions {
		if assertions[index].txTarget {
			assertions[index].e = tx
		}
	}
	at, err := db.nextTimestamp(ctx, runner, config.at)
	if err != nil {
		return TxReport{}, err
	}
	var eventID string
	if config.eventID != nil {
		eventID = *config.eventID
	} else {
		var eventErr error
		eventID, eventErr = db.store.nextEventID(tx)
		if eventErr != nil {
			return TxReport{}, eventErr
		}
	}
	eventUUID, parseErr := parseUUID(eventID)
	if parseErr != nil {
		return TxReport{}, parseErr
	}
	if eventUUID[8]&0xc0 != 0x80 || (config.eventID == nil && eventUUID[6]>>4 != 4) {
		return TxReport{}, fail(ErrType, "event id %q is not an RFC 9562 UUIDv4", eventID)
	}
	if err := alloc.finalize(ctx, tx, eventUUID); err != nil {
		return TxReport{}, err
	}
	report := TxReport{Status: "applied", Tx: tx, At: at, EventID: eventID, BasisTx: basis, IDs: reportIDs(alloc, plan), Asserted: []Fact{}, Retracted: []Fact{}}
	metadata := []plannedFact{{e: tx, a: 1, attr: systemNames[1], value: storedValue{logical: at, storage: at, tag: TagInstant}}}
	if byValue != nil {
		metadata = append(metadata, plannedFact{e: tx, a: 2, attr: systemNames[2], value: *byValue})
	}
	if sourceValue != nil {
		metadata = append(metadata, plannedFact{e: tx, a: 3, attr: systemNames[3], value: *sourceValue})
	}
	if metaValue != nil {
		metadata = append(metadata, plannedFact{e: tx, a: 4, attr: systemNames[4], value: *metaValue})
	}
	for _, fact := range append(metadata, assertions...) {
		inserted, insertErr := db.insertFact(ctx, runner, fact, tx)
		if insertErr != nil {
			return TxReport{}, insertErr
		}
		report.Asserted = append(report.Asserted, db.renderPlanned(inserted, fact, tx, nil, alloc))
	}
	garbageCandidates := map[string][]byte{}
	for _, fact := range retractions {
		schema, schemaErr := db.schemaFor(ctx, runner, fact.a, nil)
		if schemaErr != nil {
			return TxReport{}, schemaErr
		}
		if schema.deletesHistory() {
			if _, err := runner.ExecContext(ctx, "DELETE FROM fgraph_facts WHERE id=?", fact.id); err != nil {
				return TxReport{}, wrap(ErrFormat, err, "cannot delete no-history fact %d", fact.id)
			}
		} else if _, err := runner.ExecContext(ctx, "UPDATE fgraph_facts SET rx=? WHERE id=? AND rx IS NULL", tx, fact.id); err != nil {
			return TxReport{}, wrap(ErrFormat, err, "cannot retract fact %d", fact.id)
		}
		if fact.t == TagTextRef || fact.t == TagBytesRef || fact.t == TagVector {
			if hash, ok := fact.v.([]byte); ok {
				garbageCandidates[hex.EncodeToString(hash)] = append([]byte(nil), hash...)
			}
		}
		if fact.t == TagText || fact.t == TagTextRef {
			if _, err := runner.ExecContext(ctx, "DELETE FROM fgraph_fts WHERE rowid=?", fact.id); err != nil {
				return TxReport{}, wrap(ErrFormat, err, "cannot remove fact %d from full-text search", fact.id)
			}
		}
		rx := tx
		rendered, renderErr := db.renderRaw(ctx, runner, fact, &rx)
		if renderErr != nil {
			return TxReport{}, renderErr
		}
		report.Retracted = append(report.Retracted, rendered)
	}
	if err := db.validateTouchedShapes(ctx, runner, plan); err != nil {
		return TxReport{}, err
	}
	for _, hash := range garbageCandidates {
		if _, err := runner.ExecContext(ctx, `DELETE FROM fgraph_blobs WHERE hash=? AND NOT EXISTS (
			SELECT 1 FROM fgraph_facts WHERE t IN (7,8,9) AND v=?
		)`, hash, hash); err != nil {
			return TxReport{}, wrap(ErrFormat, err, "cannot garbage-collect orphaned blob %x", hash)
		}
	}
	if err := alloc.flush(ctx); err != nil {
		return TxReport{}, err
	}
	eventRecord, recordErr := db.exportTransaction(ctx, runner, tx, at)
	if recordErr != nil {
		return TxReport{}, recordErr
	}
	eventData, eventHash, hashErr := canonicalEventData(eventRecord)
	if hashErr != nil {
		return TxReport{}, hashErr
	}
	if config.eventHash != nil {
		if !bytes.Equal(eventHash[:], config.eventHash[:]) {
			return TxReport{}, fail(ErrConflict, "event %s payload does not reproduce its canonical event hash", eventID)
		}
		eventHash = *config.eventHash
	}
	var operationID any
	var requestDigest any
	if config.operationID != nil {
		operationID = *config.operationID
		requestDigest = requestHash[:]
	}
	if _, err := runner.ExecContext(ctx, "INSERT INTO fgraph_events(tx,event_hash,event_data,operation_id,request_hash) VALUES (?,?,?,?,?)", tx, eventHash[:], eventData, operationID, requestDigest); err != nil {
		return TxReport{}, wrap(ErrConflict, err, "cannot record event %s; retry with the same operation id if supplied", eventID)
	}
	return report, nil
}

func compactPendingAllocations(
	ctx context.Context,
	runner sqlRunner,
	plan *transactionPlan,
	assertions []plannedFact,
	txFacts []plannedFact,
) error {
	alloc := plan.allocator
	first, next := alloc.first, alloc.next
	if first == next {
		return nil
	}
	kept := map[int64]bool{}
	for _, id := range alloc.ids {
		if id >= first && id < next {
			kept[id] = true
		}
	}
	for _, assertion := range assertions {
		if assertion.e >= first && assertion.e < next {
			kept[assertion.e] = true
		}
		if assertion.a >= first && assertion.a < next {
			kept[assertion.a] = true
		}
		if assertion.value.tag == TagRef {
			ref := asInt64(assertion.value.storage)
			if ref >= first && ref < next {
				kept[ref] = true
			}
		}
	}
	for _, fact := range txFacts {
		if fact.a >= first && fact.a < next {
			kept[fact.a] = true
		}
		if fact.value.tag == TagRef {
			ref := asInt64(fact.value.storage)
			if ref >= first && ref < next {
				kept[ref] = true
			}
		}
	}
	ordered := make([]int64, 0, len(kept))
	for id := range kept {
		ordered = append(ordered, id)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	remap := make(map[int64]int64, len(ordered))
	changed := len(ordered) != int(next-first)
	for index, old := range ordered {
		newID := first + int64(index)
		remap[old] = newID
		changed = changed || old != newID
	}
	if !changed {
		return nil
	}
	for _, old := range ordered {
		newID := remap[old]
		if old == newID {
			continue
		}
		if _, err := runner.ExecContext(ctx, "UPDATE fgraph_ids SET id=? WHERE id=?", newID, old); err != nil {
			return wrap(ErrFormat, err, "cannot compact pending identity %d to %d", old, newID)
		}
	}
	for index := range assertions {
		assertion := &assertions[index]
		assertion.e = remapPendingID(assertion.e, remap)
		assertion.a = remapPendingID(assertion.a, remap)
		if assertion.value.tag == TagRef {
			if ref, exists := remap[asInt64(assertion.value.storage)]; exists {
				assertion.value.storage = ref
				assertion.value.logical = ref
			}
		}
	}
	for index := range txFacts {
		fact := &txFacts[index]
		fact.a = remapPendingID(fact.a, remap)
		if fact.value.tag == TagRef {
			if ref, exists := remap[asInt64(fact.value.storage)]; exists {
				fact.value.storage = ref
				fact.value.logical = ref
			}
		}
	}
	for name, id := range alloc.ids {
		alloc.ids[name] = remapPendingID(id, remap)
	}
	pending := alloc.pending[:0]
	for _, identity := range alloc.pending {
		_, exists := remap[identity.id]
		if !exists && identity.id >= first && identity.id < next {
			// Named identities are always retained through alloc.ids; this path is
			// therefore limited to anonymous allocations canceled by later input.
			continue
		}
		identity.id = remapPendingID(identity.id, remap)
		pending = append(pending, identity)
	}
	alloc.pending = pending
	for name, id := range plan.tempids {
		mapped, exists := remap[id]
		if !exists && id >= first && id < next {
			delete(plan.tempids, name)
			continue
		}
		if exists {
			plan.tempids[name] = mapped
		}
	}
	alloc.next = first + int64(len(ordered))
	alloc.dirty = alloc.next != first
	return nil
}

func remapPendingID(id int64, remap map[int64]int64) int64 {
	if mapped, exists := remap[id]; exists {
		return mapped
	}
	return id
}

func (db *DB) validatePlannedDeclarations(ctx context.Context, runner sqlRunner, plan *transactionPlan) error {
	assertions := activePlanAssertions(plan)
	declarations := map[int64]*declareOptions{}
	for _, fact := range assertions {
		if fact.a < 5 || (fact.a > 10 && fact.a != 14) {
			continue
		}
		config := declarations[fact.e]
		if config == nil {
			config = &declareOptions{}
			declarations[fact.e] = config
		}
		switch fact.a {
		case 5:
			value := sqliteBool(fact.value.storage)
			config.many = &value
		case 6:
			value := sqliteBool(fact.value.storage)
			config.unique = &value
		case 7:
			value := sqliteBool(fact.value.storage)
			config.nohistory = &value
		case 8:
			value, ok := fact.value.storage.(string)
			if !ok {
				return fail(ErrFormat, "planned schema type for entity %d has type %T", fact.e, fact.value.storage)
			}
			config.typeName = &value
		case 9:
			value := asInt64(fact.value.storage)
			config.dims = &value
		case 10:
			value, ok := fact.value.logical.(string)
			if !ok {
				return fail(ErrFormat, "planned schema documentation for entity %d has type %T", fact.e, fact.value.logical)
			}
			config.doc = &value
		case 14:
			value, ok := fact.value.logical.(string)
			if !ok {
				return fail(ErrFormat, "planned vector model for entity %d has type %T", fact.e, fact.value.logical)
			}
			config.vectorModel = &value
		}
	}
	for _, request := range plan.retracts {
		if request.missing {
			continue
		}
		if request.a == nil && !attributePattern.MatchString(db.declarationEntityName(request.e, plan.allocator)) {
			continue
		}
		if request.a != nil && (*request.a < 5 || (*request.a > 10 && *request.a != 14)) {
			continue
		}
		if _, exists := declarations[request.e]; !exists {
			declarations[request.e] = &declareOptions{}
		}
	}
	entities := make([]int64, 0, len(declarations))
	for entity := range declarations {
		entities = append(entities, entity)
	}
	sort.Slice(entities, func(i, j int) bool { return entities[i] < entities[j] })
	for _, entity := range entities {
		name := db.declarationEntityName(entity, plan.allocator)
		if err := validateDeclarationOptions(name, *declarations[entity]); err != nil {
			return err
		}
		schema, err := db.finalSchemaForPlan(ctx, runner, entity, plan, assertions)
		if err != nil {
			return err
		}
		if err := db.validateFinalAttribute(ctx, runner, entity, name, schema, plan, assertions); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) finalSchemaForPlan(
	ctx context.Context,
	runner sqlRunner,
	entity int64,
	plan *transactionPlan,
	assertions []plannedFact,
) (attributeSchema, error) {
	rows, err := runner.QueryContext(ctx, `SELECT id,e,a,v,t,tx,rx FROM fgraph_facts
		WHERE e=? AND (a BETWEEN 5 AND 10 OR a=14) AND rx IS NULL ORDER BY id`, entity)
	if err != nil {
		return attributeSchema{}, wrap(ErrFormat, err, "cannot read final schema for attribute %d", entity)
	}
	facts, err := scanRawFacts(rows)
	if err != nil {
		return attributeSchema{}, err
	}
	schema := attributeSchema{}
	for _, fact := range facts {
		if rawRetracted(plan.retracts, fact) {
			continue
		}
		if err := applySchemaFact(&schema, fact.a, fact.v); err != nil {
			return attributeSchema{}, err
		}
	}
	for _, fact := range assertions {
		if fact.e == entity && (fact.a >= 5 && fact.a <= 10 || fact.a == 14) {
			if err := applySchemaFact(&schema, fact.a, fact.value.storage); err != nil {
				return attributeSchema{}, err
			}
		}
	}
	return schema, nil
}

type schemaValidationFact struct {
	value   storedValue
	e       int64
	planned bool
}

func (db *DB) validateFinalAttribute(
	ctx context.Context,
	runner sqlRunner,
	attrID int64,
	attr string,
	schema attributeSchema,
	plan *transactionPlan,
	assertions []plannedFact,
) error {
	if schema.unique && (schema.typeName == "" || schema.typeName == "json" || schema.typeName == "vector") {
		return fail(ErrSchema, "%q must declare a scalar non-json, non-vector type while unique is enabled", attr)
	}
	rows, err := runner.QueryContext(ctx, `SELECT id,e,a,v,t,tx,rx FROM fgraph_facts
		WHERE a=? AND rx IS NULL ORDER BY id`, attrID)
	if err != nil {
		return wrap(ErrFormat, err, "cannot read final values for declared attribute %q", attr)
	}
	rawFacts, err := scanRawFacts(rows)
	if err != nil {
		return err
	}
	facts := make([]schemaValidationFact, 0, len(rawFacts)+len(assertions))
	for _, fact := range rawFacts {
		if rawRetracted(plan.retracts, fact) {
			continue
		}
		facts = append(facts, schemaValidationFact{
			e:     fact.e,
			value: storedValue{storage: fact.v, tag: fact.t},
		})
	}
	seenPlanned := map[[2]int64]storedValue{}
	for _, assertion := range assertions {
		if assertion.a != attrID {
			continue
		}
		key := [2]int64{assertion.e, assertion.a}
		if prior, exists := seenPlanned[key]; exists && !schema.many && !storedEqual(prior, assertion.value) {
			return fail(ErrConflict, "%q is cardinality one but the transaction leaves several values on entity %d", attr, assertion.e)
		}
		seenPlanned[key] = assertion.value
		if !schema.many {
			kept := facts[:0]
			for _, fact := range facts {
				if fact.e != assertion.e {
					kept = append(kept, fact)
				}
			}
			facts = kept
		}
		duplicate := false
		for _, fact := range facts {
			if fact.e == assertion.e && storedEqual(fact.value, assertion.value) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			facts = append(facts, schemaValidationFact{e: assertion.e, value: assertion.value, planned: true})
		}
	}
	type schemaOwner struct {
		entity  int64
		planned bool
	}
	owners := map[string]schemaOwner{}
	counts := map[int64]int{}
	plannedCounts := map[int64]int{}
	for index := range facts {
		fact := &facts[index]
		if schema.typeName != "" && !tagCompatible(schema.typeName, fact.value.tag) {
			return fail(ErrSchema, "%q has final live values incompatible with declared type %s", attr, schema.typeName)
		}
		counts[fact.e]++
		if fact.planned {
			plannedCounts[fact.e]++
		}
		if !schema.many && counts[fact.e] > 1 {
			if plannedCounts[fact.e] > 0 {
				return fail(ErrConflict, "%q leaves several values on entity %d while many is disabled", attr, fact.e)
			}
			return fail(ErrSchema, "%q has final entities with several live values while many is disabled", attr)
		}
		if schema.unique {
			key := fmt.Sprintf("%d:%v", fact.value.tag, storageKey(fact.value.storage))
			if owner, exists := owners[key]; exists && owner.entity != fact.e {
				if owner.planned || fact.planned {
					return fail(ErrConflict, "%q leaves duplicate live values while unique is enabled", attr)
				}
				return fail(ErrSchema, "%q has duplicate final live values while unique is enabled", attr)
			}
			owners[key] = schemaOwner{entity: fact.e, planned: fact.planned}
		}
		if schema.dimsSet && fact.value.tag == TagVector {
			logical := fact.value.logical
			if logical == nil {
				logical, err = db.logicalValue(ctx, runner, fact.value.storage, fact.value.tag)
				if err != nil {
					return err
				}
			}
			vector, ok := logical.([]float32)
			if !ok || int64(len(vector)) != schema.dims {
				return fail(ErrSchema, "%q has final vectors that do not match declared dimensions %d", attr, schema.dims)
			}
		}
	}
	return nil
}

func (db *DB) declarationEntityName(entity int64, alloc *allocator) string {
	for name, id := range alloc.ids {
		if id == entity {
			return name
		}
	}
	for name, id := range db.store.names {
		if id == entity {
			return name
		}
	}
	return fmt.Sprintf("entity:%d", entity)
}

func reportIDs(alloc *allocator, plan *transactionPlan) map[string]int64 {
	ids := make(map[string]int64, len(alloc.ids)+len(plan.tempids))
	for name, id := range alloc.ids {
		if !attributePattern.MatchString(name) {
			ids[name] = id
		}
	}
	for name, id := range plan.tempids {
		ids[name] = id
	}
	return ids
}

func (db *DB) planData(ctx context.Context, runner sqlRunner, plan *transactionPlan, data any) error {
	if fields, ok := objectFields(data); ok {
		if len(fields) == 0 || identityOnlyTempID(fields) {
			return nil
		}
		_, err := db.planMap(ctx, runner, plan, fields)
		return err
	}
	items, ok := data.([]any)
	if !ok {
		return fail(ErrType, "transaction has type %T; use a map, operation, or array of them", data)
	}
	if isOperation(items) {
		return db.planOperation(ctx, runner, plan, items)
	}
	for _, item := range items {
		if fields, itemOK := objectFields(item); itemOK {
			if len(fields) == 0 || identityOnlyTempID(fields) {
				continue
			}
			if _, err := db.planMap(ctx, runner, plan, fields); err != nil {
				return err
			}
			continue
		}
		op, itemOK := item.([]any)
		if !itemOK || !isOperation(op) {
			return fail(ErrType, "transaction item has type %T; use a map or assert/retract operation", item)
		}
		if err := db.planOperation(ctx, runner, plan, op); err != nil {
			return err
		}
	}
	return nil
}

func identityOnlyTempID(fields []Field) bool {
	if len(fields) != 1 || fields[0].Name != "id" {
		return false
	}
	if value, ok := fields[0].Value.(TempID); ok {
		return value != ""
	}
	tempFields, ok := objectFields(fields[0].Value)
	if !ok || len(tempFields) != 1 || tempFields[0].Name != "tmp" {
		return false
	}
	name, nameOK := tempFields[0].Value.(string)
	return nameOK && name != ""
}

func isOperation(items []any) bool {
	if len(items) == 0 {
		return false
	}
	name, ok := items[0].(string)
	return ok && (name == "assert" || name == "retract" || name == "cas")
}

func (db *DB) planMap(ctx context.Context, runner sqlRunner, plan *transactionPlan, fields []Field) (int64, error) {
	var idSpec any
	hasID := false
	for _, field := range fields {
		if field.Name == "id" {
			idSpec, hasID = field.Value, true
			break
		}
	}
	prepared := map[int]preparedMapField{}
	var entity int64
	var pinned bool
	var err error
	if hasID {
		entity, pinned, err = db.resolveWriteEntity(ctx, runner, plan, idSpec, true)
	} else {
		var found bool
		entity, found, prepared, err = db.upsertOwnerForMap(ctx, runner, plan, fields)
		if err == nil && !found {
			entity, err = plan.allocator.anonymous()
		}
	}
	if err != nil {
		return 0, err
	}
	start := len(plan.assertions)
	for index, field := range fields {
		if field.Name == "id" {
			continue
		}
		item, ready := prepared[index]
		if !ready {
			item.attrID, _, err = plan.allocator.name(ctx, field.Name, true, true)
			if err != nil {
				return 0, err
			}
			item.schema, err = db.schemaForPlan(ctx, runner, item.attrID, plan)
			if err != nil {
				return 0, err
			}
			if _, isArray := field.Value.([]any); isArray && !item.schema.many {
				return 0, fail(ErrConflict, "%q holds one value per entity; declare it many or wrap a literal array with {\"json\":...}", field.Name)
			}
			item.values, err = db.expandAttributeValue(ctx, runner, plan, field.Name, item.schema, field.Value)
			if err != nil {
				return 0, err
			}
		}
		if len(item.values) > 1 && !item.schema.many {
			return 0, fail(ErrConflict, "%q holds one value per entity; declare it many to hold several", field.Name)
		}
		for _, value := range item.values {
			if item.schema.unique {
				owner, found, ownerErr := db.uniqueOwnerIncludingPlan(ctx, runner, plan, item.attrID, value)
				if ownerErr != nil {
					return 0, ownerErr
				}
				if found && owner != entity {
					if pinned {
						return 0, fail(ErrConflict, "%q value %v already belongs to entity %d; omit the pinned id to upsert", field.Name, value.logical, owner)
					}
					for i := start; i < len(plan.assertions); i++ {
						plan.assertions[i].e = owner
					}
					for temp, tempID := range plan.tempids {
						if tempID == entity {
							plan.tempids[temp] = owner
						}
					}
					entity = owner
				}
			}
			if value.tag == TagVector && !item.schema.dimsSet {
				dims, dimsErr := vectorDimensions(value, field.Name)
				if dimsErr != nil {
					return 0, dimsErr
				}
				if err := db.ensureVectorSchema(plan, item.attrID, field.Name, dims, item.schema.typeName == ""); err != nil {
					return 0, err
				}
				item.schema.dims, item.schema.dimsSet = dims, true
			}
			plan.order++
			appendAssertion(plan, plannedFact{e: entity, a: item.attrID, attr: field.Name, value: value, order: plan.order})
		}
	}
	if err := db.validateUserWriteTarget(ctx, runner, entity); err != nil {
		return 0, err
	}
	return entity, nil
}

func (db *DB) upsertOwnerForMap(
	ctx context.Context,
	runner sqlRunner,
	plan *transactionPlan,
	fields []Field,
) (int64, bool, map[int]preparedMapField, error) {
	prepared := map[int]preparedMapField{}
	var owner int64
	ownerFound := false
	for index, field := range fields {
		if field.Name == "id" {
			continue
		}
		attrID, found, err := plan.allocator.name(ctx, field.Name, true, false)
		if err != nil {
			return 0, false, nil, err
		}
		if !found {
			continue
		}
		schema, err := db.schemaForPlan(ctx, runner, attrID, plan)
		if err != nil {
			return 0, false, nil, err
		}
		if !schema.unique {
			continue
		}
		if _, isArray := field.Value.([]any); isArray && !schema.many {
			return 0, false, nil, fail(ErrConflict, "%q holds one value per entity; declare it many or wrap a literal array with {\"json\":...}", field.Name)
		}
		values, err := db.expandAttributeValue(ctx, runner, plan, field.Name, schema, field.Value)
		if err != nil {
			return 0, false, nil, err
		}
		prepared[index] = preparedMapField{attrID: attrID, schema: schema, values: values}
		for _, value := range values {
			candidate, found, err := db.uniqueOwnerIncludingPlan(ctx, runner, plan, attrID, value)
			if err != nil {
				return 0, false, nil, err
			}
			if !found {
				continue
			}
			if ownerFound && owner != candidate {
				return 0, false, nil, fail(ErrConflict, "unpinned map unique values belong to entities %d and %d; split or correct the map", owner, candidate)
			}
			owner, ownerFound = candidate, true
		}
	}
	return owner, ownerFound, prepared, nil
}

func (db *DB) uniqueOwnerIncludingPlan(
	ctx context.Context,
	runner sqlRunner,
	plan *transactionPlan,
	attr int64,
	value storedValue,
) (int64, bool, error) {
	for _, assertion := range plan.assertions {
		if assertion.a == attr && storedEqual(assertion.value, value) {
			return assertion.e, true, nil
		}
	}
	return db.uniqueOwner(ctx, runner, attr, value)
}

func (db *DB) validateUserWriteTarget(ctx context.Context, runner sqlRunner, entity int64) error {
	if entity <= GenesisTx {
		return fail(ErrUnsupported, "entity %d is reserved for fgraph system data; write application entities instead", entity)
	}
	var transaction int
	if err := runner.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM fgraph_facts WHERE e=? AND a=1 AND tx=e)", entity).Scan(&transaction); err != nil {
		return wrap(ErrFormat, err, "cannot classify write target %d", entity)
	}
	if transaction != 0 {
		return fail(ErrUnsupported, "transaction entity %d is an immutable audit receipt; attach metadata when creating the transaction", entity)
	}
	return nil
}

func (db *DB) resolveWriteEntity(ctx context.Context, runner sqlRunner, plan *transactionPlan, spec any, present bool) (int64, bool, error) {
	if !present {
		id, err := plan.allocator.anonymous()
		return id, false, err
	}
	switch value := spec.(type) {
	case string:
		id, _, err := plan.allocator.name(ctx, value, false, true)
		return id, true, err
	case int:
		return db.resolvePinnedWriteID(ctx, runner, int64(value))
	case int64:
		return db.resolvePinnedWriteID(ctx, runner, value)
	case TempID:
		id, err := db.tempEntity(plan, string(value))
		return id, false, err
	case []any:
		id, found, err := db.resolveLookup(ctx, runner, plan, value, true)
		if err != nil {
			return 0, false, err
		}
		if !found {
			allocated, allocErr := plan.allocator.anonymous()
			return allocated, false, allocErr
		}
		return id, false, nil
	default:
		fields, ok := objectFields(value)
		if ok && len(fields) == 1 && fields[0].Name == "tmp" {
			name, nameOK := fields[0].Value.(string)
			if !nameOK || name == "" {
				return 0, false, fail(ErrType, "tempid must be a non-empty string; use {\"tmp\":\"name\"}")
			}
			id, err := db.tempEntity(plan, name)
			return id, false, err
		}
		return 0, false, fail(ErrType, "entity id has type %T; use a name, positive int64, unique lookup, or tempid", spec)
	}
}

func (db *DB) resolvePinnedWriteID(ctx context.Context, runner sqlRunner, value int64) (int64, bool, error) {
	if value < 1 {
		return 0, true, fail(ErrType, "entity id %d is invalid; use a positive integer", value)
	}
	id, found, err := db.resolveNumericEntity(ctx, runner, value)
	if err != nil {
		return 0, true, err
	}
	if !found {
		return 0, true, fail(ErrNotFound, "entity id %d does not exist; use a name, tempid, or known id", value)
	}
	return id, true, nil
}

func (db *DB) tempEntity(plan *transactionPlan, name string) (int64, error) {
	if id, ok := plan.tempids[name]; ok {
		return id, nil
	}
	id, err := plan.allocator.anonymous()
	if err != nil {
		return 0, err
	}
	plan.tempids[name] = id
	return id, nil
}

func (db *DB) resolveLookup(ctx context.Context, runner sqlRunner, plan *transactionPlan, lookup []any, write bool) (int64, bool, error) {
	if len(lookup) != 2 {
		return 0, false, fail(ErrType, "lookup has %d items; use [\"unique/attribute\", value]", len(lookup))
	}
	attr, ok := lookup[0].(string)
	if !ok {
		return 0, false, fail(ErrType, "lookup attribute has type %T; use an attribute name", lookup[0])
	}
	attrID, found, err := plan.allocator.name(ctx, attr, true, false)
	if err != nil || !found {
		if err != nil {
			return 0, false, err
		}
		if write {
			return 0, false, fail(ErrNotFound, "lookup attribute %q does not exist; declare it unique before using a lookup", attr)
		}
		return 0, false, nil
	}
	schema, err := db.schemaForPlan(ctx, runner, attrID, plan)
	if err != nil {
		return 0, false, err
	}
	if !schema.unique {
		return 0, false, fail(ErrSchema, "lookup attribute %q is not unique; declare it unique first", attr)
	}
	var value storedValue
	if write {
		value, err = db.resolveFactValue(ctx, runner, plan, attr, schema, lookup[1])
	} else {
		value, err = db.resolveReadFactValue(ctx, runner, attr, schema, lookup[1])
	}
	if err != nil {
		return 0, false, err
	}
	return db.uniqueOwner(ctx, runner, attrID, value)
}

func (db *DB) expandAttributeValue(ctx context.Context, runner sqlRunner, plan *transactionPlan, attr string, schema attributeSchema, input any) ([]storedValue, error) {
	if items, ok := input.([]any); ok {
		values := make([]storedValue, 0, len(items))
		for _, item := range items {
			value, err := db.expandSingleAttributeValue(ctx, runner, plan, attr, schema, item)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	}
	value, err := db.expandSingleAttributeValue(ctx, runner, plan, attr, schema, input)
	if err != nil {
		return nil, err
	}
	return []storedValue{value}, nil
}

func (db *DB) expandSingleAttributeValue(ctx context.Context, runner sqlRunner, plan *transactionPlan, attr string, schema attributeSchema, input any) (storedValue, error) {
	if _, ok := objectFields(input); ok {
		fields, _ := objectFields(input)
		if len(fields) != 1 || !isValueWrapper(fields[0].Name) {
			if schema.typeName != "ref" {
				return storedValue{}, fail(ErrType, "nested map on %q requires ref type; declare the attribute as ref or wrap literal data with {\"json\":...}", attr)
			}
			child, err := db.planMap(ctx, runner, plan, fields)
			if err != nil {
				return storedValue{}, err
			}
			return storedValue{logical: child, storage: child, tag: TagRef}, nil
		}
	}
	return db.resolveFactValue(ctx, runner, plan, attr, schema, input)
}

func isValueWrapper(name string) bool {
	switch name {
	case "ref", "instant", "bytes", "vector", "json", "tmp":
		return true
	default:
		return false
	}
}

func (db *DB) resolveFactValue(ctx context.Context, runner sqlRunner, plan *transactionPlan, attr string, schema attributeSchema, input any) (storedValue, error) {
	value, err := scalarValue(input)
	if err != nil {
		return storedValue{}, err
	}
	if value.tag == TagRef {
		ref, ok := value.logical.(RefValue)
		if !ok {
			return storedValue{}, fail(ErrType, "%q reference is malformed; use {\"ref\": name-or-id}", attr)
		}
		id, found, resolveErr := db.resolveReference(ctx, runner, plan, ref.Target, true)
		if resolveErr != nil {
			return storedValue{}, resolveErr
		}
		if !found {
			return storedValue{}, fail(ErrNotFound, "%q reference target %v does not exist; name it or assert it first", attr, ref.Target)
		}
		value.logical, value.storage = id, id
	}
	if schema.typeName != "" && !tagCompatible(schema.typeName, value.tag) {
		return storedValue{}, fail(ErrType, "%q requires %s values, got %s; use the matching typed wrapper or change its declaration", attr, schema.typeName, tagNames[value.tag])
	}
	if value.tag == TagVector {
		dims, dimsErr := vectorDimensions(value, attr)
		if dimsErr != nil {
			return storedValue{}, dimsErr
		}
		if schema.dimsSet && dims != schema.dims {
			return storedValue{}, fail(ErrType, "%q requires %d vector dimensions, got %d; provide the fixed dimension", attr, schema.dims, dims)
		}
	}
	return value, nil
}

func (db *DB) resolveReadFactValue(
	ctx context.Context,
	runner sqlRunner,
	attr string,
	schema attributeSchema,
	input any,
) (storedValue, error) {
	value, err := scalarValue(input)
	if err != nil {
		return storedValue{}, err
	}
	if value.tag == TagRef {
		ref, ok := value.logical.(RefValue)
		if !ok {
			return storedValue{}, fail(ErrType, "%q reference is malformed; use {\"ref\": name-or-id}", attr)
		}
		id, found, resolveErr := db.resolveReadEntity(ctx, runner, ref.Target)
		if resolveErr != nil {
			return storedValue{}, resolveErr
		}
		if !found {
			return storedValue{}, fail(ErrNotFound, "%q reference target %v does not exist; use a known entity", attr, ref.Target)
		}
		value.logical, value.storage = id, id
	}
	if schema.typeName != "" && !tagCompatible(schema.typeName, value.tag) {
		return storedValue{}, fail(ErrType, "%q requires %s values, got %s; use the matching typed wrapper", attr, schema.typeName, tagNames[value.tag])
	}
	if value.tag == TagVector {
		dims, dimsErr := vectorDimensions(value, attr)
		if dimsErr != nil {
			return storedValue{}, dimsErr
		}
		if schema.dimsSet && dims != schema.dims {
			return storedValue{}, fail(ErrType, "%q requires %d vector dimensions, got %d; provide the fixed dimension", attr, schema.dims, dims)
		}
	}
	return value, nil
}

func vectorDimensions(value storedValue, attr string) (int64, error) {
	vector, ok := value.logical.([]float32)
	if !ok {
		return 0, fail(ErrType, "%q vector is malformed; use {\"vector\":[finite numbers]}", attr)
	}
	return int64(len(vector)), nil
}

func (db *DB) resolveReference(ctx context.Context, runner sqlRunner, plan *transactionPlan, target any, create bool) (int64, bool, error) {
	switch target := target.(type) {
	case string:
		return plan.allocator.name(ctx, target, false, create)
	case int:
		if target < 1 {
			return 0, false, fail(ErrType, "reference id %d is invalid; use a positive integer", target)
		}
		return db.resolveNumericEntity(ctx, runner, int64(target))
	case int64:
		if target < 1 {
			return 0, false, fail(ErrType, "reference id %d is invalid; use a positive int64", target)
		}
		return db.resolveNumericEntity(ctx, runner, target)
	case []any:
		return db.resolveLookup(ctx, runner, plan, target, create)
	case TempID:
		id, err := db.tempEntity(plan, string(target))
		return id, err == nil, err
	default:
		fields, ok := objectFields(target)
		if ok && len(fields) == 1 && fields[0].Name == "tmp" {
			name, nameOK := fields[0].Value.(string)
			if !nameOK {
				return 0, false, fail(ErrType, "reference tempid has type %T; use a string", fields[0].Value)
			}
			id, err := db.tempEntity(plan, name)
			return id, err == nil, err
		}
		return 0, false, fail(ErrType, "reference target has type %T; use a name, id, unique lookup, or tempid", target)
	}
}

func (db *DB) ensureVectorSchema(plan *transactionPlan, attrID int64, attr string, dims int64, inferType bool) error {
	if inferType {
		found := false
		for _, assertion := range plan.assertions {
			if assertion.e == attrID && assertion.a == 8 {
				if assertion.value.storage != "vector" {
					return fail(ErrConflict, "%q receives a vector after declaring a different type in one transaction", attr)
				}
				found = true
				break
			}
		}
		if !found {
			plan.order++
			appendAssertion(plan, plannedFact{
				e: attrID, a: 8, attr: systemNames[8],
				value: storedValue{logical: "vector", storage: "vector", tag: TagText}, order: plan.order,
			})
		}
	}
	return db.ensureVectorDims(plan, attrID, attr, dims)
}

// ensureVectorDims retains the narrow dimension helper used by internal tests;
// transaction planning calls it after applying any inferred vector type.
func (db *DB) ensureVectorDims(plan *transactionPlan, attrID int64, attr string, dims int64) error {
	for _, assertion := range plan.assertions {
		if assertion.e == attrID && assertion.a == 9 {
			if assertion.value.storage != dims {
				return fail(ErrConflict, "%q receives vectors with different dimensions in one transaction; use one fixed dimension", attr)
			}
			return nil
		}
	}
	plan.order++
	appendAssertion(plan, plannedFact{
		e: attrID, a: 9, attr: systemNames[9],
		value: storedValue{logical: dims, storage: dims, tag: TagInt}, order: plan.order,
	})
	return nil
}

func (db *DB) planOperation(ctx context.Context, runner sqlRunner, plan *transactionPlan, op []any) error {
	name, ok := op[0].(string)
	if !ok {
		return fail(ErrType, "operation name has type %T; use assert or retract", op[0])
	}
	switch name {
	case "assert":
		if len(op) != 4 {
			return fail(ErrType, "assert operation has %d items; use [\"assert\", entity, attribute, value]", len(op))
		}
		entity, found, err := db.resolveOperationEntity(ctx, runner, plan, op[1], true)
		if err != nil || !found {
			return err
		}
		if validateErr := db.validateUserWriteTarget(ctx, runner, entity); validateErr != nil {
			return validateErr
		}
		attr, ok := op[2].(string)
		if !ok {
			return fail(ErrType, "assert attribute has type %T; use an attribute name", op[2])
		}
		attrID, _, err := plan.allocator.name(ctx, attr, true, true)
		if err != nil {
			return err
		}
		schema, err := db.schemaForPlan(ctx, runner, attrID, plan)
		if err != nil {
			return err
		}
		value, err := db.resolveFactValue(ctx, runner, plan, attr, schema, op[3])
		if err != nil {
			return err
		}
		if value.tag == TagVector && !schema.dimsSet {
			dims, dimsErr := vectorDimensions(value, attr)
			if dimsErr != nil {
				return dimsErr
			}
			if err := db.ensureVectorSchema(plan, attrID, attr, dims, schema.typeName == ""); err != nil {
				return err
			}
		}
		plan.order++
		appendAssertion(plan, plannedFact{e: entity, a: attrID, attr: attr, value: value, order: plan.order})
		return nil
	case "retract":
		if len(op) < 2 || len(op) > 4 {
			return fail(ErrType, "retract operation has %d items; use [\"retract\", entity, attribute?, value?]", len(op))
		}
		entity, found, err := db.resolveOperationEntity(ctx, runner, plan, op[1], false)
		if err != nil {
			return err
		}
		if !found {
			appendRetraction(plan, retractRequest{missing: true})
			return nil
		}
		if err := db.validateUserWriteTarget(ctx, runner, entity); err != nil {
			return err
		}
		request := retractRequest{e: entity}
		if len(op) >= 3 {
			attr, ok := op[2].(string)
			if !ok {
				return fail(ErrType, "retract attribute has type %T; use an attribute name", op[2])
			}
			attrID, attrFound, attrErr := plan.allocator.name(ctx, attr, true, false)
			if attrErr != nil {
				return attrErr
			}
			if !attrFound {
				appendRetraction(plan, retractRequest{missing: true})
				return nil
			}
			request.a, request.attr = &attrID, attr
			if len(op) == 4 {
				schema, schemaErr := db.schemaForPlan(ctx, runner, attrID, plan)
				if schemaErr != nil {
					return schemaErr
				}
				value, valueErr := db.resolveFactValue(ctx, runner, plan, attr, schema, op[3])
				if valueErr != nil {
					return valueErr
				}
				request.value = &value
			}
		}
		appendRetraction(plan, request)
		return nil
	case "cas":
		return db.planCAS(ctx, runner, plan, op)
	default:
		return fail(ErrType, "unknown operation %q; use assert, retract, or cas", name)
	}
}

func (db *DB) planCAS(ctx context.Context, runner sqlRunner, plan *transactionPlan, op []any) error {
	if len(op) != 5 {
		return fail(ErrType, "cas operation has %d items; use [\"cas\", entity, attribute, expected, replacement]", len(op))
	}
	entity, found, err := db.resolveOperationEntity(ctx, runner, plan, op[1], false)
	if err != nil {
		return err
	}
	if !found {
		return fail(ErrNotFound, "cas entity %v does not exist", op[1])
	}
	if validateErr := db.validateUserWriteTarget(ctx, runner, entity); validateErr != nil {
		return validateErr
	}
	attr, ok := op[2].(string)
	if !ok {
		return fail(ErrType, "cas attribute has type %T; use an attribute name", op[2])
	}
	attrID, found, err := plan.allocator.name(ctx, attr, true, false)
	if err != nil {
		return err
	}
	if !found {
		return fail(ErrNotFound, "cas attribute %q does not exist", attr)
	}
	schema, err := db.schemaForPlan(ctx, runner, attrID, plan)
	if err != nil {
		return err
	}
	if schema.many {
		return fail(ErrSchema, "cas requires a cardinality-one attribute; %q is many", attr)
	}
	target := fmt.Sprintf("%d/%d", entity, attrID)
	if plan.casTargets == nil {
		plan.casTargets = map[string]bool{}
	}
	if plan.casTargets[target] {
		return fail(ErrConflict, "cas target cannot be compared more than once in the same transaction")
	}
	// Track the target independently of its datom delta: missing-to-missing CAS
	// still owns this attribute for transaction-isolation purposes.
	plan.casTargets[target] = true
	current, err := db.liveFacts(ctx, runner, entity, &attrID)
	if err != nil {
		return err
	}
	if len(current) > 1 {
		return fail(ErrFormat, "cardinality-one attribute %q has %d live values; run doctor", attr, len(current))
	}
	missing, err := missingCASValue(op[3])
	if err != nil {
		return err
	}
	if missing {
		if len(current) != 0 {
			return fail(ErrConflict, "cas expected %q to be missing on entity %v", attr, op[1])
		}
	} else {
		expected, valueErr := db.resolveFactValue(ctx, runner, plan, attr, schema, op[3])
		if valueErr != nil {
			return valueErr
		}
		if len(current) != 1 || current[0].t != expected.tag || !storageEqual(current[0].v, expected.storage) {
			return fail(ErrConflict, "cas expected value does not match %q on entity %v", attr, op[1])
		}
	}
	desiredMissing, err := missingCASValue(op[4])
	if err != nil {
		return err
	}
	var replacement storedValue
	if !desiredMissing {
		replacement, err = db.resolveFactValue(ctx, runner, plan, attr, schema, op[4])
		if err != nil {
			return err
		}
	}
	if len(current) == 1 {
		request := retractRequest{e: entity, a: &attrID, attr: attr, cas: true}
		appendRetraction(plan, request)
	}
	if !desiredMissing {
		plan.order++
		appendAssertion(plan, plannedFact{
			e: entity, a: attrID, attr: attr, value: replacement, order: plan.order, cas: true,
		})
	}
	return nil
}

func missingCASValue(value any) (bool, error) {
	fields, ok := objectFields(value)
	if !ok {
		return false, nil
	}
	if len(fields) == 1 && fields[0].Name == "missing" {
		missing, typeOK := fields[0].Value.(bool)
		if !typeOK || !missing {
			return false, fail(ErrType, "cas missing sentinel must be {\"missing\":true}")
		}
		return true, nil
	}
	return false, nil
}

func validateCASIsolation(plan *transactionPlan) error {
	casKeys := map[string]bool{}
	for key := range plan.casTargets {
		casKeys[key] = true
	}
	for _, assertion := range plan.assertions {
		if assertion.cas {
			casKeys[fmt.Sprintf("%d/%d", assertion.e, assertion.a)] = true
		}
	}
	for _, assertion := range plan.assertions {
		if !assertion.cas && casKeys[fmt.Sprintf("%d/%d", assertion.e, assertion.a)] {
			return fail(ErrConflict, "cas target cannot be changed by another operation in the same transaction")
		}
	}
	for _, request := range plan.retracts {
		if request.cas {
			continue
		}
		if request.a == nil {
			for key := range casKeys {
				if strings.HasPrefix(key, fmt.Sprintf("%d/", request.e)) {
					return fail(ErrConflict, "cas target cannot be changed by an entity retraction in the same transaction")
				}
			}
			continue
		}
		if casKeys[fmt.Sprintf("%d/%d", request.e, *request.a)] {
			return fail(ErrConflict, "cas target cannot be changed by another operation in the same transaction")
		}
	}
	return nil
}

func appendRetraction(plan *transactionPlan, request retractRequest) {
	plan.order++
	request.order = plan.order
	plan.retracts = append(plan.retracts, request)
	if plan.schemas != nil && (request.a == nil || isSchemaAttribute(*request.a)) {
		delete(plan.schemas, request.e)
	}
}

func (db *DB) resolveOperationEntity(ctx context.Context, runner sqlRunner, plan *transactionPlan, spec any, create bool) (int64, bool, error) {
	switch spec := spec.(type) {
	case string:
		return plan.allocator.name(ctx, spec, false, create)
	case int:
		return db.resolveOperationNumeric(ctx, runner, int64(spec), create)
	case int64:
		return db.resolveOperationNumeric(ctx, runner, spec, create)
	case float64:
		if integer, ok := exactInt64Float(spec); ok && integer >= 1 {
			return db.resolveOperationNumeric(ctx, runner, integer, create)
		}
		return 0, false, fail(ErrType, "operation entity id %v is invalid; use a positive integer", spec)
	case TempID:
		name := string(spec)
		if id, exists := plan.tempids[name]; exists {
			return id, true, nil
		}
		if !create {
			return 0, false, nil
		}
		id, err := db.tempEntity(plan, name)
		return id, err == nil, err
	case []any:
		return db.resolveLookup(ctx, runner, plan, spec, create)
	default:
		fields, ok := objectFields(spec)
		if ok && len(fields) == 1 && fields[0].Name == "tmp" {
			name, nameOK := fields[0].Value.(string)
			if !nameOK {
				return 0, false, fail(ErrType, "tempid must be a string")
			}
			if id, exists := plan.tempids[name]; exists {
				return id, true, nil
			}
			if !create {
				return 0, false, nil
			}
			id, err := db.tempEntity(plan, name)
			return id, err == nil, err
		}
		return 0, false, fail(ErrType, "operation entity has type %T; use a name, id, lookup, or tempid", spec)
	}
}

func (db *DB) resolveOperationNumeric(ctx context.Context, runner sqlRunner, id int64, create bool) (int64, bool, error) {
	if id < 1 {
		return 0, false, fail(ErrType, "operation entity id %d is invalid; use a positive integer", id)
	}
	resolved, found, err := db.resolveNumericEntity(ctx, runner, id)
	if err != nil || found || !create {
		return resolved, found, err
	}
	return 0, false, fail(ErrNotFound, "operation entity id %d does not exist; use a name, tempid, or known id", id)
}

func (db *DB) diffPlan(ctx context.Context, runner sqlRunner, plan *transactionPlan) ([]plannedFact, []rawFact, error) {
	type workingFact struct {
		raw       *rawFact
		assertion *plannedFact
		value     storedValue
		e         int64
		a         int64
	}
	type orderedOperation struct {
		assertion  *plannedFact
		retraction *retractRequest
		order      int
	}
	factKey := func(entity, attribute int64, value storedValue) string {
		return fmt.Sprintf("%d/%d/%d/%v", entity, attribute, value.tag, storageKey(value.storage))
	}

	current, err := db.workingFactsForPlan(ctx, runner, plan)
	if err != nil {
		return nil, nil, err
	}
	working := make(map[string]workingFact, len(current)+len(plan.assertions))
	protectedOwners := map[int64]struct{}{}
	for index := range current {
		fact := &current[index]
		value := storedValue{storage: fact.v, tag: fact.t}
		working[factKey(fact.e, fact.a, value)] = workingFact{e: fact.e, a: fact.a, value: value, raw: fact}
		if fact.e == fact.tx && fact.a == 1 {
			protectedOwners[fact.e] = struct{}{}
		}
	}

	operations := make([]orderedOperation, 0, len(plan.assertions)+len(plan.retracts))
	for index := range plan.assertions {
		assertion := &plan.assertions[index]
		operations = append(operations, orderedOperation{assertion: assertion, order: assertion.order})
	}
	for index := range plan.retracts {
		retraction := &plan.retracts[index]
		operations = append(operations, orderedOperation{retraction: retraction, order: retraction.order})
	}
	sort.SliceStable(operations, func(i, j int) bool { return operations[i].order < operations[j].order })

	assertions := activePlanAssertions(plan)
	inserted := make([]plannedFact, 0, len(assertions))
	canceled := map[int]struct{}{}
	retractByID := map[int64]rawFact{}
	retractByKey := map[string]int64{}
	for _, operation := range operations {
		if operation.retraction != nil {
			request := operation.retraction
			if request.missing {
				continue
			}
			for key, fact := range working {
				own := fact.e == request.e
				_, protected := protectedOwners[fact.e]
				inbound := request.a == nil && fact.value.tag == TagRef && asInt64(fact.value.storage) == request.e && fact.e > GenesisTx && !protected
				if !own && !inbound {
					continue
				}
				if request.a != nil && fact.a != *request.a {
					continue
				}
				if request.value != nil && !storedEqual(fact.value, *request.value) {
					continue
				}
				delete(working, key)
				if fact.assertion != nil {
					canceled[fact.assertion.order] = struct{}{}
				} else if fact.raw != nil {
					retractByID[fact.raw.id] = *fact.raw
					retractByKey[key] = fact.raw.id
				}
			}
			continue
		}

		assertion := operation.assertion
		if assertion == nil {
			continue
		}
		key := factKey(assertion.e, assertion.a, assertion.value)
		if _, exists := working[key]; exists {
			continue
		}
		// The final active assertions and retractions are fixed before this loop,
		// so the transaction plan's per-attribute schema cache is authoritative.
		schema, err := db.schemaForPlan(ctx, runner, assertion.a, plan)
		if err != nil {
			return nil, nil, err
		}
		if !schema.many {
			for currentKey, fact := range working {
				if fact.e != assertion.e || fact.a != assertion.a {
					continue
				}
				if fact.assertion != nil {
					return nil, nil, fail(ErrConflict, "%q holds one value per entity; assert only one value or declare it many", assertion.attr)
				}
				delete(working, currentKey)
				if fact.raw != nil {
					retractByID[fact.raw.id] = *fact.raw
					retractByKey[currentKey] = fact.raw.id
				}
			}
		}
		if restoredID, restored := retractByKey[key]; restored {
			restoredFact := retractByID[restoredID]
			delete(retractByID, restoredID)
			delete(retractByKey, key)
			working[key] = workingFact{
				e: restoredFact.e, a: restoredFact.a,
				value: storedValue{storage: restoredFact.v, tag: restoredFact.t}, raw: &restoredFact,
			}
			continue
		}
		if schema.unique {
			for _, fact := range working {
				if fact.a == assertion.a && fact.e != assertion.e && storedEqual(fact.value, assertion.value) {
					return nil, nil, fail(ErrConflict, "%q value %v already belongs to entity %d; use an unpinned map to upsert", assertion.attr, assertion.value.logical, fact.e)
				}
			}
		}
		working[key] = workingFact{e: assertion.e, a: assertion.a, value: assertion.value, assertion: assertion}
		inserted = append(inserted, *assertion)
	}

	final := make([]plannedFact, 0, len(inserted))
	for _, assertion := range inserted {
		if _, removed := canceled[assertion.order]; !removed {
			final = append(final, assertion)
		}
	}
	retractions := make([]rawFact, 0, len(retractByID))
	for _, fact := range retractByID {
		retractions = append(retractions, fact)
	}
	sort.Slice(retractions, func(i, j int) bool { return retractions[i].id < retractions[j].id })
	return final, retractions, nil
}

func (db *DB) workingFactsForPlan(ctx context.Context, runner sqlRunner, plan *transactionPlan) ([]rawFact, error) {
	byID := map[int64]rawFact{}
	load := func(query string, args ...any) error {
		rows, err := runner.QueryContext(ctx, query, args...)
		if err != nil {
			return wrap(ErrFormat, err, "cannot read touched transaction state")
		}
		facts, err := scanRawFacts(rows)
		if err != nil {
			return err
		}
		for _, fact := range facts {
			byID[fact.id] = fact
		}
		return nil
	}
	entities := map[int64]bool{}
	for _, assertion := range plan.assertions {
		entities[assertion.e] = true
		schema, err := db.schemaForPlan(ctx, runner, assertion.a, plan)
		if err != nil {
			return nil, err
		}
		if schema.unique {
			if err := load("SELECT id,e,a,v,t,tx,rx FROM fgraph_facts WHERE rx IS NULL AND a=? AND v=? AND t=? ORDER BY id", assertion.a, assertion.value.storage, assertion.value.tag); err != nil {
				return nil, err
			}
		}
	}
	for _, request := range plan.retracts {
		if request.missing {
			continue
		}
		entities[request.e] = true
		if request.a != nil {
			continue
		}
		// Whole-entity retraction is still local: load the entity plus inbound
		// application references, never transaction provenance owners.
		if err := load(`SELECT id,e,a,v,t,tx,rx FROM fgraph_facts f
			WHERE f.rx IS NULL AND (f.e=? OR (f.t=0 AND f.v=? AND f.e>? AND NOT EXISTS (
				SELECT 1 FROM fgraph_facts receipt WHERE receipt.e=f.e AND receipt.a=1 AND receipt.tx=receipt.e
			))) ORDER BY f.id`, request.e, request.e, GenesisTx); err != nil {
			return nil, err
		}
	}
	orderedEntities := make([]int64, 0, len(entities))
	for entity := range entities {
		orderedEntities = append(orderedEntities, entity)
	}
	sort.Slice(orderedEntities, func(left, right int) bool { return orderedEntities[left] < orderedEntities[right] })
	for offset := 0; offset < len(orderedEntities); offset += 400 {
		end := min(offset+400, len(orderedEntities))
		chunk := orderedEntities[offset:end]
		placeholders := strings.TrimRight(strings.Repeat("?,", len(chunk)), ",")
		args := make([]any, len(chunk))
		for index, entity := range chunk {
			args[index] = entity
		}
		if err := load("SELECT id,e,a,v,t,tx,rx FROM fgraph_facts WHERE rx IS NULL AND e IN ("+placeholders+") ORDER BY id", args...); err != nil {
			return nil, err
		}
	}
	result := make([]rawFact, 0, len(byID))
	for _, fact := range byID {
		result = append(result, fact)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].id < result[j].id })
	return result, nil
}

func activePlanAssertions(plan *transactionPlan) []plannedFact {
	assertions := append([]plannedFact(nil), plan.assertions...)
	sort.SliceStable(assertions, func(i, j int) bool { return assertions[i].order < assertions[j].order })
	active := assertions[:0]
	for _, assertion := range assertions {
		canceled := false
		for _, request := range plan.retracts {
			if request.order > assertion.order && retractMatchesPlanned(request, assertion) {
				canceled = true
				break
			}
		}
		if !canceled {
			active = append(active, assertion)
		}
	}
	return active
}

func retractMatchesPlanned(request retractRequest, assertion plannedFact) bool {
	if request.missing {
		return false
	}
	if request.a == nil {
		return assertion.e == request.e || (assertion.value.tag == TagRef && asInt64(assertion.value.storage) == request.e)
	}
	if assertion.e != request.e || assertion.a != *request.a {
		return false
	}
	return request.value == nil || storedEqual(assertion.value, *request.value)
}

func rawRetracted(requests []retractRequest, fact rawFact) bool {
	for _, request := range requests {
		if request.missing {
			continue
		}
		if request.a == nil {
			if fact.e == request.e || (fact.t == TagRef && asInt64(fact.v) == request.e) {
				return true
			}
			continue
		}
		if fact.e == request.e && fact.a == *request.a &&
			(request.value == nil || (fact.t == request.value.tag && storageEqual(fact.v, request.value.storage))) {
			return true
		}
	}
	return false
}

func storageKey(value any) any {
	if bytesValue, ok := value.([]byte); ok {
		return hex.EncodeToString(bytesValue)
	}
	return value
}

func storedEqual(left, right storedValue) bool {
	return left.tag == right.tag && storageEqual(left.storage, right.storage)
}

func storageEqual(left, right any) bool {
	leftBytes, leftOK := left.([]byte)
	rightBytes, rightOK := right.([]byte)
	if leftOK || rightOK {
		return leftOK && rightOK && bytes.Equal(leftBytes, rightBytes)
	}
	return reflect.DeepEqual(left, right)
}

func (db *DB) liveFacts(ctx context.Context, runner sqlRunner, entity int64, attr *int64) ([]rawFact, error) {
	query := "SELECT id,e,a,v,t,tx,rx FROM fgraph_facts WHERE rx IS NULL AND e=?"
	args := []any{entity}
	if attr != nil {
		query += " AND a=?"
		args = append(args, *attr)
	}
	query += " ORDER BY id"
	rows, err := runner.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot read live facts for entity %d", entity)
	}
	return scanRawFacts(rows)
}

func scanRawFacts(rows *sql.Rows) (facts []rawFact, resultErr error) {
	defer func() {
		resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "stored fact rows"))
	}()
	facts = []rawFact{}
	for rows.Next() {
		var fact rawFact
		if err := rows.Scan(&fact.id, &fact.e, &fact.a, &fact.v, &fact.t, &fact.tx, &fact.rx); err != nil {
			return nil, wrap(ErrFormat, err, "cannot decode stored fact row")
		}
		facts = append(facts, fact)
	}
	if err := rows.Err(); err != nil {
		return nil, wrap(ErrFormat, err, "cannot finish reading stored facts")
	}
	return facts, nil
}

func (db *DB) uniqueOwner(ctx context.Context, runner sqlRunner, attr int64, value storedValue) (int64, bool, error) {
	var owner int64
	err := runner.QueryRowContext(ctx, "SELECT e FROM fgraph_facts WHERE a=? AND v=? AND t=? AND rx IS NULL LIMIT 1", attr, value.storage, value.tag).Scan(&owner)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, wrap(ErrFormat, err, "cannot resolve unique value for attribute %d", attr)
	}
	return owner, true, nil
}

func (db *DB) schemaFor(ctx context.Context, runner sqlRunner, attr int64, pending []plannedFact) (attributeSchema, error) {
	return db.schemaForChanges(ctx, runner, attr, pending, nil)
}

func (db *DB) schemaForPlan(ctx context.Context, runner sqlRunner, attr int64, plan *transactionPlan) (attributeSchema, error) {
	if plan.schemas != nil {
		if schema, ok := plan.schemas[attr]; ok {
			return schema, nil
		}
	}
	schema, err := db.schemaForChanges(ctx, runner, attr, activePlanAssertions(plan), plan.retracts)
	if err == nil {
		if plan.schemas == nil {
			plan.schemas = map[int64]attributeSchema{}
		}
		plan.schemas[attr] = schema
	}
	return schema, err
}

func (db *DB) schemaForChanges(
	ctx context.Context,
	runner sqlRunner,
	attr int64,
	pending []plannedFact,
	retracts []retractRequest,
) (attributeSchema, error) {
	schema := attributeSchema{}
	visibility, visibilityArgs := db.visibility("f")
	args := append([]any{attr}, visibilityArgs...)
	rows, err := runner.QueryContext(ctx, `SELECT f.id,f.e,f.a,f.v,f.t,f.tx,f.rx FROM fgraph_facts f
		WHERE f.e=? AND (f.a BETWEEN 5 AND 10 OR f.a=14) AND `+visibility+` ORDER BY f.id`, args...)
	if err != nil {
		return schema, wrap(ErrFormat, err, "cannot read schema for attribute %d", attr)
	}
	facts, err := scanRawFacts(rows)
	if err != nil {
		return schema, err
	}
	for _, fact := range facts {
		if rawRetracted(retracts, fact) {
			continue
		}
		if err := applySchemaFact(&schema, fact.a, fact.v); err != nil {
			return schema, err
		}
	}
	for _, fact := range pending {
		if fact.e == attr && (fact.a >= 5 && fact.a <= 10 || fact.a == 14) {
			if err := applySchemaFact(&schema, fact.a, fact.value.storage); err != nil {
				return schema, err
			}
		}
	}
	return schema, nil
}

func applySchemaFact(schema *attributeSchema, attribute int64, value any) error {
	switch attribute {
	case 5:
		schema.many = sqliteBool(value)
	case 6:
		schema.unique = sqliteBool(value)
	case 7:
		schema.nohistory, schema.nohistorySet = sqliteBool(value), true
	case 8:
		typeName, ok := value.(string)
		if !ok {
			return fail(ErrFormat, "stored schema type has type %T; repair the attribute declaration", value)
		}
		schema.typeName = typeName
	case 9:
		schema.dims, schema.dimsSet = asInt64(value), true
	case 14:
		model, ok := value.(string)
		if !ok {
			return fail(ErrFormat, "stored vector model has type %T; repair the attribute declaration", value)
		}
		schema.vectorModel = model
	}
	return nil
}

func sqliteBool(value any) bool { return asInt64(value) != 0 }

func asInt64(value any) int64 {
	switch value := value.(type) {
	case int64:
		return value
	case int:
		return int64(value)
	case bool:
		if value {
			return 1
		}
	}
	return 0
}

func (db *DB) insertFact(ctx context.Context, runner sqlRunner, fact plannedFact, tx int64) (int64, error) {
	if fact.value.blob != nil {
		if _, err := runner.ExecContext(ctx, "INSERT OR IGNORE INTO fgraph_blobs(hash,data) VALUES (?,?)", fact.value.hash, fact.value.blob); err != nil {
			return 0, wrap(ErrFormat, err, "cannot store content-addressed value for %q", fact.attr)
		}
	}
	result, err := runner.ExecContext(ctx, "INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES (?,?,?,?,?,NULL)", fact.e, fact.a, fact.value.storage, fact.value.tag, tx)
	if err != nil {
		return 0, wrap(ErrConflict, err, "cannot assert %d/%s/%v; retract the duplicate or correct its declaration", fact.e, fact.attr, fact.value.logical)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, wrap(ErrFormat, err, "cannot obtain inserted fact id for %q", fact.attr)
	}
	if fact.value.tag == TagText || fact.value.tag == TagTextRef {
		text, ok := fact.value.logical.(string)
		if !ok {
			return 0, fail(ErrFormat, "text fact for %q decoded as %T; repair the stored value", fact.attr, fact.value.logical)
		}
		if _, err := runner.ExecContext(ctx, "INSERT INTO fgraph_fts(rowid,text) VALUES (?,?)", id, text); err != nil {
			return 0, wrap(ErrFormat, err, "cannot index text fact %d", id)
		}
	}
	return id, nil
}

func (db *DB) renderPlanned(id int64, fact plannedFact, tx int64, rx *int64, alloc *allocator) Fact {
	return Fact{ID: id, E: db.displayEntityAllocated(fact.e, alloc), A: fact.attr, V: db.renderLogicalAllocated(fact.value.logical, fact.value.tag, alloc), Tag: fact.value.tag, Tx: tx, Rx: rx}
}

func (db *DB) planTxFacts(ctx context.Context, runner sqlRunner, plan *transactionPlan, tx int64, data any) ([]plannedFact, error) {
	if tuples, ok := data.([]any); ok {
		result := make([]plannedFact, 0, len(tuples))
		for _, tupleValue := range tuples {
			tuple, tupleOK := tupleValue.([]any)
			if !tupleOK || len(tuple) != 3 {
				return nil, fail(ErrType, "tx_facts item %v is invalid; use [attribute,value,logical-tag]", tupleValue)
			}
			attr, attrOK := tuple[0].(string)
			tagName, tagOK := tuple[2].(string)
			if !attrOK || !tagOK {
				return nil, fail(ErrType, "tx_facts item %v needs text attribute and logical tag", tupleValue)
			}
			valueInput, err := taggedInput(tuple[1], tagName)
			if err != nil {
				return nil, err
			}
			facts, err := db.planTxFact(ctx, runner, plan, tx, attr, valueInput)
			if err != nil {
				return nil, err
			}
			appendAssertions(plan, facts...)
			result = append(result, facts...)
		}
		return result, nil
	}
	fields, ok := objectFields(data)
	if !ok {
		return nil, fail(ErrType, "tx metadata facts have type %T; use an attribute map", data)
	}
	result := []plannedFact{}
	for _, field := range fields {
		if field.Name == "id" {
			return nil, fail(ErrSchema, "transaction metadata cannot set id; the transactor allocates the transaction entity last")
		}
		facts, err := db.planTxMapFacts(ctx, runner, plan, tx, field.Name, field.Value)
		if err != nil {
			return nil, err
		}
		appendAssertions(plan, facts...)
		result = append(result, facts...)
	}
	return result, nil
}

func (db *DB) planTxFact(
	ctx context.Context,
	runner sqlRunner,
	plan *transactionPlan,
	tx int64,
	attr string,
	input any,
) ([]plannedFact, error) {
	switch attr {
	case systemNames[1], systemNames[2], systemNames[3], systemNames[4]:
		return nil, fail(ErrSchema, "transaction fact %q is reserved; use WithAt, WithBy, WithSource, or WithMeta", attr)
	}
	attrID, _, err := plan.allocator.name(ctx, attr, true, true)
	if err != nil {
		return nil, err
	}
	schema, err := db.schemaForPlan(ctx, runner, attrID, plan)
	if err != nil {
		return nil, err
	}
	value, err := db.resolveFactValue(ctx, runner, plan, attr, schema, input)
	if err != nil {
		return nil, err
	}
	if schema.unique {
		owner, found, ownerErr := db.uniqueOwnerIncludingPlan(ctx, runner, plan, attrID, value)
		if ownerErr != nil {
			return nil, ownerErr
		}
		if found && owner != tx {
			return nil, fail(ErrConflict, "unique transaction fact %q value %v already belongs to entity %d", attr, value.logical, owner)
		}
	}
	return db.finishTxFact(plan, tx, attrID, attr, schema, value)
}

func (db *DB) planTxMapFacts(
	ctx context.Context,
	runner sqlRunner,
	plan *transactionPlan,
	tx int64,
	attr string,
	input any,
) ([]plannedFact, error) {
	switch attr {
	case systemNames[1], systemNames[2], systemNames[3], systemNames[4]:
		return nil, fail(ErrSchema, "transaction fact %q is reserved; use WithAt, WithBy, WithSource, or WithMeta", attr)
	}
	attrID, _, err := plan.allocator.name(ctx, attr, true, true)
	if err != nil {
		return nil, err
	}
	schema, err := db.schemaForPlan(ctx, runner, attrID, plan)
	if err != nil {
		return nil, err
	}
	if _, isArray := input.([]any); isArray && !schema.many {
		return nil, fail(ErrConflict, "%q holds one value per transaction; declare it many or wrap a literal array with {\"json\":...}", attr)
	}
	values, err := db.expandAttributeValue(ctx, runner, plan, attr, schema, input)
	if err != nil {
		return nil, err
	}
	if len(values) > 1 && !schema.many {
		return nil, fail(ErrConflict, "%q holds one value per transaction; declare it many or wrap a literal array with {\"json\":...}", attr)
	}
	result := []plannedFact{}
	for _, value := range values {
		if schema.unique {
			owner, found, ownerErr := db.uniqueOwnerIncludingPlan(ctx, runner, plan, attrID, value)
			if ownerErr != nil {
				return nil, ownerErr
			}
			if found && owner != tx {
				return nil, fail(ErrConflict, "unique transaction fact %q value %v already belongs to entity %d", attr, value.logical, owner)
			}
		}
		facts, valueErr := db.finishTxFact(plan, tx, attrID, attr, schema, value)
		if valueErr != nil {
			return nil, valueErr
		}
		result = append(result, facts...)
		if value.tag == TagVector {
			dims, dimsErr := vectorDimensions(value, attr)
			if dimsErr != nil {
				return nil, dimsErr
			}
			schema.dims, schema.dimsSet = dims, true
		}
	}
	return result, nil
}

func (db *DB) finishTxFact(
	plan *transactionPlan,
	tx int64,
	attrID int64,
	attr string,
	schema attributeSchema,
	value storedValue,
) ([]plannedFact, error) {
	if value.tag == TagVector && !schema.dimsSet {
		dims, dimsErr := vectorDimensions(value, attr)
		if dimsErr != nil {
			return nil, dimsErr
		}
		if dimsErr = db.ensureVectorSchema(plan, attrID, attr, dims, schema.typeName == ""); dimsErr != nil {
			return nil, dimsErr
		}
	}
	plan.order++
	return []plannedFact{{e: tx, a: attrID, attr: attr, value: value, order: plan.order, txTarget: true}}, nil
}

func (db *DB) nextTimestamp(ctx context.Context, runner sqlRunner, override *int64) (int64, error) {
	if override != nil {
		if err := validateInstantMicros(*override); err != nil {
			return 0, err
		}
		return *override, nil
	}
	proposed := db.store.clock()
	var latest sql.NullInt64
	if err := runner.QueryRowContext(ctx, "SELECT MAX(v) FROM fgraph_facts WHERE a=1 AND tx=e AND t=5 AND rx IS NULL").Scan(&latest); err != nil {
		return 0, wrap(ErrFormat, err, "cannot determine latest transaction timestamp")
	}
	if latest.Valid && proposed <= latest.Int64 {
		proposed = latest.Int64 + 1_000_000
	}
	if err := validateInstantMicros(proposed); err != nil {
		return 0, err
	}
	return proposed, nil
}

func (db *DB) Declare(ctx context.Context, attr string, options ...DeclareOption) (TxReport, error) {
	return db.declareWithTxOptions(ctx, attr, nil, options...)
}

func (db *DB) declareWithTxOptions(
	ctx context.Context,
	attr string,
	transactionOptions []TxOption,
	options ...DeclareOption,
) (TxReport, error) {
	config := declareOptions{}
	for _, option := range options {
		option(&config)
	}
	if config.typeName == nil && config.many == nil && config.unique == nil && config.nohistory == nil && config.dims == nil && config.doc == nil && config.vectorModel == nil {
		return TxReport{}, fail(ErrSchema, "declaration %q sets no behavior; provide Type, Ref, Many, Unique, NoHistory, Dims, Doc, or VectorModel", attr)
	}
	if err := validateDeclarationOptions(attr, config); err != nil {
		return TxReport{}, err
	}
	data := E{"id": attr}
	if config.typeName != nil {
		data[systemNames[8]] = *config.typeName
	}
	if config.many != nil {
		data[systemNames[5]] = *config.many
	}
	if config.unique != nil {
		data[systemNames[6]] = *config.unique
	}
	if config.nohistory != nil {
		data[systemNames[7]] = *config.nohistory
	}
	if config.dims != nil {
		data[systemNames[9]] = *config.dims
	}
	if config.doc != nil {
		data[systemNames[10]] = *config.doc
	}
	if config.vectorModel != nil {
		data[systemNames[14]] = *config.vectorModel
	}
	transactionOptions = append(append([]TxOption{}, transactionOptions...), func(tx *txOptions) {
		tx.declarationAttr = attr
		tx.declaration = &config
	})
	return db.Transact(ctx, data, transactionOptions...)
}

func validateDeclarationOptions(attr string, config declareOptions) error {
	if err := validateName(attr, true); err != nil {
		return err
	}
	if config.typeName != nil {
		if *config.typeName == "text_ref" || *config.typeName == "bytes_ref" || *config.typeName == "" {
			return fail(ErrSchema, "type %q is not declarable; use text or bytes (indirection is automatic)", *config.typeName)
		}
		if _, ok := parseTagName(*config.typeName); !ok {
			return fail(ErrSchema, "type %q is unknown; use bool, int, float, text, instant, bytes, vector, json, or ref", *config.typeName)
		}
	}
	if config.dims != nil && *config.dims < 1 {
		return fail(ErrSchema, "vector dimensions %d are invalid; use a positive integer", *config.dims)
	}
	if config.vectorModel != nil {
		if len(*config.vectorModel) == 0 || len(*config.vectorModel) > 512 || !utf8.ValidString(*config.vectorModel) {
			return fail(ErrSchema, "vector model must be valid UTF-8 between 1 and 512 bytes")
		}
		if config.typeName != nil && *config.typeName != "vector" {
			return fail(ErrSchema, "vector model requires vector type, got %q", *config.typeName)
		}
	}
	return nil
}

func (db *DB) validateDeclarationOn(ctx context.Context, runner sqlRunner, attr string, config declareOptions) error {
	if err := validateDeclarationOptions(attr, config); err != nil {
		return err
	}
	id, found := db.store.names[attr]
	if config.unique != nil && *config.unique {
		typeName := ""
		if config.typeName != nil {
			typeName = *config.typeName
		} else if found {
			schema, err := db.schemaFor(ctx, runner, id, nil)
			if err != nil {
				return err
			}
			typeName = schema.typeName
		}
		if typeName == "" {
			return fail(ErrSchema, "%q must declare a scalar type before unique; add Type(...) with Unique()", attr)
		}
		if typeName == "json" || typeName == "vector" {
			return fail(ErrSchema, "%q cannot be unique with %s values; choose a scalar declared type", attr, typeName)
		}
	}
	if !found {
		return nil
	}
	if config.unique != nil && *config.unique {
		var duplicates int64
		if err := runner.QueryRowContext(ctx, `SELECT COUNT(*) FROM (
			SELECT v,t FROM fgraph_facts WHERE a=? AND rx IS NULL GROUP BY v,t HAVING COUNT(DISTINCT e)>1
		)`, id).Scan(&duplicates); err != nil {
			return wrap(ErrFormat, err, "cannot validate unique declaration for %q", attr)
		}
		if duplicates > 0 {
			return fail(ErrSchema, "%q has duplicate live values; retract duplicates before declaring it unique", attr)
		}
	}
	if config.many != nil && !*config.many {
		var multiple int64
		if err := runner.QueryRowContext(ctx, `SELECT COUNT(*) FROM (
			SELECT e FROM fgraph_facts WHERE a=? AND rx IS NULL GROUP BY e HAVING COUNT(*)>1
		)`, id).Scan(&multiple); err != nil {
			return wrap(ErrFormat, err, "cannot validate cardinality-one declaration for %q", attr)
		}
		if multiple > 0 {
			return fail(ErrSchema, "%q has entities with several live values; retract extras before disabling many", attr)
		}
	}
	if config.typeName != nil {
		var conflicts int64
		allowed := []Tag{}
		for tag := TagRef; tag <= TagJSON; tag++ {
			if tagCompatible(*config.typeName, tag) {
				allowed = append(allowed, tag)
			}
		}
		placeholders := "?" + strings.Repeat(",?", len(allowed)-1)
		args := []any{id}
		for _, tag := range allowed {
			args = append(args, tag)
		}
		query := "SELECT COUNT(*) FROM fgraph_facts WHERE a=? AND rx IS NULL AND t NOT IN (" + placeholders + ")"
		if err := runner.QueryRowContext(ctx, query, args...).Scan(&conflicts); err != nil {
			return wrap(ErrFormat, err, "cannot validate type declaration for %q", attr)
		}
		if conflicts > 0 {
			return fail(ErrSchema, "%q has live values of another type; retract or convert them before declaring %s", attr, *config.typeName)
		}
	}
	return nil
}

func (db *DB) Retract(ctx context.Context, ref any, args ...any) (TxReport, error) {
	op := make([]any, 0, 2+len(args))
	op = append(op, "retract", ref)
	op = append(op, args...)
	return db.Transact(ctx, op)
}

// MarshalWire converts convenience wrappers to the normative JSON wire form.
func MarshalWire(value any) ([]byte, error) {
	return json.Marshal(wireValue(value))
}

func wireValue(value any) any {
	switch value := value.(type) {
	case RefValue:
		return map[string]any{"ref": wireValue(value.Target)}
	case InstantValue:
		return map[string]any{"instant": value.Micros}
	case BytesValue:
		return map[string]any{"bytes": value}
	case VectorValue:
		return map[string]any{"vector": float32JSON([]float32(value))}
	case JSONValue:
		return map[string]any{"json": value.Value}
	case TempID:
		return map[string]any{"tmp": string(value)}
	case E:
		out := make(map[string]any, len(value))
		for key, item := range value {
			out[key] = wireValue(item)
		}
		return out
	case []any:
		out := make([]any, len(value))
		for i, item := range value {
			out[i] = wireValue(item)
		}
		return out
	default:
		return value
	}
}
