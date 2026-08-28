package fgraph

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/big"
	"slices"
	"sort"
	"strings"
)

type cell struct {
	value any
	tag   Tag
}

const queryNullTag Tag = -1

type queryFact struct {
	cell  cell
	a     string
	aID   int64
	e     int64
	tx    int64
	added bool
}

type binding map[string]cell

type ruleDef struct {
	name string
	args []string
	body []any
}

type queryEvaluator struct {
	ctx       context.Context
	runner    sqlRunner
	db        *DB
	rules     map[string][]ruleDef
	relations map[string][][]cell
	source    string
	budget    int
	work      int
}

func ParseQuery(value any) (Q, error) {
	plain, err := plainJSON(value)
	if err != nil {
		return Q{}, err
	}
	queryObject, objectOK := plain.(map[string]any)
	if !objectOK {
		return Q{}, fail(ErrQuery, "query has type %T; use an object with find and where", plain)
	}
	allowed := map[string]bool{
		"find": true, "where": true, "in": true, "order": true,
		"rules": true, "limit": true, "offset": true, "source": true,
	}
	for key := range queryObject {
		if !allowed[key] {
			return Q{}, fail(ErrQuery, "query field %q is unknown; use find, where, in, order, rules, limit, or offset", key)
		}
	}
	find, err := queryArrayField(queryObject, "find")
	if err != nil {
		return Q{}, err
	}
	where, err := queryArrayField(queryObject, "where")
	if err != nil {
		return Q{}, err
	}
	order, err := queryArrayField(queryObject, "order")
	if err != nil {
		return Q{}, err
	}
	rules, err := queryArrayField(queryObject, "rules")
	if err != nil {
		return Q{}, err
	}
	inputs, err := queryStringArrayField(queryObject, "in")
	if err != nil {
		return Q{}, err
	}
	query := Q{Find: find, Where: where, In: inputs, Order: order, Rules: rules, Source: "current"}
	if rawSource, exists := queryObject["source"]; exists {
		source, ok := rawSource.(string)
		if !ok || (source != "current" && source != "history") {
			return Q{}, fail(ErrQuery, "query source %v is invalid; use current or history", rawSource)
		}
		query.Source = source
	}
	if rawLimit, exists := queryObject["limit"]; exists && rawLimit != nil {
		limit, limitErr := queryIntegerField(rawLimit, "limit")
		if limitErr != nil {
			return Q{}, limitErr
		}
		query.Limit = &limit
	}
	if rawOffset, exists := queryObject["offset"]; exists && rawOffset != nil {
		offset, offsetErr := queryIntegerField(rawOffset, "offset")
		if offsetErr != nil {
			return Q{}, offsetErr
		}
		query.Offset = offset
	}
	return query, nil
}

func queryArrayField(object map[string]any, name string) ([]any, error) {
	value, exists := object[name]
	if !exists || value == nil {
		return nil, nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fail(ErrQuery, "query field %q has type %T; use an array", name, value)
	}
	return items, nil
}

func queryStringArrayField(object map[string]any, name string) ([]string, error) {
	value, exists := object[name]
	if !exists || value == nil {
		return nil, nil
	}
	if strings, ok := value.([]string); ok {
		return append([]string(nil), strings...), nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fail(ErrQuery, "query field %q has type %T; use a text array", name, value)
	}
	result := make([]string, len(items))
	for index, item := range items {
		text, ok := item.(string)
		if !ok {
			return nil, fail(ErrQuery, "query field %q item %d has type %T; use text variables", name, index, item)
		}
		result[index] = text
	}
	return result, nil
}

func queryIntegerField(value any, name string) (int, error) {
	var integer int64
	switch value := value.(type) {
	case int:
		return value, nil
	case int64:
		integer = value
	case float64:
		var ok bool
		integer, ok = exactInt64Float(value)
		if !ok {
			return 0, fail(ErrQuery, "query field %q value %v is not an exact integer", name, value)
		}
	default:
		return 0, fail(ErrQuery, "query field %q has type %T; use an integer", name, value)
	}
	converted := int(integer)
	if int64(converted) != integer {
		return 0, fail(ErrQuery, "query field %q value %d exceeds this platform's integer range", name, integer)
	}
	return converted, nil
}

func (db *DB) QueryJSON(ctx context.Context, value any, args map[string]any) (Result, error) {
	query, err := ParseQuery(value)
	if err != nil {
		return Result{}, err
	}
	return db.Query(ctx, query, args)
}

// Pull returns one entity projected through an explicit pull pattern.
func (db *DB) Pull(ctx context.Context, ref any, pattern []any) (map[string]any, error) {
	if err := validatePullPattern(pattern); err != nil {
		return nil, err
	}
	var result map[string]any
	err := db.withRead(ctx, func(runner sqlRunner) error {
		entity, found, err := db.resolveReadEntity(ctx, runner, ref)
		if err != nil {
			return err
		}
		if !found {
			return fail(ErrNotFound, "entity %v does not exist", ref)
		}
		evaluator := &queryEvaluator{
			db: db, ctx: ctx, runner: runner, source: "current", budget: db.store.queryBudget,
		}
		result, err = evaluator.pullPattern(entity, pattern, 1, map[int64]bool{entity: true})
		return err
	})
	return result, err
}

func (db *DB) Query(ctx context.Context, query Q, args map[string]any) (Result, error) {
	if query.Source == "" {
		query.Source = "current"
	}
	if query.Source != "current" && query.Source != "history" {
		return Result{}, fail(ErrQuery, "query source %q is invalid; use current or history", query.Source)
	}
	if len(query.Find) == 0 {
		return Result{}, fail(ErrQuery, "query find is empty; add at least one variable, aggregate, or pull")
	}
	if err := validateFindItems(query.Find); err != nil {
		return Result{}, err
	}
	if err := validateFindBindings(query); err != nil {
		return Result{}, err
	}
	var result Result
	err := db.withRead(ctx, func(runner sqlRunner) error {
		evaluator := &queryEvaluator{
			db: db, ctx: ctx, runner: runner,
			relations: map[string][][]cell{}, budget: db.store.queryBudget, source: query.Source,
		}
		if parseErr := evaluator.parseRules(query.Rules); parseErr != nil {
			return parseErr
		}
		if relationErr := evaluator.buildRelations(); relationErr != nil {
			return relationErr
		}
		initial := binding{}
		for _, variable := range query.In {
			value, ok := args[variable]
			if !ok {
				return fail(ErrQuery, "input %q is unbound; provide it in args", variable)
			}
			parsed, constantErr := evaluator.constantCell(value)
			if constantErr != nil {
				return constantErr
			}
			initial[variable] = parsed
		}
		bindings, err := evaluator.evalClauses(query.Where, []binding{initial})
		if err != nil {
			return err
		}
		result, err = evaluator.project(query, bindings)
		return err
	})
	return result, err
}

func validateFindBindings(query Q) error {
	bound, err := outwardClauseBindings(query.Where, query.In)
	if err != nil {
		return err
	}
	for _, item := range query.Find {
		variable, ok := item.(string)
		if !ok {
			items, arrayOK := item.([]any)
			if arrayOK && len(items) >= 2 {
				variable, ok = items[1].(string)
			}
		}
		if ok && strings.HasPrefix(variable, "?") && !bound[variable] {
			return fail(ErrQuery, "find variable %q is unbound; bind it in where or in", variable)
		}
	}
	return nil
}

func validateFindItems(find []any) error {
	hasAggregate, hasPull := false, false
	for _, item := range find {
		if variable, ok := item.(string); ok && strings.HasPrefix(variable, "?") {
			continue
		}
		parts, ok := item.([]any)
		if !ok {
			return fail(ErrQuery, "find item %v is invalid; use a variable, aggregate, or [\"pull\",variable,pattern]", item)
		}
		if len(parts) == 2 && findAggregate(parts) != "" {
			variable, variableOK := parts[1].(string)
			if !variableOK || !strings.HasPrefix(variable, "?") {
				return fail(ErrQuery, "aggregate variable %v is invalid; use a ?variable", parts[1])
			}
			hasAggregate = true
			continue
		}
		if len(parts) == 3 && parts[0] == "pull" {
			variable, variableOK := parts[1].(string)
			pattern, patternOK := parts[2].([]any)
			if !variableOK || !strings.HasPrefix(variable, "?") || !patternOK {
				return fail(ErrQuery, "pull item %v is invalid; use [\"pull\",\"?entity\",pattern]", item)
			}
			if err := validatePullPattern(pattern); err != nil {
				return err
			}
			hasPull = true
			continue
		}
		return fail(ErrQuery, "find item %v is invalid; use a variable, aggregate, or [\"pull\",variable,pattern]", item)
	}
	if hasAggregate && hasPull {
		return fail(ErrQuery, "pull and aggregates cannot be mixed in v1; aggregate first and pull matching entities separately")
	}
	return nil
}

func validateClauseBindings(clauses []any, inputs []string) error {
	_, err := outwardClauseBindings(clauses, inputs)
	return err
}

func outwardClauseBindings(clauses []any, inputs []string) (map[string]bool, error) {
	bound := make(map[string]bool, len(inputs))
	for _, variable := range inputs {
		bound[variable] = true
	}
	return bound, validateClauseSequence(clauses, bound)
}

func validateClauseSequence(clauses []any, bound map[string]bool) error {
	for _, clause := range clauses {
		if items, ok := clause.([]any); ok {
			variables := clauseVariables(items)
			if len(items) > 0 && isPredicateOperator(items[0]) {
				if len(items) != 3 {
					return fail(ErrQuery, "predicate %v is invalid; use an operator with two operands", items)
				}
				for _, variable := range variables {
					if !bound[variable] {
						return fail(ErrQuery, "predicate variable %q is unbound; bind it in an earlier pattern or input", variable)
					}
				}
				continue
			}
			if len(items) < 3 || len(items) > 5 {
				return fail(ErrQuery, "pattern %v is invalid; use [entity,attribute,value,tx?,added?]", items)
			}
			attr, attrOK := items[1].(string)
			if !attrOK || (attr != "_" && !strings.HasPrefix(attr, "?") && !attributePattern.MatchString(attr)) {
				return fail(ErrQuery, "pattern attribute %v is invalid; use a name, variable, or _", items[1])
			}
			for _, variable := range variables {
				bound[variable] = true
			}
			continue
		}
		fields, ok := objectFields(clause)
		if !ok || len(fields) != 1 {
			return fail(ErrQuery, "clause %v is invalid; use a pattern, predicate, not, or, or rule", clause)
		}
		switch fields[0].Name {
		case "not":
			inner, ok := fields[0].Value.([]any)
			if !ok {
				return fail(ErrQuery, "not clause has type %T; use an array of clauses", fields[0].Value)
			}
			variables := clauseVariables(inner)
			correlated := false
			for _, variable := range variables {
				correlated = correlated || bound[variable]
			}
			if !correlated {
				return fail(ErrQuery, "negation is uncorrelated; bind at least one of %v in an outer pattern or input", variables)
			}
			innerBound := make(map[string]bool, len(bound))
			for variable := range bound {
				innerBound[variable] = true
			}
			if err := validateClauseSequence(inner, innerBound); err != nil {
				return err
			}
		case "or":
			branches, ok := fields[0].Value.([]any)
			if !ok || len(branches) == 0 {
				return fail(ErrQuery, "or clause must contain one or more clause arrays")
			}
			var expected []string
			for _, branch := range branches {
				branchClauses, ok := branch.([]any)
				if !ok {
					return fail(ErrQuery, "or branch has type %T; use an array of clauses", branch)
				}
				branchBound := make(map[string]bool, len(bound))
				for variable := range bound {
					branchBound[variable] = true
				}
				if err := validateClauseSequence(branchClauses, branchBound); err != nil {
					return err
				}
				outward := []string{}
				for variable := range branchBound {
					if !bound[variable] {
						outward = append(outward, variable)
					}
				}
				sort.Strings(outward)
				if expected == nil {
					expected = outward
				} else if !reflectStrings(expected, outward) {
					return fail(ErrQuery, "every or branch must bind the same outward variables; got %v and %v", expected, outward)
				}
			}
			for _, variable := range expected {
				bound[variable] = true
			}
		case "rule":
			invocation, ok := fields[0].Value.([]any)
			if !ok || len(invocation) == 0 {
				return fail(ErrQuery, "rule invocation has type %T; use [name,args...]", fields[0].Value)
			}
			if _, ok := invocation[0].(string); !ok {
				return fail(ErrQuery, "rule invocation name has type %T; use a text rule name", invocation[0])
			}
			for _, variable := range clauseVariables(invocation) {
				bound[variable] = true
			}
		default:
			return fail(ErrQuery, "unknown clause object %q; use not, or, or rule", fields[0].Name)
		}
	}
	return nil
}

// Qry is a short alias for callers that prefer the specification's q spelling.
func (db *DB) Qry(ctx context.Context, query Q, args map[string]any) (Result, error) {
	return db.Query(ctx, query, args)
}

func (db *DB) queryFacts(ctx context.Context, runner sqlRunner, sources ...string) (facts []queryFact, resultErr error) {
	source := "current"
	if len(sources) > 0 {
		source = sources[0]
	}
	var where string
	args := []any{}
	if source == "current" {
		where, args = db.visibility("f")
	} else if db.asOf != nil {
		where, args = "f.tx <= ?", []any{*db.asOf}
	} else {
		where = "1=1"
	}
	rows, err := runner.QueryContext(ctx, `SELECT f.v,f.t,f.e,f.a,i.name,f.tx,f.rx
		FROM fgraph_facts f JOIN fgraph_ids i ON i.id=f.a WHERE `+where+` ORDER BY f.tx,f.id`, args...)
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot load visible facts for query")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "query fact rows")) }()
	facts = []queryFact{}
	for rows.Next() {
		var raw any
		var tag Tag
		var entity int64
		var attrID, tx int64
		var rx sql.NullInt64
		var attr string
		if err := rows.Scan(&raw, &tag, &entity, &attrID, &attr, &tx, &rx); err != nil {
			return nil, wrap(ErrFormat, err, "cannot decode query fact")
		}
		logical, err := db.logicalValue(ctx, runner, raw, tag)
		if err != nil {
			return nil, err
		}
		facts = append(facts, queryFact{e: entity, a: attr, aID: attrID, tx: tx, added: true, cell: cell{tag: tag, value: logical}})
		if source == "history" && rx.Valid && (db.asOf == nil || rx.Int64 <= *db.asOf) {
			facts = append(facts, queryFact{e: entity, a: attr, aID: attrID, tx: rx.Int64, added: false, cell: cell{tag: tag, value: logical}})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, wrap(ErrFormat, err, "cannot finish loading query facts")
	}
	return facts, nil
}

type orderedQueryClause struct {
	value   any
	pattern []any
	ordinal int
}

func initialBindingNames(input []binding) map[string]bool {
	bound := map[string]bool{}
	if len(input) == 0 {
		return bound
	}
	for variable := range input[0] {
		present := true
		for _, values := range input[1:] {
			if _, exists := values[variable]; !exists {
				present = false
				break
			}
		}
		if present {
			bound[variable] = true
		}
	}
	return bound
}

func isPatternClause(items []any) bool {
	return len(items) >= 3 && len(items) <= 5 && !isPredicateClause(items)
}

func termPlannedBound(term any, bound map[string]bool) bool {
	variable, ok := term.(string)
	return !ok || variable == "_" || !strings.HasPrefix(variable, "?") || bound[variable]
}

func patternAccessRank(pattern []any, bound map[string]bool) (int, string) {
	eBound := termPlannedBound(pattern[0], bound)
	aBound := termPlannedBound(pattern[1], bound)
	vBound := termPlannedBound(pattern[2], bound)
	switch {
	case eBound && aBound && vBound:
		return 0, "eavt/exact"
	case eBound && aBound:
		if variable, ok := pattern[0].(string); ok && bound[variable] && len(pattern) == 3 {
			return 0, "eavt/batch"
		}
		return 0, "eavt/ea"
	case aBound && vBound:
		return 1, "avet"
	case eBound:
		return 2, "eavt/e"
	case aBound:
		return 3, "avet/a"
	case vBound:
		return 4, "value-scan"
	default:
		return 5, "scan"
	}
}

// orderQueryClauses reorders only contiguous pattern blocks. Predicates,
// negation, disjunction, and rules are semantic barriers and remain in place.
func orderQueryClauses(clauses []any, initial map[string]bool) []orderedQueryClause {
	bound := make(map[string]bool, len(initial))
	for variable := range initial {
		bound[variable] = true
	}
	result := make([]orderedQueryClause, 0, len(clauses))
	for offset := 0; offset < len(clauses); {
		items, pattern := clauses[offset].([]any)
		if !pattern || !isPatternClause(items) {
			result = append(result, orderedQueryClause{value: clauses[offset], ordinal: offset})
			items, predicate := clauses[offset].([]any)
			if !predicate || !isPredicateClause(items) {
				fields, object := objectFields(clauses[offset])
				if object && len(fields) == 1 && fields[0].Name != "not" {
					if values, ok := fields[0].Value.([]any); ok {
						for _, variable := range clauseVariables(values) {
							bound[variable] = true
						}
					}
				}
			}
			offset++
			continue
		}
		end := offset
		block := []orderedQueryClause{}
		for end < len(clauses) {
			pattern, patternOK := clauses[end].([]any)
			if !patternOK || !isPatternClause(pattern) {
				break
			}
			block = append(block, orderedQueryClause{pattern: pattern, value: clauses[end], ordinal: end})
			end++
		}
		for len(block) > 0 {
			best := 0
			bestRank, _ := patternAccessRank(block[0].pattern, bound)
			for index := 1; index < len(block); index++ {
				rank, _ := patternAccessRank(block[index].pattern, bound)
				if rank < bestRank || (rank == bestRank && block[index].ordinal < block[best].ordinal) {
					best, bestRank = index, rank
				}
			}
			chosen := block[best]
			result = append(result, chosen)
			for _, variable := range clauseVariables(chosen.pattern) {
				bound[variable] = true
			}
			block = append(block[:best], block[best+1:]...)
		}
		offset = end
	}
	return result
}

func (e *queryEvaluator) evalClauses(clauses []any, input []binding) ([]binding, error) {
	bindings := input
	for _, planned := range orderQueryClauses(clauses, initialBindingNames(input)) {
		clause := planned.value
		var err error
		switch clause := clause.(type) {
		case []any:
			if isPredicateClause(clause) {
				bindings, err = e.evalPredicate(clause, bindings)
			} else {
				bindings, err = e.evalPattern(clause, bindings)
			}
		default:
			fields, ok := objectFields(clause)
			if !ok || len(fields) != 1 {
				return nil, fail(ErrQuery, "clause has type %T; use a pattern, predicate, not, or, or rule", clause)
			}
			switch fields[0].Name {
			case "not":
				inner, ok := fields[0].Value.([]any)
				if !ok {
					return nil, fail(ErrQuery, "not body has type %T; use an array of clauses", fields[0].Value)
				}
				bindings, err = e.evalNot(inner, bindings)
			case "or":
				branches, ok := fields[0].Value.([]any)
				if !ok {
					return nil, fail(ErrQuery, "or body has type %T; use an array of clause arrays", fields[0].Value)
				}
				bindings, err = e.evalOr(branches, bindings)
			case "rule":
				invocation, ok := fields[0].Value.([]any)
				if !ok {
					return nil, fail(ErrQuery, "rule invocation has type %T; use [name,args...]", fields[0].Value)
				}
				bindings, err = e.evalRule(invocation, bindings)
			default:
				return nil, fail(ErrQuery, "unknown clause object %q; use not, or, or rule", fields[0].Name)
			}
		}
		if err != nil {
			return nil, err
		}
		if len(bindings) == 0 {
			break
		}
	}
	return bindings, nil
}

func (e *queryEvaluator) spend() error {
	if err := e.ctx.Err(); err != nil {
		return wrap(ErrQuery, err, "query evaluation canceled")
	}
	if e.budget == 0 {
		e.budget = DefaultQueryBudget
	}
	if e.work >= e.budget {
		return fail(ErrTooLarge, "query exhausted its work budget; narrow the clauses or open with a larger query budget")
	}
	e.work++
	return nil
}

func isPredicateClause(clause []any) bool {
	return len(clause) == 3 && isPredicateOperator(clause[0])
}

func isPredicateOperator(value any) bool {
	op, ok := value.(string)
	if !ok {
		return false
	}
	switch op {
	case "=", "!=", "<", "<=", ">", ">=", "contains", "starts-with":
		return true
	default:
		return false
	}
}

func (e *queryEvaluator) evalPattern(pattern []any, input []binding) ([]binding, error) {
	if err := e.ctx.Err(); err != nil {
		return nil, wrap(ErrQuery, err, "query evaluation canceled")
	}
	if len(pattern) < 3 || len(pattern) > 5 {
		return nil, fail(ErrQuery, "pattern has %d items; use [entity,attribute,value,tx?,added?]", len(pattern))
	}
	attr, ok := pattern[1].(string)
	if !ok || (attr != "_" && !strings.HasPrefix(attr, "?") && !attributePattern.MatchString(attr)) {
		return nil, fail(ErrQuery, "pattern attribute %v is invalid; use a name, variable, or _", pattern[1])
	}
	if attributePattern.MatchString(attr) {
		if _, exists := e.db.store.names[attr]; !exists {
			return []binding{}, nil
		}
	}
	if len(pattern) == 5 {
		switch value := pattern[4].(type) {
		case bool:
		case string:
			if value != "_" && !strings.HasPrefix(value, "?") {
				return nil, fail(ErrQuery, "added term must be a bool, variable, or _")
			}
		default:
			return nil, fail(ErrQuery, "added term must be a bool, variable, or _")
		}
	}
	if output, handled, err := e.evalBoundEntityBatch(pattern, input); handled || err != nil {
		return output, err
	}
	output := []binding{}
	for _, current := range input {
		matches, err := e.evalIndexedPattern(pattern, current)
		if err != nil {
			return nil, err
		}
		output = append(output, matches...)
	}
	return distinctBindings(output)
}

func (e *queryEvaluator) evalBoundEntityBatch(pattern []any, input []binding) ([]binding, bool, error) {
	if e.source != "current" || len(pattern) != 3 || len(input) < 2 {
		return nil, false, nil
	}
	entityVariable, ok := pattern[0].(string)
	if !ok || !strings.HasPrefix(entityVariable, "?") {
		return nil, false, nil
	}
	attributeName, ok := pattern[1].(string)
	if !ok || !attributePattern.MatchString(attributeName) {
		return nil, false, nil
	}
	attribute, found := e.db.store.names[attributeName]
	if !found {
		return []binding{}, true, nil
	}
	if valueVariable, variable := pattern[2].(string); variable && strings.HasPrefix(valueVariable, "?") {
		for _, current := range input {
			if _, bound := current[valueVariable]; bound {
				return nil, false, nil
			}
		}
	} else if pattern[2] != "_" {
		return nil, false, nil
	}
	byEntity := map[int64][]binding{}
	entities := []int64{}
	for _, current := range input {
		value, bound := current[entityVariable]
		if !bound || value.tag != TagRef {
			return nil, false, nil
		}
		entity := asInt64(value.value)
		if len(byEntity[entity]) == 0 {
			entities = append(entities, entity)
		}
		byEntity[entity] = append(byEntity[entity], current)
	}
	slices.Sort(entities)
	result := []binding{}
	for offset := 0; offset < len(entities); offset += 400 {
		end := min(offset+400, len(entities))
		chunk := entities[offset:end]
		query, alias, args, err := e.patternSQLBase()
		if err != nil {
			return nil, true, err
		}
		query += " AND " + alias + ".a=? AND " + alias + ".e IN (" + strings.TrimRight(strings.Repeat("?,", len(chunk)), ",") + ") ORDER BY " + alias + ".id"
		args = append(args, attribute)
		for _, entity := range chunk {
			args = append(args, entity)
		}
		rows, err := e.runner.QueryContext(e.ctx, query, args...)
		if err != nil {
			return nil, true, wrap(ErrFormat, err, "cannot execute batched connected pattern")
		}
		chunkErr := func() (resultErr error) {
			defer func() {
				resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "batched connected pattern"))
			}()
			for rows.Next() {
				var raw any
				var tag Tag
				var entity, actualAttribute, tx, added int64
				var storedAttributeName string
				if scanErr := rows.Scan(&raw, &tag, &entity, &actualAttribute, &storedAttributeName, &tx, &added); scanErr != nil {
					return wrap(ErrFormat, scanErr, "cannot decode batched connected pattern")
				}
				logical, logicalErr := e.db.logicalValue(e.ctx, e.runner, raw, tag)
				if logicalErr != nil {
					return logicalErr
				}
				actual := []cell{{tag: TagRef, value: entity}, {tag: TagRef, value: actualAttribute}, {tag: tag, value: logical}}
				for _, current := range byEntity[entity] {
					if spendErr := e.spend(); spendErr != nil {
						return spendErr
					}
					next := cloneBinding(current)
					matched := true
					for index, term := range pattern {
						matched, err = e.matchTerm(term, actual[index], next, index < 2)
						if err != nil || !matched {
							break
						}
					}
					if err != nil {
						return err
					}
					if matched {
						result = append(result, next)
					}
				}
			}
			if rowsErr := rows.Err(); rowsErr != nil {
				return wrap(ErrFormat, rowsErr, "cannot finish batched connected pattern")
			}
			return nil
		}()
		if chunkErr != nil {
			return nil, true, chunkErr
		}
	}
	distinct, err := distinctBindings(result)
	return distinct, true, err
}

func (e *queryEvaluator) patternTermCell(term any, values binding, entity bool) (cell, bool, bool, error) {
	if variable, ok := term.(string); ok {
		if variable == "_" {
			return cell{}, false, true, nil
		}
		if strings.HasPrefix(variable, "?") {
			value, bound := values[variable]
			return value, bound, true, nil
		}
	}
	return e.preparePatternTerm(term, entity)
}

func (e *queryEvaluator) patternSQLBase() (string, string, []any, error) {
	if e.source == "history" {
		basis, err := e.db.basisOn(e.ctx, e.runner)
		if err != nil {
			return "", "", nil, err
		}
		if e.db.asOf != nil && *e.db.asOf < basis {
			basis = *e.db.asOf
		}
		base := `SELECT d.v,d.t,d.e,d.a,i.name,d.dtx,d.added FROM (
			SELECT f.v,f.t,f.e,f.a,f.tx AS dtx,1 AS added,f.id FROM fgraph_facts f WHERE f.tx<=?
			UNION ALL
			SELECT f.v,f.t,f.e,f.a,f.rx AS dtx,0 AS added,f.id FROM fgraph_facts f WHERE f.rx IS NOT NULL AND f.rx<=?
		) d JOIN fgraph_ids i ON i.id=d.a WHERE 1=1`
		return base, "d", []any{basis, basis}, nil
	}
	visibility, args := e.db.visibility("f")
	base := `SELECT f.v,f.t,f.e,f.a,i.name,f.tx AS dtx,1 AS added
		FROM fgraph_facts f JOIN fgraph_ids i ON i.id=f.a WHERE ` + visibility
	return base, "f", args, nil
}

func storedCell(value cell) (storedValue, error) {
	switch value.tag {
	case TagRef:
		return storedValue{logical: asInt64(value.value), storage: asInt64(value.value), tag: TagRef}, nil
	case TagBool:
		boolean, ok := value.value.(bool)
		if !ok {
			return storedValue{}, fail(ErrQuery, "bound bool query value decoded as %T", value.value)
		}
		integer := int64(0)
		if boolean {
			integer = 1
		}
		return storedValue{logical: boolean, storage: integer, tag: TagBool}, nil
	case TagInt:
		integer := asInt64(value.value)
		return storedValue{logical: integer, storage: integer, tag: TagInt}, nil
	case TagFloat:
		floating, ok := numeric(value.value)
		if !ok {
			return storedValue{}, fail(ErrQuery, "bound float query value decoded as %T", value.value)
		}
		return floatStored(floating)
	case TagText, TagTextRef:
		text, ok := value.value.(string)
		if !ok {
			return storedValue{}, fail(ErrQuery, "bound text query value decoded as %T", value.value)
		}
		return textStored(text)
	case TagInstant:
		return instantStored(asInt64(value.value))
	case TagBytes, TagBytesRef:
		bytesValue, ok := value.value.([]byte)
		if !ok {
			return storedValue{}, fail(ErrQuery, "bound bytes query value decoded as %T", value.value)
		}
		return bytesStored(bytesValue)
	case TagVector:
		vector, ok := value.value.([]float32)
		if !ok {
			return storedValue{}, fail(ErrQuery, "bound vector query value decoded as %T", value.value)
		}
		return vectorStored(vector)
	case TagJSON:
		return jsonStored(value.value)
	default:
		return storedValue{}, fail(ErrQuery, "bound query value has unsupported tag %d", value.tag)
	}
}

func (e *queryEvaluator) evalIndexedPattern(pattern []any, current binding) (result []binding, resultErr error) {
	query, alias, args, baseErr := e.patternSQLBase()
	if baseErr != nil {
		return nil, baseErr
	}
	conditions := []string{}
	bound := make([]cell, 5)
	isBound := make([]bool, 5)
	for index, term := range pattern {
		entity := index == 0 || index == 1 || index == 3
		value, present, found, termErr := e.patternTermCell(term, current, entity)
		if termErr != nil {
			return nil, termErr
		}
		if !found {
			return []binding{}, nil
		}
		bound[index], isBound[index] = value, present
	}
	columns := []string{alias + ".e", alias + ".a", alias + ".v", alias + ".dtx", alias + ".added"}
	if e.source == "current" {
		columns[3], columns[4] = alias+".tx", "1"
	}
	for _, index := range []int{0, 1, 3, 4} {
		if index >= len(pattern) || !isBound[index] {
			continue
		}
		value := bound[index].value
		if index == 4 {
			boolean, ok := value.(bool)
			if !ok {
				return nil, fail(ErrQuery, "added term must be a bool, variable, or _")
			}
			if boolean {
				value = int64(1)
			} else {
				value = int64(0)
			}
		} else {
			value = asInt64(value)
		}
		conditions = append(conditions, columns[index]+"=?")
		args = append(args, value)
	}
	// The query cell already carries its canonical logical tag, so a pinned
	// attribute/value pair is storage-safe even when the attribute is undeclared.
	if len(pattern) >= 3 && isBound[2] && isBound[1] {
		stored, err := storedCell(bound[2])
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, alias+".v=?", alias+".t=?")
		args = append(args, stored.storage, stored.tag)
	}
	if len(conditions) > 0 {
		query += " AND " + strings.Join(conditions, " AND ")
	}
	query += " ORDER BY dtx,added," + alias + ".e," + alias + ".a"
	rows, err := e.runner.QueryContext(e.ctx, query, args...)
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot execute indexed datom pattern")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "indexed query pattern rows")) }()
	for rows.Next() {
		if err := e.spend(); err != nil {
			return nil, err
		}
		var raw any
		var tag Tag
		var entity, attribute, tx, added int64
		var attributeName string
		if err := rows.Scan(&raw, &tag, &entity, &attribute, &attributeName, &tx, &added); err != nil {
			return nil, wrap(ErrFormat, err, "cannot decode indexed query pattern")
		}
		logical, err := e.db.logicalValue(e.ctx, e.runner, raw, tag)
		if err != nil {
			return nil, err
		}
		next := cloneBinding(current)
		actual := []cell{
			{tag: TagRef, value: entity},
			{tag: TagRef, value: attribute},
			{tag: tag, value: logical},
			{tag: TagRef, value: tx},
			{tag: TagBool, value: added != 0},
		}
		matched := true
		for index, term := range pattern {
			entityTerm := index == 0 || index == 1 || index == 3
			matched, err = e.matchTerm(term, actual[index], next, entityTerm)
			if err != nil {
				return nil, err
			}
			if !matched {
				break
			}
		}
		if matched {
			result = append(result, next)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, wrap(ErrFormat, err, "cannot finish indexed query pattern")
	}
	return result, nil
}

func (e *queryEvaluator) preparePatternTerm(term any, entity bool) (cell, bool, bool, error) {
	if text, ok := term.(string); ok && (text == "_" || strings.HasPrefix(text, "?")) {
		return cell{}, false, true, nil
	}
	if entity {
		id, found, err := e.db.resolveReadEntity(e.ctx, e.runner, term)
		if err != nil {
			return cell{}, true, false, wrap(ErrQuery, err, "entity pattern constant %v is invalid", term)
		}
		return cell{tag: TagRef, value: id}, true, found, nil
	}
	constant, err := e.constantCell(term)
	if err != nil {
		return cell{}, true, false, err
	}
	if constant.tag == TagRef {
		id := asInt64(constant.value)
		if id < 1 {
			return cell{}, true, false, nil
		}
		_, found, resolveErr := e.db.resolveNumericEntity(e.ctx, e.runner, id)
		if resolveErr != nil {
			return cell{}, true, false, resolveErr
		}
		if !found {
			return cell{}, true, false, nil
		}
	}
	return constant, true, true, nil
}

func (e *queryEvaluator) matchTerm(term any, actual cell, values binding, entity bool) (bool, error) {
	if text, ok := term.(string); ok {
		if text == "_" {
			return true, nil
		}
		if strings.HasPrefix(text, "?") {
			if prior, bound := values[text]; bound {
				return cellsEqual(prior, actual), nil
			}
			values[text] = actual
			return true, nil
		}
		if entity {
			id, found := e.db.store.names[text]
			return found && id == asInt64(actual.value), nil
		}
	}
	constant, err := e.constantCell(term)
	if err != nil {
		return false, err
	}
	if entity && constant.tag != TagRef {
		if constant.tag == TagInt {
			constant.tag = TagRef
		} else {
			return false, fail(ErrQuery, "entity pattern constant %v is invalid; use a name, id, variable, or _", term)
		}
	}
	return cellsEqual(constant, actual), nil
}

func (e *queryEvaluator) constantCell(value any) (cell, error) {
	stored, err := scalarValue(value)
	if err != nil {
		return cell{}, fail(ErrQuery, "invalid query constant %v: %v", value, err)
	}
	if stored.tag == TagRef {
		ref, ok := stored.logical.(RefValue)
		if !ok {
			return cell{}, fail(ErrQuery, "query ref has invalid internal type %T; use RefTo(name-or-id)", stored.logical)
		}
		switch target := ref.Target.(type) {
		case string:
			id, found := e.db.store.names[target]
			if !found {
				return cell{tag: TagRef, value: int64(-1)}, nil
			}
			stored.logical = id
		case int64:
			stored.logical = target
		case int:
			stored.logical = int64(target)
		default:
			return cell{}, fail(ErrQuery, "query ref target has type %T; use a name or integer id", target)
		}
	}
	return cell{tag: stored.tag, value: stored.logical}, nil
}

func cloneBinding(source binding) binding {
	clone := make(binding, len(source)+1)
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func cellsEqual(left, right cell) bool {
	if left.tag != right.tag {
		return false
	}
	return storageEqual(left.value, right.value)
}

func (e *queryEvaluator) evalPredicate(clause []any, input []binding) ([]binding, error) {
	op, ok := clause[0].(string)
	if !ok {
		return nil, fail(ErrQuery, "predicate operator has type %T; use a supported operator", clause[0])
	}
	output := []binding{}
	for _, values := range input {
		if err := e.spend(); err != nil {
			return nil, err
		}
		left, leftBound, err := e.termValue(clause[1], values)
		if err != nil {
			return nil, err
		}
		right, rightBound, err := e.termValue(clause[2], values)
		if err != nil {
			return nil, err
		}
		if !leftBound || !rightBound {
			return nil, fail(ErrQuery, "predicate %q uses an unbound variable; bind variables in an earlier pattern or input", op)
		}
		matched, err := compareCells(op, left, right)
		if err != nil {
			return nil, err
		}
		if matched {
			output = append(output, values)
		}
	}
	return distinctBindings(output)
}

func (e *queryEvaluator) termValue(term any, values binding) (cell, bool, error) {
	if variable, ok := term.(string); ok && strings.HasPrefix(variable, "?") {
		value, found := values[variable]
		return value, found, nil
	}
	value, err := e.constantCell(term)
	return value, err == nil, err
}

func compareCells(op string, left, right cell) (bool, error) {
	switch op {
	case "=":
		return predicateCellsEqual(left, right), nil
	case "!=":
		return !predicateCellsEqual(left, right), nil
	case "contains", "starts-with":
		leftText, leftOK := left.value.(string)
		rightText, rightOK := right.value.(string)
		if !leftOK || !rightOK {
			return false, fail(ErrQuery, "%s requires text operands; bind it to text facts", op)
		}
		if op == "contains" {
			return strings.Contains(leftText, rightText), nil
		}
		return strings.HasPrefix(leftText, rightText), nil
	case "<", "<=", ">", ">=":
		if left.tag != right.tag && (!numericCell(left) || !numericCell(right)) {
			return false, fail(ErrQuery, "%s cannot order unlike logical types; compare matching types or two numbers", op)
		}
		comparison, ok := orderedCompare(left.value, right.value)
		if !ok {
			return false, fail(ErrQuery, "%s operands %T and %T are not comparable; use two numbers or two strings", op, left.value, right.value)
		}
		switch op {
		case "<":
			return comparison < 0, nil
		case "<=":
			return comparison <= 0, nil
		case ">":
			return comparison > 0, nil
		default:
			return comparison >= 0, nil
		}
	default:
		return false, fail(ErrQuery, "unknown predicate %q; use =, !=, comparison, contains, or starts-with", op)
	}
}

func predicateCellsEqual(left, right cell) bool {
	if numericCell(left) && numericCell(right) {
		comparison, ok := orderedNumericCompare(left.value, right.value)
		if !ok {
			return false
		}
		return comparison == 0
	}
	return cellsEqual(left, right)
}

func numericCell(value cell) bool {
	return value.tag == TagInt || value.tag == TagFloat
}

func orderedNumericCompare(left, right any) (int, bool) {
	l, ok := numericRat(left)
	if !ok {
		return 0, false
	}
	r, ok := numericRat(right)
	if !ok {
		return 0, false
	}
	return l.Cmp(r), true
}

func orderedCompare(left, right any) (int, bool) {
	if comparison, ok := orderedNumericCompare(left, right); ok {
		return comparison, true
	}
	if l, ok := left.(bool); ok {
		r, rightOK := right.(bool)
		if !rightOK {
			return 0, false
		}
		switch {
		case !l && r:
			return -1, true
		case l && !r:
			return 1, true
		default:
			return 0, true
		}
	}
	l, leftOK := left.(string)
	r, rightOK := right.(string)
	if !leftOK || !rightOK {
		return 0, false
	}
	return strings.Compare(l, r), true
}

func numericRat(value any) (*big.Rat, bool) {
	switch value := value.(type) {
	case int:
		return new(big.Rat).SetInt64(int64(value)), true
	case int64:
		return new(big.Rat).SetInt64(value), true
	case float32:
		rat := new(big.Rat).SetFloat64(float64(value))
		return rat, rat != nil
	case float64:
		rat := new(big.Rat).SetFloat64(value)
		return rat, rat != nil
	default:
		return nil, false
	}
}

func numeric(value any) (float64, bool) {
	switch value := value.(type) {
	case int64:
		return float64(value), true
	case float64:
		return value, true
	case float32:
		return float64(value), true
	default:
		return 0, false
	}
}

func (e *queryEvaluator) evalNot(clauses []any, input []binding) ([]binding, error) {
	variables := clauseVariables(clauses)
	for _, values := range input {
		correlated := false
		for _, variable := range variables {
			_, bound := values[variable]
			correlated = correlated || bound
		}
		if !correlated {
			return nil, fail(ErrQuery, "negation is uncorrelated; bind at least one of %v in an outer pattern or input", variables)
		}
	}
	output := []binding{}
	for _, values := range input {
		matches, err := e.evalClauses(clauses, []binding{cloneBinding(values)})
		if err != nil {
			return nil, err
		}
		if len(matches) == 0 {
			output = append(output, values)
		}
	}
	return output, nil
}

func (e *queryEvaluator) evalOr(branches []any, input []binding) ([]binding, error) {
	if len(branches) == 0 {
		return nil, fail(ErrQuery, "or has no branches; add at least one clause list")
	}
	output := []binding{}
	for _, branch := range branches {
		clauses, ok := branch.([]any)
		if !ok {
			return nil, fail(ErrQuery, "or branch has type %T; use an array of clauses", branch)
		}
		matches, err := e.evalClauses(clauses, cloneBindings(input))
		if err != nil {
			return nil, err
		}
		output = append(output, matches...)
	}
	return distinctBindings(output)
}

func cloneBindings(input []binding) []binding {
	result := make([]binding, len(input))
	for i, values := range input {
		result[i] = cloneBinding(values)
	}
	return result
}

func clauseVariables(clauses []any) []string {
	set := map[string]bool{}
	var visit func(any)
	visit = func(value any) {
		switch value := value.(type) {
		case string:
			if strings.HasPrefix(value, "?") {
				set[value] = true
			}
		case []any:
			for _, item := range value {
				visit(item)
			}
		case Object:
			for _, field := range value.Fields {
				visit(field.Value)
			}
		case map[string]any:
			for _, item := range value {
				visit(item)
			}
		}
	}
	visit(clauses)
	variables := make([]string, 0, len(set))
	for variable := range set {
		variables = append(variables, variable)
	}
	sort.Strings(variables)
	return variables
}

func reflectStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func distinctBindings(input []binding) ([]binding, error) {
	seen := map[string]bool{}
	output := []binding{}
	for _, values := range input {
		key, err := bindingKey(values)
		if err != nil {
			return nil, err
		}
		if !seen[key] {
			seen[key] = true
			output = append(output, values)
		}
	}
	return output, nil
}

func bindingKey(values binding) (string, error) {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, key := range keys {
		cellKey, err := structuralCellKey(values[key])
		if err != nil {
			return "", err
		}
		appendKeyPart(&b, key)
		appendKeyPart(&b, cellKey)
	}
	return b.String(), nil
}

func appendKeyPart(builder *strings.Builder, part string) {
	fmt.Fprintf(builder, "%d:", len(part))
	builder.WriteString(part)
}

func structuralCellKey(value cell) (string, error) {
	encoded, err := canonicalJSON(storageKey(value.value))
	if err != nil {
		return "", wrap(ErrQuery, err, "cannot encode a query value for structural comparison")
	}
	var key strings.Builder
	appendKeyPart(&key, fmt.Sprintf("%d", value.tag))
	appendKeyPart(&key, string(encoded))
	return key.String(), nil
}

func (e *queryEvaluator) project(query Q, bindings []binding) (Result, error) {
	columns := make([]string, len(query.Find))
	hasAggregate := false
	for i, item := range query.Find {
		columns[i] = findLabel(item)
		if findAggregate(item) != "" {
			hasAggregate = true
		}
	}
	rows := [][]any{}
	rowCells := [][]cell{}
	rowBindings := []binding{}
	if hasAggregate {
		groups := map[string][]binding{}
		groupOrder := []string{}
		for _, values := range bindings {
			key, err := e.groupKey(query.Find, values)
			if err != nil {
				return Result{}, err
			}
			if _, exists := groups[key]; !exists {
				groupOrder = append(groupOrder, key)
			}
			groups[key] = append(groups[key], values)
		}
		if len(bindings) == 0 && allAggregates(query.Find) {
			groups[""] = []binding{}
			groupOrder = append(groupOrder, "")
		}
		seenRows := map[string]bool{}
		for _, key := range groupOrder {
			row, cells, err := e.aggregateRow(query.Find, groups[key])
			if err != nil {
				return Result{}, err
			}
			encoded, err := canonicalJSON(row)
			if err != nil {
				return Result{}, wrap(ErrQuery, err, "cannot deduplicate aggregate query row")
			}
			if seenRows[string(encoded)] {
				continue
			}
			seenRows[string(encoded)] = true
			rows = append(rows, row)
			rowCells = append(rowCells, cells)
			rowBindings = append(rowBindings, nil)
		}
	} else {
		seen := map[string]bool{}
		for _, values := range bindings {
			row, cells, err := e.projectRow(query.Find, values)
			if err != nil {
				return Result{}, err
			}
			encoded, err := canonicalJSON(row)
			if err != nil {
				return Result{}, wrap(ErrQuery, err, "cannot deduplicate projected query row")
			}
			if !seen[string(encoded)] {
				seen[string(encoded)] = true
				rows = append(rows, row)
				rowCells = append(rowCells, cells)
				rowBindings = append(rowBindings, values)
			}
		}
	}
	if len(query.Order) > 0 {
		bound, bindingErr := outwardClauseBindings(query.Where, query.In)
		if bindingErr != nil {
			return Result{}, bindingErr
		}
		order, err := parseOrderBound(query.Order, columns, bound, hasAggregate)
		if err != nil {
			return Result{}, err
		}
		indices := make([]int, len(rows))
		for i := range indices {
			indices[i] = i
		}
		var sortErr error
		sort.SliceStable(indices, func(i, j int) bool {
			left, right := indices[i], indices[j]
			for _, item := range order {
				var leftCell, rightCell cell
				if item.index >= 0 {
					leftCell, rightCell = rowCells[left][item.index], rowCells[right][item.index]
				} else {
					leftCell, rightCell = rowBindings[left][item.variable], rowBindings[right][item.variable]
				}
				comparison, compareErr := e.compareOrderCells(leftCell, rightCell)
				if compareErr != nil {
					sortErr = compareErr
					return false
				}
				if comparison != 0 {
					if item.desc {
						return comparison > 0
					}
					return comparison < 0
				}
			}
			return false
		})
		if sortErr != nil {
			return Result{}, sortErr
		}
		sortedRows := make([][]any, len(rows))
		for i, index := range indices {
			sortedRows[i] = rows[index]
		}
		rows = sortedRows
	}
	start := query.Offset
	if start < 0 {
		return Result{}, fail(ErrQuery, "offset %d is invalid; use zero or a positive integer", start)
	}
	if start > len(rows) {
		start = len(rows)
	}
	end := len(rows)
	if query.Limit != nil {
		if *query.Limit < 0 {
			return Result{}, fail(ErrQuery, "limit %d is invalid; use zero or a positive integer", *query.Limit)
		}
		if *query.Limit == 0 {
			end = start
		} else if start+*query.Limit < end {
			end = start + *query.Limit
		}
	}
	return Result{Columns: columns, Rows: rows[start:end]}, nil
}

func (e *queryEvaluator) compareOrderCells(left, right cell) (int, error) {
	leftValue := e.db.renderLogical(left.value, left.tag)
	rightValue := e.db.renderLogical(right.value, right.tag)
	leftCategory := renderedOrderCategory(leftValue)
	rightCategory := renderedOrderCategory(rightValue)
	if leftCategory != rightCategory {
		return leftCategory - rightCategory, nil
	}
	switch leftCategory {
	case 0:
		leftBool, leftOK := leftValue.(bool)
		rightBool, rightOK := rightValue.(bool)
		if !leftOK || !rightOK {
			return 0, fail(ErrQuery, "order boolean values %T and %T are invalid; store boolean values", leftValue, rightValue)
		}
		switch {
		case !leftBool && rightBool:
			return -1, nil
		case leftBool && !rightBool:
			return 1, nil
		default:
			return 0, nil
		}
	case 1:
		comparison, ok := orderedNumericCompare(leftValue, rightValue)
		if !ok {
			return 0, fail(ErrQuery, "order numeric values %T and %T are invalid; store finite int or float values", leftValue, rightValue)
		}
		return comparison, nil
	case 2:
		leftText, leftOK := leftValue.(string)
		rightText, rightOK := rightValue.(string)
		if !leftOK || !rightOK {
			return 0, fail(ErrQuery, "order text values %T and %T are invalid; store text values", leftValue, rightValue)
		}
		return strings.Compare(leftText, rightText), nil
	default:
		leftJSON, leftErr := canonicalJSON(leftValue)
		if leftErr != nil {
			return 0, wrap(ErrQuery, leftErr, "cannot encode left order value")
		}
		rightJSON, rightErr := canonicalJSON(rightValue)
		if rightErr != nil {
			return 0, wrap(ErrQuery, rightErr, "cannot encode right order value")
		}
		return bytes.Compare(leftJSON, rightJSON), nil
	}
}

func renderedOrderCategory(value any) int {
	switch value.(type) {
	case bool:
		return 0
	case int, int64, float32, float64:
		return 1
	case string:
		return 2
	default:
		return 3
	}
}

func findLabel(item any) string {
	if variable, ok := item.(string); ok {
		return variable
	}
	items, ok := item.([]any)
	if !ok || len(items) < 2 {
		return fmt.Sprint(item)
	}
	name, nameOK := items[0].(string)
	variable, variableOK := items[1].(string)
	if !nameOK || !variableOK {
		return fmt.Sprint(item)
	}
	if name == "pull" {
		return "pull(" + variable + ")"
	}
	return name + "(" + variable + ")"
}

func findAggregate(item any) string {
	items, ok := item.([]any)
	if !ok || len(items) != 2 {
		return ""
	}
	name, ok := items[0].(string)
	if !ok {
		return ""
	}
	switch name {
	case "count", "count-distinct", "sum", "min", "max", "avg":
		return name
	default:
		return ""
	}
}

func allAggregates(items []any) bool {
	for _, item := range items {
		if findAggregate(item) == "" {
			return false
		}
	}
	return true
}

func (e *queryEvaluator) groupKey(find []any, values binding) (string, error) {
	var b strings.Builder
	for _, item := range find {
		if findAggregate(item) != "" {
			continue
		}
		variable, ok := item.(string)
		if ok {
			value := values[variable]
			cellKey, err := structuralCellKey(value)
			if err != nil {
				return "", err
			}
			appendKeyPart(&b, variable)
			appendKeyPart(&b, cellKey)
		}
	}
	return b.String(), nil
}

func (e *queryEvaluator) projectRow(find []any, values binding) ([]any, []cell, error) {
	row := make([]any, 0, len(find))
	cells := make([]cell, 0, len(find))
	for _, item := range find {
		if variable, ok := item.(string); ok {
			value, found := values[variable]
			if !found {
				return nil, nil, fail(ErrQuery, "find variable %q is unbound; bind it in where or in", variable)
			}
			row = append(row, e.db.renderLogical(value.value, value.tag))
			cells = append(cells, value)
			continue
		}
		items, ok := item.([]any)
		if !ok || len(items) != 3 || items[0] != "pull" {
			return nil, nil, fail(ErrQuery, "find item %v is invalid; use a variable, aggregate, or [\"pull\",variable,pattern]", item)
		}
		variable, ok := items[1].(string)
		if !ok {
			return nil, nil, fail(ErrQuery, "pull variable %v is invalid", items[1])
		}
		value, found := values[variable]
		if !found || value.tag != TagRef {
			return nil, nil, fail(ErrQuery, "pull variable %q is not bound to an entity", variable)
		}
		pattern, ok := items[2].([]any)
		if !ok {
			return nil, nil, fail(ErrQuery, "pull pattern has type %T; use an attribute array", items[2])
		}
		if err := validatePullPattern(pattern); err != nil {
			return nil, nil, err
		}
		pulled, err := e.pullPattern(asInt64(value.value), pattern, 1, map[int64]bool{})
		if err != nil {
			return nil, nil, err
		}
		row = append(row, pulled)
		cells = append(cells, cell{tag: TagJSON, value: pulled})
	}
	return row, cells, nil
}

func validatePullPattern(pattern []any) error {
	for _, item := range pattern {
		if item == "*" {
			continue
		}
		if attr, ok := item.(string); ok {
			if err := validatePullAttribute(attr); err != nil {
				return err
			}
			continue
		}
		fields, ok := objectFields(item)
		if !ok || len(fields) != 1 {
			return fail(ErrQuery, "pull item %v is invalid; use an attribute, *, or nested ref object", item)
		}
		if err := validatePullAttribute(fields[0].Name); err != nil {
			return err
		}
		subpattern, ok := fields[0].Value.([]any)
		if !ok {
			return fail(ErrQuery, "nested pull pattern for %q has type %T; use an attribute array", fields[0].Name, fields[0].Value)
		}
		if err := validatePullPattern(subpattern); err != nil {
			return err
		}
	}
	return nil
}

func validatePullAttribute(attr string) error {
	forward := strings.Replace(attr, "/_", "/", 1)
	if !attributePattern.MatchString(forward) {
		return fail(ErrQuery, "pull attribute %q is invalid; use namespace/name or namespace/_name", attr)
	}
	return nil
}

func (e *queryEvaluator) pullPattern(entity int64, pattern []any, depth int, seen map[int64]bool) (map[string]any, error) {
	all := false
	attrs := map[string]any{}
	for _, item := range pattern {
		if item == "*" {
			all = true
			continue
		}
		if attr, ok := item.(string); ok {
			attrs[attr] = nil
			continue
		}
		fields, ok := objectFields(item)
		if !ok || len(fields) != 1 {
			return nil, fail(ErrQuery, "pull item %v is invalid; use an attribute, *, or nested ref object", item)
		}
		if strings.Contains(fields[0].Name, "/_") {
			return nil, fail(ErrQuery, "nested pull attribute %q is reverse; use it as a standalone reverse attribute", fields[0].Name)
		}
		attrID, declared := e.db.store.names[fields[0].Name]
		schema := attributeSchema{}
		if declared {
			var err error
			schema, err = e.db.schemaFor(e.ctx, e.runner, attrID, nil)
			if err != nil {
				return nil, err
			}
		}
		populated, allRefs := false, true
		if err := e.scanPullFacts(nil, fields[0].Name, nil, func(fact queryFact) error {
			populated = true
			allRefs = allRefs && fact.cell.tag == TagRef
			return nil
		}); err != nil {
			return nil, err
		}
		if schema.typeName != "" && schema.typeName != "ref" {
			return nil, fail(ErrQuery, "nested pull attribute %q declares %s values, not references", fields[0].Name, schema.typeName)
		}
		if schema.typeName == "" && (!populated || !allRefs) {
			return nil, fail(ErrQuery, "nested pull attribute %q is not a reference; declare it ref or store reference facts", fields[0].Name)
		}
		if !allRefs {
			return nil, fail(ErrQuery, "nested pull attribute %q has non-reference values; correct its schema and facts", fields[0].Name)
		}
		attrs[fields[0].Name] = fields[0].Value
	}
	full, err := e.db.pullEntity(e.ctx, e.runner, entity, 0, seen, e.spend)
	if err != nil {
		return nil, err
	}
	result := map[string]any{}
	if all {
		result = full
	}
	for attr, sub := range attrs {
		if strings.Contains(attr, "/_") {
			forward := strings.Replace(attr, "/_", "/", 1)
			entities := []any{}
			if err := e.scanPullFacts(nil, forward, &entity, func(fact queryFact) error {
				entities = append(entities, e.db.renderLogical(fact.e, TagRef))
				return nil
			}); err != nil {
				return nil, err
			}
			result[attr] = entities
			continue
		}
		value, exists := full[attr]
		if !exists {
			continue
		}
		if sub == nil {
			result[attr] = value
			continue
		}
		subPattern, ok := sub.([]any)
		if !ok {
			return nil, fail(ErrQuery, "nested pull pattern for %q has type %T; use an attribute array", attr, sub)
		}
		refs := []int64{}
		if err := e.scanPullFacts(&entity, attr, nil, func(fact queryFact) error {
			if fact.cell.tag == TagRef {
				refs = append(refs, asInt64(fact.cell.value))
			}
			return nil
		}); err != nil {
			return nil, err
		}
		nested := []any{}
		for _, ref := range refs {
			if seen[ref] || depth <= 0 {
				nested = append(nested, e.db.renderLogical(ref, TagRef))
				continue
			}
			seen[ref] = true
			pulled, err := e.pullPattern(ref, subPattern, depth-1, seen)
			delete(seen, ref)
			if err != nil {
				return nil, err
			}
			nested = append(nested, pulled)
		}
		attrID, exists := e.db.store.names[attr]
		schema := attributeSchema{}
		if exists {
			var err error
			schema, err = e.db.schemaFor(e.ctx, e.runner, attrID, nil)
			if err != nil {
				return nil, err
			}
		}
		if len(nested) == 1 && !schema.many {
			result[attr] = nested[0]
		} else {
			result[attr] = nested
		}
	}
	return result, nil
}

func (e *queryEvaluator) scanPullFacts(
	entity *int64,
	attribute string,
	reference *int64,
	visit func(queryFact) error,
) (resultErr error) {
	attributeID, found := e.db.store.names[attribute]
	if !found {
		return nil
	}
	query, alias, args, err := e.patternSQLBase()
	if err != nil {
		return err
	}
	query += " AND " + alias + ".a=?"
	args = append(args, attributeID)
	if entity != nil {
		query += " AND " + alias + ".e=?"
		args = append(args, *entity)
	}
	if reference != nil {
		query += " AND " + alias + ".v=? AND " + alias + ".t=?"
		args = append(args, *reference, TagRef)
	}
	query += " ORDER BY dtx,added," + alias + ".e," + alias + ".id"
	rows, err := e.runner.QueryContext(e.ctx, query, args...)
	if err != nil {
		return wrap(ErrFormat, err, "cannot scan pull facts for %q", attribute)
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "pull fact rows")) }()
	for rows.Next() {
		if err := e.spend(); err != nil {
			return err
		}
		var raw any
		var tag Tag
		var fact queryFact
		var added int64
		if err := rows.Scan(&raw, &tag, &fact.e, &fact.aID, &fact.a, &fact.tx, &added); err != nil {
			return wrap(ErrFormat, err, "cannot decode pull fact for %q", attribute)
		}
		logical, err := e.db.logicalValue(e.ctx, e.runner, raw, tag)
		if err != nil {
			return err
		}
		fact.cell, fact.added = cell{tag: tag, value: logical}, added != 0
		if err := visit(fact); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return wrap(ErrFormat, err, "cannot finish pull facts for %q", attribute)
	}
	return nil
}

func (e *queryEvaluator) aggregateRow(find []any, values []binding) ([]any, []cell, error) {
	row := []any{}
	cells := []cell{}
	for _, item := range find {
		name := findAggregate(item)
		if name == "" {
			projected, projectedCells, err := e.projectRow([]any{item}, values[0])
			if err != nil {
				return nil, nil, err
			}
			row, cells = append(row, projected[0]), append(cells, projectedCells[0])
			continue
		}
		items, ok := item.([]any)
		if !ok || len(items) != 2 {
			return nil, nil, fail(ErrQuery, "aggregate find item %v is invalid; use [aggregate,variable]", item)
		}
		variable, ok := items[1].(string)
		if !ok {
			return nil, nil, fail(ErrQuery, "aggregate %q argument must be a variable", name)
		}
		collected := []cell{}
		for _, binding := range values {
			if value, found := binding[variable]; found {
				collected = append(collected, value)
			}
		}
		value, err := aggregate(name, collected)
		if err != nil {
			return nil, nil, err
		}
		row = append(row, e.db.renderLogical(value.value, value.tag))
		cells = append(cells, value)
	}
	return row, cells, nil
}

func aggregate(name string, values []cell) (cell, error) {
	switch name {
	case "count":
		return cell{tag: TagInt, value: int64(len(values))}, nil
	case "count-distinct":
		seen := map[string]bool{}
		for _, value := range values {
			key, err := structuralCellKey(value)
			if err != nil {
				return cell{}, err
			}
			seen[key] = true
		}
		return cell{tag: TagInt, value: int64(len(seen))}, nil
	case "sum", "avg":
		if len(values) == 0 {
			return cell{tag: queryNullTag}, nil
		}
		allInt := name == "sum"
		for _, value := range values {
			if value.tag != TagInt && value.tag != TagFloat {
				return cell{}, fail(ErrQuery, "%s requires numeric values; bind it to int or float facts", name)
			}
			if value.tag != TagInt {
				allInt = false
			}
		}
		if allInt {
			integerTotal := new(big.Int)
			for _, value := range values {
				integer, ok := value.value.(int64)
				if !ok {
					return cell{}, fail(ErrQuery, "%s requires numeric values; bind it to int or float facts", name)
				}
				integerTotal.Add(integerTotal, big.NewInt(integer))
			}
			if !integerTotal.IsInt64() {
				return cell{}, fail(ErrQuery, "sum exceeds signed 64-bit integer range; aggregate fewer values or store a deliberate float")
			}
			return cell{tag: TagInt, value: integerTotal.Int64()}, nil
		}
		floatTotal := float64(0)
		for _, value := range values {
			number, ok := numeric(value.value)
			if !ok {
				return cell{}, fail(ErrQuery, "%s requires numeric values; bind it to int or float facts", name)
			}
			floatTotal += number
		}
		if math.IsNaN(floatTotal) || math.IsInf(floatTotal, 0) {
			return cell{}, fail(ErrQuery, "%s produces a non-finite result; aggregate smaller finite values", name)
		}
		if name == "avg" {
			return cell{tag: TagFloat, value: floatTotal / float64(len(values))}, nil
		}
		return cell{tag: TagFloat, value: floatTotal}, nil
	case "min", "max":
		if len(values) == 0 {
			return cell{tag: queryNullTag}, nil
		}
		best := values[0]
		if (best.tag != TagInt && best.tag != TagFloat) || !isFiniteNumericCell(best) {
			return cell{}, fail(ErrQuery, "%s requires numeric values; bind it to int or float facts", name)
		}
		for _, value := range values[1:] {
			if (value.tag != TagInt && value.tag != TagFloat) || !isFiniteNumericCell(value) {
				return cell{}, fail(ErrQuery, "%s requires numeric values; bind it to int or float facts", name)
			}
			comparison, ok := orderedCompare(value.value, best.value)
			if !ok {
				return cell{}, fail(ErrQuery, "%s values are not mutually comparable", name)
			}
			if (name == "min" && comparison < 0) || (name == "max" && comparison > 0) {
				best = value
			}
		}
		return best, nil
	default:
		return cell{}, fail(ErrQuery, "unknown aggregate %q", name)
	}
}

func isFiniteNumericCell(value cell) bool {
	number, ok := numeric(value.value)
	return ok && !math.IsNaN(number) && !math.IsInf(number, 0)
}

type orderItem struct {
	variable string
	index    int
	desc     bool
}

func parseOrderBound(raw []any, columns []string, bound map[string]bool, aggregate bool) ([]orderItem, error) {
	result := []orderItem{}
	for _, value := range raw {
		item, ok := value.([]any)
		if !ok || len(item) != 2 {
			return nil, fail(ErrQuery, "order item %v is invalid; use [find-item, asc|desc]", value)
		}
		name, nameOK := item[0].(string)
		direction, directionOK := item[1].(string)
		if !nameOK || !directionOK || (direction != "asc" && direction != "desc") {
			return nil, fail(ErrQuery, "order item %v is invalid; use [find-item, asc|desc]", value)
		}
		index := -1
		for i, column := range columns {
			if column == name {
				index = i
				break
			}
		}
		if index < 0 {
			if aggregate || !bound[name] {
				return nil, fail(ErrQuery, "order variable %q is neither projected nor safely bound; add it to find or bind it in where", name)
			}
		}
		result = append(result, orderItem{index: index, variable: name, desc: direction == "desc"})
	}
	return result, nil
}

func (e *queryEvaluator) parseRules(raw []any) error {
	e.rules = map[string][]ruleDef{}
	for _, item := range raw {
		value, ok := objectMap(item)
		if !ok {
			return fail(ErrQuery, "rule definition has type %T; use {head:[...],body:[...]}", item)
		}
		if len(value) != 2 {
			return fail(ErrQuery, "rule definition has fields %v; use exactly head and body", sortedFields(value))
		}
		head, headOK := value["head"].([]any)
		body, bodyOK := value["body"].([]any)
		if !headOK || !bodyOK || len(head) < 1 {
			return fail(ErrQuery, "rule definition needs non-empty head and body arrays")
		}
		name, ok := head[0].(string)
		if !ok || name == "" {
			return fail(ErrQuery, "rule head name must be non-empty text")
		}
		args := make([]string, 0, len(head)-1)
		for _, arg := range head[1:] {
			variable, ok := arg.(string)
			if !ok || !strings.HasPrefix(variable, "?") {
				return fail(ErrQuery, "rule %q head arguments must be variables", name)
			}
			args = append(args, variable)
		}
		if definitions := e.rules[name]; len(definitions) > 0 && len(definitions[0].args) != len(args) {
			return fail(ErrQuery, "rule %q definitions have different arities %d and %d; use one arity per name", name, len(definitions[0].args), len(args))
		}
		bodyBound := map[string]bool{}
		if err := validateClauseSequence(body, bodyBound); err != nil {
			return wrap(ErrQuery, err, "rule %q body is unsafe", name)
		}
		for _, variable := range args {
			if !bodyBound[variable] {
				return fail(ErrQuery, "rule %q head variable %q is unbound in its body", name, variable)
			}
		}
		e.rules[name] = append(e.rules[name], ruleDef{name: name, args: args, body: body})
	}
	return nil
}

func (e *queryEvaluator) buildRelations() error {
	dependencies := map[string]map[string]bool{}
	for name, definitions := range e.rules {
		dependencies[name] = map[string]bool{}
		for _, definition := range definitions {
			collectRuleCalls(definition.body, dependencies[name])
		}
	}
	visiting, done := map[string]bool{}, map[string]bool{}
	var build func(string) error
	build = func(name string) error {
		if done[name] {
			return nil
		}
		if visiting[name] {
			return fail(ErrQuery, "rules are mutually recursive through %q; use self-recursion only in v1", name)
		}
		visiting[name] = true
		dependencyNames := make([]string, 0, len(dependencies[name]))
		for dependency := range dependencies[name] {
			dependencyNames = append(dependencyNames, dependency)
		}
		sort.Strings(dependencyNames)
		for _, dependency := range dependencyNames {
			if dependency == name {
				continue
			}
			if _, exists := e.rules[dependency]; !exists {
				return fail(ErrQuery, "rule %q invokes undefined rule %q; add its definition", name, dependency)
			}
			if err := build(dependency); err != nil {
				return err
			}
		}
		delete(visiting, name)
		if err := e.deriveRelation(name); err != nil {
			return err
		}
		done[name] = true
		return nil
	}
	names := make([]string, 0, len(e.rules))
	for name := range e.rules {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := build(name); err != nil {
			return err
		}
	}
	return nil
}

func collectRuleCalls(value any, target map[string]bool) {
	fields, ok := objectFields(value)
	if ok {
		for _, field := range fields {
			if field.Name == "rule" {
				if invocation, ok := field.Value.([]any); ok && len(invocation) > 0 {
					if name, ok := invocation[0].(string); ok {
						target[name] = true
					}
				}
			}
			collectRuleCalls(field.Value, target)
		}
		return
	}
	if items, ok := value.([]any); ok {
		for _, item := range items {
			collectRuleCalls(item, target)
		}
	}
}

func (e *queryEvaluator) deriveRelation(name string) error {
	rows := [][]cell{}
	seen := map[string]bool{}
	for {
		e.relations[name] = rows
		changed := false
		for _, definition := range e.rules[name] {
			bindings, err := e.evalClauses(definition.body, []binding{{}})
			if err != nil {
				return err
			}
			for _, values := range bindings {
				tuple := make([]cell, len(definition.args))
				var key strings.Builder
				for i, variable := range definition.args {
					value, found := values[variable]
					if !found {
						return fail(ErrQuery, "rule %q head variable %q is unbound in its body", name, variable)
					}
					tuple[i] = value
					cellKey, keyErr := structuralCellKey(value)
					if keyErr != nil {
						return keyErr
					}
					appendKeyPart(&key, cellKey)
				}
				if !seen[key.String()] {
					seen[key.String()] = true
					rows = append(rows, tuple)
					changed = true
				}
			}
		}
		if !changed {
			break
		}
	}
	e.relations[name] = rows
	return nil
}

func (e *queryEvaluator) evalRule(invocation []any, input []binding) ([]binding, error) {
	if len(invocation) < 1 {
		return nil, fail(ErrQuery, "rule invocation is empty; use [name,args...]")
	}
	name, ok := invocation[0].(string)
	if !ok {
		return nil, fail(ErrQuery, "rule name has type %T; use text", invocation[0])
	}
	definitions, exists := e.rules[name]
	if !exists {
		return nil, fail(ErrQuery, "rule %q is undefined; add it under rules", name)
	}
	if len(invocation)-1 != len(definitions[0].args) {
		return nil, fail(ErrQuery, "rule %q expects %d arguments, got %d", name, len(definitions[0].args), len(invocation)-1)
	}
	output := []binding{}
	for _, values := range input {
		for _, tuple := range e.relations[name] {
			if err := e.spend(); err != nil {
				return nil, err
			}
			next := cloneBinding(values)
			matched := true
			for i, term := range invocation[1:] {
				ok, err := e.matchTerm(term, tuple[i], next, tuple[i].tag == TagRef)
				if err != nil {
					return nil, err
				}
				if !ok {
					matched = false
					break
				}
			}
			if matched {
				output = append(output, next)
			}
		}
	}
	return distinctBindings(output)
}
