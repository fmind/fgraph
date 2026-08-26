package fgraph

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

const maxDatomPage = 1000

const maxDatomCursorBytes = 4096

type datomCursor struct {
	Seek   *datomSeek `json:"seek,omitempty"`
	Digest string     `json:"digest"`
	Index  string     `json:"index"`
	Source string     `json:"source"`
	Basis  int64      `json:"basis"`
	Format int        `json:"format"`
}

type datomSeekValue struct {
	Kind  string  `json:"kind"`
	Text  string  `json:"text,omitempty"`
	Int   int64   `json:"int,omitempty"`
	Float float64 `json:"float,omitempty"`
}

type datomSeek struct {
	V     datomSeekValue `json:"v"`
	E     int64          `json:"e"`
	A     int64          `json:"a"`
	T     int64          `json:"t"`
	Tx    int64          `json:"tx"`
	Added int64          `json:"added"`
	ID    int64          `json:"id"`
}

type datomRaw struct {
	v                    any
	attr                 string
	id, e, a, t, tx, add int64
}

func seekFromDatom(item datomRaw) *datomSeek {
	return &datomSeek{
		V: encodeDatomSeekValue(item.v), E: item.e, A: item.a, T: item.t,
		Tx: item.tx, Added: item.add, ID: item.id,
	}
}

func encodeDatomSeekValue(value any) datomSeekValue {
	switch value := value.(type) {
	case int64:
		return datomSeekValue{Kind: "int", Int: value}
	case float64:
		return datomSeekValue{Kind: "float", Float: value}
	case string:
		return datomSeekValue{Kind: "text", Text: value}
	case []byte:
		return datomSeekValue{Kind: "blob", Text: hex.EncodeToString(value)}
	default:
		return datomSeekValue{Kind: "invalid"}
	}
}

func (value datomSeekValue) decode() (any, error) {
	switch value.Kind {
	case "int":
		return value.Int, nil
	case "float":
		return value.Float, nil
	case "text":
		return value.Text, nil
	case "blob":
		decoded, err := hex.DecodeString(value.Text)
		if err != nil {
			return nil, wrap(ErrType, err, "datom cursor has malformed blob seek value")
		}
		return decoded, nil
	default:
		return nil, fail(ErrType, "datom cursor has unknown seek value kind %q", value.Kind)
	}
}

func (seek *datomSeek) arguments(index string, value any) []any {
	switch index {
	case "avet":
		return []any{seek.A, value, seek.E, seek.T, seek.Tx, seek.Added, seek.ID}
	case "vaet":
		return []any{value, seek.A, seek.E, seek.T, seek.Tx, seek.Added, seek.ID}
	default:
		return []any{seek.E, seek.A, value, seek.T, seek.Tx, seek.Added, seek.ID}
	}
}

func (db *DB) Datoms(ctx context.Context, options DatomOptions) (DatomPage, error) {
	if options.Index == "" {
		options.Index = "eavt"
	}
	if options.Source == "" {
		options.Source = "current"
	}
	if options.Index != "eavt" && options.Index != "avet" && options.Index != "vaet" {
		return DatomPage{}, fail(ErrQuery, "datom index %q is invalid; use eavt, avet, or vaet", options.Index)
	}
	if options.Source != "current" && options.Source != "history" {
		return DatomPage{}, fail(ErrQuery, "datom source %q is invalid; use current or history", options.Source)
	}
	if len(options.Components) > 5 {
		return DatomPage{}, fail(ErrQuery, "datom components has %d items; use at most the five-position logical index prefix", len(options.Components))
	}
	if options.Limit == 0 {
		options.Limit = 100
	}
	if options.Limit < 1 || options.Limit > maxDatomPage {
		return DatomPage{}, fail(ErrTooLarge, "datom limit %d is invalid; use 1..%d", options.Limit, maxDatomPage)
	}
	digest, digestErr := datomArgumentsDigest(options)
	if digestErr != nil {
		return DatomPage{}, digestErr
	}
	page := DatomPage{Items: []Datom{}}
	readErr := db.withRead(ctx, func(runner sqlRunner) error {
		currentBasis, basisErr := db.basisOn(ctx, runner)
		if basisErr != nil {
			return basisErr
		}
		basis := currentBasis
		if db.asOf != nil && *db.asOf < basis {
			basis = *db.asOf
		}
		var seek *datomSeek
		if options.Cursor != "" {
			cursor, cursorErr := decodeDatomCursor(options.Cursor)
			if cursorErr != nil {
				return cursorErr
			}
			if cursor.Digest != digest || cursor.Index != options.Index || cursor.Source != options.Source {
				return fail(ErrConflict, "datom cursor belongs to different arguments; restart without a cursor")
			}
			if cursor.Basis > currentBasis || (db.asOf != nil && cursor.Basis > *db.asOf) {
				return fail(ErrConflict, "datom cursor basis %d is not visible in this view", cursor.Basis)
			}
			basis, seek = cursor.Basis, cursor.Seek
		}
		page.BasisTx = basis
		constraints, args, found, constraintErr := db.datomConstraints(ctx, runner, options.Index, options.Components)
		if constraintErr != nil || !found {
			return constraintErr
		}
		raw, pageErr := db.readDatomPage(ctx, runner, options, basis, currentBasis, constraints, args, seek)
		if pageErr != nil {
			return pageErr
		}
		hasMore := len(raw) > options.Limit
		if hasMore {
			raw = raw[:options.Limit]
		}
		for _, item := range raw {
			logical, logicalErr := db.logicalValue(ctx, runner, item.v, Tag(item.t))
			if logicalErr != nil {
				return logicalErr
			}
			page.Items = append(page.Items, Datom{
				E: db.displayEntity(item.e), A: item.attr,
				V:  db.renderLogical(logical, Tag(item.t)),
				Tx: item.tx, Added: item.add != 0, FactID: item.id,
			})
		}
		if hasMore {
			last := raw[len(raw)-1]
			var cursorErr error
			page.NextCursor, cursorErr = encodeDatomCursor(datomCursor{
				Digest: digest, Index: options.Index, Source: options.Source,
				Basis: basis, Format: FormatVersion, Seek: seekFromDatom(last),
			})
			return cursorErr
		}
		return nil
	})
	return page, readErr
}

func datomArgumentsDigest(options DatomOptions) (string, error) {
	encoded, err := canonicalJSON(map[string]any{
		"index": options.Index, "source": options.Source, "components": wireValue(options.Components),
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func encodeDatomCursor(cursor datomCursor) (string, error) {
	encoded, err := json.Marshal(cursor)
	if err != nil {
		return "", wrap(ErrFormat, err, "cannot encode datom cursor")
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeDatomCursor(value string) (datomCursor, error) {
	if len(value) > maxDatomCursorBytes {
		return datomCursor{}, fail(ErrTooLarge, "datom cursor is %d bytes; restart pagination without a cursor", len(value))
	}
	encoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return datomCursor{}, wrap(ErrType, err, "datom cursor is malformed")
	}
	var cursor datomCursor
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil || cursor.Format != FormatVersion || cursor.Basis < GenesisTx || cursor.Seek == nil {
		return datomCursor{}, wrap(ErrType, err, "datom cursor is malformed")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return datomCursor{}, wrap(ErrType, err, "datom cursor has trailing data")
	}
	return cursor, nil
}

func (db *DB) datomConstraints(
	ctx context.Context,
	runner sqlRunner,
	index string,
	components []any,
) ([]string, []any, bool, error) {
	positions := map[string][]string{
		"eavt": {"e", "a", "v", "tx", "added"},
		"avet": {"a", "v", "e", "tx", "added"},
		"vaet": {"v", "a", "e", "tx", "added"},
	}[index]
	constraints := []string{}
	args := []any{}
	if index == "vaet" {
		constraints = append(constraints, "d.t=0")
	}
	for position, input := range components {
		field := positions[position]
		switch field {
		case "e", "tx":
			id, found, err := db.resolveReadEntity(ctx, runner, input)
			if err != nil || !found {
				return nil, nil, found, err
			}
			column := field
			if field == "tx" {
				// The current/history union exposes logical transaction time as
				// dtx so assertion and retraction events share one indexed position.
				column = "dtx"
			}
			constraints, args = append(constraints, "d."+column+"=?"), append(args, id)
		case "a":
			name, ok := input.(string)
			if !ok || !attributePattern.MatchString(name) {
				return nil, nil, false, fail(ErrQuery, "datom attribute component %v is invalid", input)
			}
			id, found := db.store.names[name]
			if !found {
				return constraints, args, false, nil
			}
			constraints, args = append(constraints, "d.a=?"), append(args, id)
		case "v":
			stored, err := scalarValue(input)
			if err != nil {
				return nil, nil, false, wrap(ErrQuery, err, "invalid datom value component")
			}
			if index == "vaet" && stored.tag != TagRef {
				return nil, nil, false, fail(ErrQuery, "vaet value component must be {ref:selector}")
			}
			if stored.tag == TagRef {
				ref, ok := stored.logical.(RefValue)
				if !ok {
					return nil, nil, false, fail(ErrFormat, "datom ref value has logical type %T", stored.logical)
				}
				id, found, err := db.resolveReadEntity(ctx, runner, ref.Target)
				if err != nil || !found {
					return nil, nil, found, err
				}
				stored.storage = id
			}
			constraints = append(constraints, "d.v=?", "d.t=?")
			args = append(args, stored.storage, stored.tag)
		case "added":
			added, ok := input.(bool)
			if !ok {
				return nil, nil, false, fail(ErrQuery, "datom added component %v is invalid; use true or false", input)
			}
			constraints = append(constraints, "d.added=?")
			if added {
				args = append(args, int64(1))
			} else {
				args = append(args, int64(0))
			}
		}
	}
	return constraints, args, true, nil
}

func (db *DB) readDatomPage(
	ctx context.Context,
	runner sqlRunner,
	options DatomOptions,
	basis, currentBasis int64,
	constraints []string,
	args []any,
	seek *datomSeek,
) (result []datomRaw, resultErr error) {
	base := ""
	baseArgs := []any{}
	if options.Source == "current" {
		indexHint := ""
		if basis == currentBasis {
			indexHint = " INDEXED BY fgraph_" + options.Index
			base = "SELECT f.id,f.e,f.a,f.v,f.t,f.tx AS dtx,1 AS added FROM fgraph_facts f" + indexHint + " WHERE f.rx IS NULL"
			if options.Index == "vaet" {
				base += " AND f.t=0"
			}
		} else {
			base = "SELECT f.id,f.e,f.a,f.v,f.t,f.tx AS dtx,1 AS added FROM fgraph_facts f WHERE f.tx<=? AND (f.rx IS NULL OR f.rx>?)"
			baseArgs = append(baseArgs, basis, basis)
		}
	} else {
		base = `SELECT f.id,f.e,f.a,f.v,f.t,f.tx AS dtx,1 AS added FROM fgraph_facts f WHERE f.tx<=?
			UNION ALL
			SELECT f.id,f.e,f.a,f.v,f.t,f.rx AS dtx,0 AS added FROM fgraph_facts f WHERE f.rx IS NOT NULL AND f.rx<=?`
		baseArgs = append(baseArgs, basis, basis)
	}
	order := map[string]string{
		"eavt": "d.e,d.a,d.v,d.t,d.dtx,d.added,d.id",
		"avet": "d.a,d.v,d.e,d.t,d.dtx,d.added,d.id",
		"vaet": "d.v,d.a,d.e,d.t,d.dtx,d.added,d.id",
	}[options.Index]
	seekColumns := map[string]string{
		"eavt": "d.e,d.a,d.v,d.t,d.dtx,d.added,d.id",
		"avet": "d.a,d.v,d.e,d.t,d.dtx,d.added,d.id",
		"vaet": "d.v,d.a,d.e,d.t,d.dtx,d.added,d.id",
	}[options.Index]
	whereParts := append([]string(nil), constraints...)
	queryArgs := append(append([]any(nil), baseArgs...), args...)
	if seek != nil {
		value, err := seek.V.decode()
		if err != nil {
			return nil, err
		}
		whereParts = append(whereParts, "("+seekColumns+")>(?,?,?,?,?,?,?)")
		queryArgs = append(queryArgs, seek.arguments(options.Index, value)...)
	}
	where := ""
	if len(whereParts) > 0 {
		where = " WHERE " + strings.Join(whereParts, " AND ")
	}
	query := "SELECT d.id,d.e,d.a,d.v,d.t,d.dtx,d.added,i.name FROM (" + base + ") d JOIN fgraph_ids i ON i.id=d.a" + where + " ORDER BY " + order + " LIMIT ?"
	queryArgs = append(queryArgs, options.Limit+1)
	rows, err := runner.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot read %s datoms", options.Index)
	}
	defer func() {
		resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "datom rows"))
	}()
	result = []datomRaw{}
	for rows.Next() {
		var item datomRaw
		if err := rows.Scan(&item.id, &item.e, &item.a, &item.v, &item.t, &item.tx, &item.add, &item.attr); err != nil {
			return nil, wrap(ErrFormat, err, "cannot decode datom")
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, wrap(ErrFormat, err, "cannot finish reading datoms")
	}
	return result, nil
}

func (db *DB) Explain(ctx context.Context, query Q, args map[string]any) (ExplainPlan, error) {
	if query.Source == "" {
		query.Source = "current"
	}
	if err := validateClauseBindings(query.Where, query.In); err != nil {
		return ExplainPlan{}, err
	}
	plan := ExplainPlan{Source: query.Source, WorkLimit: db.store.queryBudget, Clauses: []ExplainClause{}, Warnings: []string{}}
	err := db.withRead(ctx, func(runner sqlRunner) error {
		basis, err := db.basisOn(ctx, runner)
		if err != nil {
			return err
		}
		if db.asOf != nil && *db.asOf < basis {
			basis = *db.asOf
		}
		plan.BasisTx = basis
		bound := map[string]bool{}
		for _, variable := range query.In {
			if _, found := args[variable]; found {
				bound[variable] = true
			}
		}
		for _, ordered := range orderQueryClauses(query.Where, bound) {
			ordinal, raw := ordered.ordinal, ordered.value
			boundList := make([]string, 0, len(bound))
			for variable := range bound {
				boundList = append(boundList, variable)
			}
			sort.Strings(boundList)
			items, ok := raw.([]any)
			if !ok || isPredicateClause(items) {
				plan.Clauses = append(plan.Clauses, ExplainClause{Ordinal: ordinal, Kind: "barrier", Access: "binding", Bound: boundList})
				fields, object := objectFields(raw)
				if object && len(fields) == 1 && fields[0].Name != "not" {
					if values, ok := fields[0].Value.([]any); ok {
						for _, variable := range clauseVariables(values) {
							bound[variable] = true
						}
					}
				}
				continue
			}
			_, access := patternAccessRank(items, bound)
			variables := clauseVariables(items)
			for _, variable := range variables {
				bound[variable] = true
			}
			plan.Clauses = append(plan.Clauses, ExplainClause{Ordinal: ordinal, Kind: "pattern", Access: access, Bound: boundList})
			if access == "scan" {
				plan.Warnings = append(plan.Warnings, fmt.Sprintf("clause %d requires a bounded datom scan", ordinal))
			}
		}
		return nil
	})
	return plan, err
}

func (db *DB) ExplainJSON(ctx context.Context, value any, args map[string]any) (ExplainPlan, error) {
	query, err := ParseQuery(value)
	if err != nil {
		return ExplainPlan{}, err
	}
	return db.Explain(ctx, query, args)
}
