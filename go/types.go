package fgraph

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"time"
)

const (
	ApplicationID        = 0x66677261
	FormatVersion        = 2
	BlobThreshold        = 256
	MaxValueBytes        = 1_048_576
	MaxJSONDepth         = 64
	MaxJSONDocumentDepth = 80
	GenesisTx            = 64
	GenesisFactCount     = 39
	FirstUserID          = 65
	Version              = "1.0.2"
	DefaultQueryBudget   = 100_000
	MaxMCPOutputBytes    = 256 << 10
)

type Tag int64

const (
	TagRef Tag = iota
	TagBool
	TagInt
	TagFloat
	TagText
	TagInstant
	TagBytes
	TagVector
	TagTextRef
	TagBytesRef
	TagJSON
)

var tagNames = [...]string{
	"ref", "bool", "int", "float", "text", "instant", "bytes", "vector", "text_ref", "bytes_ref", "json",
}

type E map[string]any

type (
	RefValue     struct{ Target any }
	InstantValue struct{ Micros int64 }
	BytesValue   []byte
	VectorValue  []float32
	JSONValue    struct{ Value any }
	TempID       string
)

func RefTo(target any) RefValue          { return RefValue{Target: target} }
func Instant(micros int64) InstantValue  { return InstantValue{Micros: micros} }
func Bytes(value []byte) BytesValue      { return BytesValue(value) }
func Vector(value []float32) VectorValue { return VectorValue(value) }
func JSON(value any) JSONValue           { return JSONValue{Value: value} }
func Tmp(name string) TempID             { return TempID(name) }

type Fact struct {
	V                any            `json:"v"`
	E                any            `json:"e"`
	Rx               *int64         `json:"rx"`
	Provenance       map[string]any `json:"provenance,omitempty"`
	RxSource         string         `json:"rx_source,omitempty"`
	By               string         `json:"by,omitempty"`
	Source           string         `json:"source,omitempty"`
	RxBy             string         `json:"rx_by,omitempty"`
	A                string         `json:"a"`
	Snippet          string         `json:"snippet,omitempty"`
	Tag              Tag            `json:"-"`
	Tx               int64          `json:"tx"`
	ID               int64          `json:"id"`
	At               int64          `json:"at,omitempty"`
	RxAt             int64          `json:"rx_at,omitempty"`
	SnippetTruncated bool           `json:"snippet_truncated,omitempty"`
	ValueTruncated   bool           `json:"value_truncated,omitempty"`
	presence         factPresence
}

type factPresence uint8

const (
	factAtPresent factPresence = 1 << iota
	factByPresent
	factSourcePresent
	factRxAtPresent
	factRxByPresent
	factRxSourcePresent
)

func (fact Fact) MarshalJSON() ([]byte, error) {
	rendered := map[string]any{
		"id": fact.ID, "e": fact.E, "a": fact.A, "v": fact.V,
		"tx": fact.Tx, "rx": fact.Rx,
	}
	if len(fact.Provenance) > 0 {
		rendered["provenance"] = fact.Provenance
	}
	if fact.Snippet != "" {
		rendered["snippet"] = fact.Snippet
	}
	if fact.SnippetTruncated {
		rendered["snippet_truncated"] = true
	}
	if fact.ValueTruncated {
		rendered["value_truncated"] = true
	}
	for name, optional := range map[string]struct {
		value   string
		present factPresence
	}{
		"by": {fact.By, factByPresent}, "source": {fact.Source, factSourcePresent},
		"rx_by": {fact.RxBy, factRxByPresent}, "rx_source": {fact.RxSource, factRxSourcePresent},
	} {
		if optional.value != "" || fact.presence&optional.present != 0 {
			rendered[name] = optional.value
		}
	}
	if fact.At != 0 || fact.presence&factAtPresent != 0 {
		rendered["at"] = fact.At
	}
	if fact.RxAt != 0 || fact.presence&factRxAtPresent != 0 {
		rendered["rx_at"] = fact.RxAt
	}
	return json.Marshal(rendered)
}

type TxReport struct {
	IDs       map[string]int64 `json:"ids"`
	Status    string           `json:"status"`
	EventID   string           `json:"event,omitempty"`
	Asserted  []Fact           `json:"asserted"`
	Retracted []Fact           `json:"retracted"`
	BasisTx   int64            `json:"basis_tx"`
	Tx        int64            `json:"tx"`
	At        int64            `json:"at,omitempty"`
}

type ApplySummary struct {
	Events         int64 `json:"events"`
	Applied        int64 `json:"applied"`
	AlreadyApplied int64 `json:"already_applied"`
	Noop           int64 `json:"noop"`
	BasisTx        int64 `json:"basis_tx"`
}

// EventReceipt is the durable control-plane metadata for one committed event.
// Hashes use the explicit sha256:<lowercase-hex> wire representation.
type EventReceipt struct {
	By          *string `json:"by,omitempty"`
	Source      *string `json:"source,omitempty"`
	OperationID *string `json:"operation_id"`
	RequestHash *string `json:"request_hash"`
	ImportedAt  *int64  `json:"imported_at,omitempty"`
	Meta        *any    `json:"meta,omitempty"`
	EventHash   string  `json:"event_hash"`
	Event       string  `json:"event"`
	Facts       []Fact  `json:"facts"`
	ReadBasisTx int64   `json:"read_basis_tx"`
	BasisTx     int64   `json:"basis_tx"`
	Tx          int64   `json:"tx"`
	At          int64   `json:"at"`
}

func (r TxReport) MarshalJSON() ([]byte, error) {
	var tx, at, event any
	if r.Tx != 0 {
		tx = r.Tx
		at = r.At
		event = r.EventID
	}
	status := r.Status
	if status == "" {
		status = "noop"
		if r.Tx != 0 {
			status = "applied"
		}
	}
	return marshalOrderedObject([]Field{
		{Name: "tx", Value: tx},
		{Name: "at", Value: at},
		{Name: "event", Value: event},
		{Name: "basis_tx", Value: r.BasisTx},
		{Name: "ids", Value: r.IDs},
		{Name: "asserted", Value: r.Asserted},
		{Name: "retracted", Value: r.Retracted},
		{Name: "status", Value: status},
	})
}

func (receipt EventReceipt) MarshalJSON() ([]byte, error) {
	return marshalOrderedObject(eventReceiptFields(receipt))
}

func compactOptionalFields(fields ...Field) []Field {
	return slices.DeleteFunc(fields, func(field Field) bool {
		return reflect.ValueOf(field.Value).IsNil()
	})
}

func eventReceiptFields(receipt EventReceipt) []Field {
	// Every optional value is a typed pointer; filtering them together keeps the
	// normative wire order without coupling it to the aligned public struct.
	fields := compactOptionalFields(
		Field{Name: "meta", Value: receipt.Meta},
		Field{Name: "by", Value: receipt.By},
		Field{Name: "source", Value: receipt.Source},
	)
	fields = append(fields,
		Field{Name: "operation_id", Value: receipt.OperationID},
		Field{Name: "request_hash", Value: receipt.RequestHash},
	)
	fields = append(fields, compactOptionalFields(Field{Name: "imported_at", Value: receipt.ImportedAt})...)
	fields = append(fields,
		Field{Name: "facts", Value: receipt.Facts},
		Field{Name: "event", Value: receipt.Event},
		Field{Name: "event_hash", Value: receipt.EventHash},
		Field{Name: "read_basis_tx", Value: receipt.ReadBasisTx},
		Field{Name: "basis_tx", Value: receipt.BasisTx},
		Field{Name: "tx", Value: receipt.Tx},
		Field{Name: "at", Value: receipt.At},
	)
	return fields
}

type Result struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

type Datom struct {
	E      any    `json:"e"`
	V      any    `json:"v"`
	A      string `json:"a"`
	Tx     int64  `json:"tx"`
	FactID int64  `json:"fact_id"`
	Added  bool   `json:"added"`
}

func (datom Datom) MarshalJSON() ([]byte, error) {
	return marshalOrderedObject([]Field{
		{Name: "e", Value: datom.E},
		{Name: "a", Value: datom.A},
		{Name: "v", Value: datom.V},
		{Name: "tx", Value: datom.Tx},
		{Name: "added", Value: datom.Added},
		{Name: "fact_id", Value: datom.FactID},
	})
}

type DatomOptions struct {
	Index      string `json:"index"`
	Source     string `json:"source,omitempty"`
	Cursor     string `json:"cursor,omitempty"`
	Components []any  `json:"components,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}

type DatomPage struct {
	NextCursor string  `json:"-"`
	Items      []Datom `json:"items"`
	BasisTx    int64   `json:"basis_tx"`
}

func (page DatomPage) MarshalJSON() ([]byte, error) {
	var cursor any
	if page.NextCursor != "" {
		cursor = page.NextCursor
	}
	return marshalOrderedObject([]Field{
		{Name: "next_cursor", Value: cursor},
		{Name: "basis_tx", Value: page.BasisTx},
		{Name: "items", Value: page.Items},
	})
}

type ExplainClause struct {
	Kind    string   `json:"kind"`
	Access  string   `json:"access"`
	Bound   []string `json:"bound"`
	Ordinal int      `json:"ordinal"`
}

func (clause ExplainClause) MarshalJSON() ([]byte, error) {
	return marshalOrderedObject([]Field{
		{Name: "bound", Value: clause.Bound},
		{Name: "kind", Value: clause.Kind},
		{Name: "access", Value: clause.Access},
		{Name: "ordinal", Value: clause.Ordinal},
	})
}

type ExplainPlan struct {
	Source    string          `json:"source"`
	Clauses   []ExplainClause `json:"clauses"`
	Warnings  []string        `json:"warnings"`
	BasisTx   int64           `json:"basis_tx"`
	WorkLimit int             `json:"work_limit"`
}

func (plan ExplainPlan) MarshalJSON() ([]byte, error) {
	return marshalOrderedObject([]Field{
		{Name: "source", Value: plan.Source},
		{Name: "basis_tx", Value: plan.BasisTx},
		{Name: "work_limit", Value: plan.WorkLimit},
		{Name: "clauses", Value: plan.Clauses},
		{Name: "warnings", Value: plan.Warnings},
	})
}

type Q struct {
	Limit  *int     `json:"limit,omitempty"`
	Source string   `json:"source,omitempty"`
	Find   []any    `json:"find"`
	Where  []any    `json:"where"`
	In     []string `json:"in,omitempty"`
	Order  []any    `json:"order,omitempty"`
	Rules  []any    `json:"rules,omitempty"`
	Offset int      `json:"offset,omitempty"`
}

type SearchOpts struct {
	Text            string
	VectorAttribute string
	TextAttributes  []string
	Vector          []float32
	Filters         [][]any
	K               int
	Expand          int
}

type SearchHit struct {
	Entity  any            `json:"entity"`
	Pull    map[string]any `json:"pull"`
	Matched []Fact         `json:"matched,omitempty"`
	Via     []any          `json:"via,omitempty"`
	Score   float64        `json:"score,omitempty"`
}

type SearchResult struct {
	Hits      []SearchHit `json:"hits"`
	Expanded  []SearchHit `json:"expanded"`
	BasisTx   int64       `json:"basis_tx"`
	Truncated bool        `json:"truncated"`
	WorkUsed  int         `json:"work_used"`
}

type Diff struct {
	Asserted  []Fact `json:"asserted"`
	Retracted []Fact `json:"retracted"`
}

type Stats struct {
	ApplicationID int64 `json:"application_id"`
	Entities      int64 `json:"entities"`
	Attributes    int64 `json:"attributes"`
	Facts         int64 `json:"facts"`
	LiveFacts     int64 `json:"live_facts"`
	Transactions  int64 `json:"transactions"`
	Blobs         int64 `json:"blobs"`
	Size          int64 `json:"size"`
	FormatVersion int64 `json:"format_version"`
}

type AttributeInfo struct {
	Dims        *int64   `json:"dims,omitempty"`
	Doc         *string  `json:"doc,omitempty"`
	VectorModel *string  `json:"vector_model,omitempty"`
	Name        string   `json:"name"`
	Types       []string `json:"types"`
	Facts       int64    `json:"facts"`
	Many        bool     `json:"many"`
	Unique      bool     `json:"unique"`
	NoHistory   bool     `json:"nohistory"`
}

// DeclaredAttribute preserves presence separately from value. In particular,
// an explicit false declaration is different from no declaration at all.
type DeclaredAttribute struct {
	Type        *string `json:"type,omitempty"`
	Many        *bool   `json:"many,omitempty"`
	Unique      *bool   `json:"unique,omitempty"`
	NoHistory   *bool   `json:"nohistory,omitempty"`
	Dims        *int64  `json:"dims,omitempty"`
	Doc         *string `json:"doc,omitempty"`
	VectorModel *string `json:"vector_model,omitempty"`
}

// EffectiveAttribute is total: nullable values are emitted as JSON null so
// agents can introspect the schema without guessing which fields exist.
type EffectiveAttribute struct {
	Type        *string `json:"type"`
	Dims        *int64  `json:"dims"`
	Doc         *string `json:"doc"`
	VectorModel *string `json:"vector_model"`
	Many        bool    `json:"many"`
	Unique      bool    `json:"unique"`
	NoHistory   bool    `json:"nohistory"`
}

func (attribute EffectiveAttribute) MarshalJSON() ([]byte, error) {
	return marshalOrderedObject([]Field{
		{Name: "type", Value: attribute.Type},
		{Name: "many", Value: attribute.Many},
		{Name: "unique", Value: attribute.Unique},
		{Name: "nohistory", Value: attribute.NoHistory},
		{Name: "dims", Value: attribute.Dims},
		{Name: "doc", Value: attribute.Doc},
		{Name: "vector_model", Value: attribute.VectorModel},
	})
}

type AttributeObservation struct {
	Types     []string `json:"types"`
	LiveFacts int64    `json:"live_facts"`
	Entities  int64    `json:"entities"`
}

type SchemaAttribute struct {
	Name      string               `json:"name"`
	Declared  DeclaredAttribute    `json:"declared"`
	Effective EffectiveAttribute   `json:"effective"`
	Observed  AttributeObservation `json:"observed"`
}

type ShapeInfo struct {
	Name     any      `json:"name"`
	Required []string `json:"required"`
	Allowed  []string `json:"allowed"`
	Closed   bool     `json:"closed"`
}

// ShapeDefinition describes the required and allowed attributes for entities
// assigned to a shape. Closed shapes implicitly allow every required
// attribute, matching the validation invariant enforced at commit time.
type ShapeDefinition struct {
	Required []string `json:"required,omitempty"`
	Allowed  []string `json:"allowed,omitempty"`
	Closed   bool     `json:"closed,omitempty"`
}

type SchemaSnapshot struct {
	Digest     string            `json:"digest"`
	Attributes []SchemaAttribute `json:"attributes"`
	Shapes     []ShapeInfo       `json:"shapes"`
	BasisTx    int64             `json:"basis_tx"`
}

type SchemaManifestAttribute struct {
	Declared DeclaredAttribute `json:"declared"`
	Name     string            `json:"name"`
}

type SchemaManifest struct {
	FGraph     string                    `json:"fgraph"`
	Digest     string                    `json:"digest"`
	Attributes []SchemaManifestAttribute `json:"attributes"`
	Shapes     []ShapeInfo               `json:"shapes"`
}

type SchemaManifestChange struct {
	Before any    `json:"before"`
	After  any    `json:"after"`
	Kind   string `json:"kind"`
	Name   string `json:"name"`
}

type SchemaManifestCheck struct {
	CurrentDigest string                 `json:"current_digest"`
	DesiredDigest string                 `json:"desired_digest"`
	Changes       []SchemaManifestChange `json:"changes"`
	BasisTx       int64                  `json:"basis_tx"`
	Valid         bool                   `json:"valid"`
}

type ValidationViolation struct {
	Code      string `json:"code"`
	Entity    any    `json:"entity"`
	Shape     any    `json:"shape"`
	Attribute string `json:"attribute"`
	Message   string `json:"message"`
}

type ValidationReport struct {
	Violations []ValidationViolation `json:"violations"`
	BasisTx    int64                 `json:"basis_tx"`
	Valid      bool                  `json:"valid"`
}

type DoctorReport struct {
	Integrity            string   `json:"integrity"`
	Problems             []string `json:"problems"`
	FTSRows              int64    `json:"fts_rows"`
	ExpectedFTSRows      int64    `json:"expected_fts_rows"`
	OrphanedBlobs        int64    `json:"orphaned_blobs"`
	FTSRowsRebuilt       int64    `json:"fts_rows_rebuilt"`
	OrphanedBlobsRemoved int64    `json:"orphaned_blobs_removed"`
	UnverifiableEvents   int64    `json:"unverifiable_event_hashes"`
	SchemaProblems       int64    `json:"schema_problems"`
	ShapeViolations      int64    `json:"shape_violations"`
	OK                   bool     `json:"ok"`
	RepairNeeded         bool     `json:"repair_needed"`
	Repaired             bool     `json:"repaired"`
}

type ExportTx struct {
	Meta      any     `json:"meta,omitempty"`
	By        string  `json:"by,omitempty"`
	Source    string  `json:"source,omitempty"`
	Asserted  [][]any `json:"asserted"`
	Retracted [][]any `json:"retracted"`
	TxFacts   [][]any `json:"tx_facts,omitempty"`
	Tx        int64   `json:"tx"`
	At        int64   `json:"at"`
}

type Clock func() int64

type OpenOption func(*openConfig)

type openConfig struct {
	clock       Clock
	eventIDs    EventIDFactory
	queryBudget int
	eventIDSet  bool
	readOnly    bool
}

func WithClock(clock Clock) OpenOption { return func(c *openConfig) { c.clock = clock } }

// EventIDFactory returns a canonical UUID for the next committed event. It is
// injectable so deterministic replicas and tests do not depend on randomness.
type EventIDFactory func() (string, error)

func WithEventIDFactory(factory EventIDFactory) OpenOption {
	return func(c *openConfig) {
		c.eventIDs = factory
		c.eventIDSet = true
	}
}
func WithReadOnly() OpenOption { return func(c *openConfig) { c.readOnly = true } }
func WithQueryBudget(budget int) OpenOption {
	return func(c *openConfig) { c.queryBudget = budget }
}

type TxOption func(*txOptions)

type txOptions struct {
	meta    any
	txFacts any
	// The apply pipeline owns these fields. They deliberately stay private so
	// ordinary transactions can only mint local UUIDv4 events and hashes.
	eventID         *string
	operationID     *string
	at              *int64
	declaration     *declareOptions
	by              *string
	originAt        *int64
	eventHash       *[32]byte
	requestHash     []byte
	requestHashBase map[string]any
	prepareData     func(context.Context, sqlRunner) (any, error)
	source          *string
	ifBasisTx       *int64
	declarationAttr string
	preallocated    []any
	force           bool
	txFactsSet      bool
	metaSet         bool
}

func WithSource(source string) TxOption { return func(o *txOptions) { o.source = &source } }
func WithBy(by string) TxOption         { return func(o *txOptions) { o.by = &by } }
func WithMeta(meta any) TxOption {
	return func(o *txOptions) {
		o.meta = meta
		o.metaSet = true
	}
}

func WithTxFacts(facts any) TxOption {
	return func(o *txOptions) {
		o.txFacts = facts
		o.txFactsSet = true
	}
}

// IfBasis requires the current committed basis to equal tx. The check is made
// under SQLite's single-writer lock before allocation, time, or UUID sampling.
func IfBasis(tx int64) TxOption     { return func(o *txOptions) { o.ifBasisTx = &tx } }
func WithBasisTx(tx int64) TxOption { return IfBasis(tx) }

// WithOperationID makes a transaction safely retryable. Reusing the same id
// with the same canonical request returns the original receipt; different
// input fails with ErrConflict.
func WithOperationID(id string) TxOption { return func(o *txOptions) { o.operationID = &id } }

type DeclareOption func(*declareOptions)

type declareOptions struct {
	typeName    *string
	many        *bool
	unique      *bool
	nohistory   *bool
	dims        *int64
	doc         *string
	vectorModel *string
}

func Type(name string) DeclareOption { return func(o *declareOptions) { o.typeName = &name } }
func Ref() DeclareOption             { return Type("ref") }
func Many(value ...bool) DeclareOption {
	return func(o *declareOptions) {
		v := true
		if len(value) > 0 {
			v = value[0]
		}
		o.many = &v
	}
}

func Unique(value ...bool) DeclareOption {
	return func(o *declareOptions) {
		v := true
		if len(value) > 0 {
			v = value[0]
		}
		o.unique = &v
	}
}

func NoHistory(value ...bool) DeclareOption {
	return func(o *declareOptions) {
		v := true
		if len(value) > 0 {
			v = value[0]
		}
		o.nohistory = &v
	}
}
func Dims(n int64) DeclareOption    { return func(o *declareOptions) { o.dims = &n } }
func Doc(text string) DeclareOption { return func(o *declareOptions) { o.doc = &text } }
func VectorModel(model string) DeclareOption {
	return func(o *declareOptions) { o.vectorModel = &model }
}

type FollowEvent struct {
	// Tx is the local cursor only. Record is the portable event/1 value and
	// deliberately contains no local numeric transaction id.
	Err    error          `json:"-"`
	Record map[string]any `json:"event"`
	Tx     int64          `json:"-"`
}

type FollowOptions struct {
	Since    int64
	Interval time.Duration
}

type Embedder func(context.Context, string) ([]float32, error)
