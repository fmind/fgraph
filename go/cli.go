package fgraph

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/urfave/cli/v3"
)

const (
	defaultDatabasePath       = "facts.fgraph"
	legacyDefaultDatabasePath = "fgraph.db"
)

func NewCLI(reader io.Reader, writer, errWriter io.Writer) *cli.Command {
	usageError := func(_ context.Context, _ *cli.Command, err error, _ bool) error {
		return cli.Exit(err, 2)
	}
	command := &cli.Command{
		Name: "fgraph", Usage: "embedded temporal fact store", Version: Version,
		Reader: reader, Writer: writer, ErrWriter: errWriter,
		OnUsageError:              usageError,
		DisableSliceFlagSeparator: true, // JSON filter values contain commas and repeat as whole flags.
		// RunCLI is a library boundary: the executable decides how to render and
		// exit for typed errors, so urfave must not terminate the process here.
		ExitErrHandler: func(context.Context, *cli.Command, error) {},
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "db", Value: defaultDatabasePath, Sources: cli.EnvVars("FGRAPH_DB"), Usage: "SQLite database path"},
			&cli.BoolFlag{Name: "json", Usage: "emit machine-readable JSON"},
			&cli.IntFlag{Name: "query-budget", Value: DefaultQueryBudget, Sources: cli.EnvVars("FGRAPH_QUERY_BUDGET"), Usage: "maximum deterministic query work units"},
		},
	}
	command.Commands = []*cli.Command{
		{Name: "init", Usage: "initialize a database and show its info", Action: withArgumentCount(0, 0, "init accepts no arguments", databaseAction(false, func(ctx context.Context, cmd *cli.Command, db *DB) error {
			stats, err := db.Stats(ctx)
			return outputResult(cmd, stats, err)
		}))},
		{Name: "info", Usage: "show database statistics", Action: withArgumentCount(0, 0, "info accepts no arguments", databaseAction(true, func(ctx context.Context, cmd *cli.Command, db *DB) error {
			stats, err := db.Stats(ctx)
			return outputResult(cmd, stats, err)
		}))},
		{Name: "add", Usage: "assert JSON facts from an argument or stdin", ArgsUsage: "<json|@file|->", Flags: addFlags(), Action: withArgumentCount(1, 1, "add needs exactly one JSON argument, @file, or -", databaseAction(false, addAction))},
		{Name: "retract", Usage: "retract an entity, attribute, or exact value", ArgsUsage: "<entity> [attribute] [value-json]", Flags: mutationFlags(), Action: withArgumentCount(1, 3, "retract needs entity, optional attribute, and optional value", databaseAction(false, retractAction))},
		{Name: "get", Usage: "pull one entity, optionally at a historical transaction", ArgsUsage: "<entity>", Flags: []cli.Flag{&cli.IntFlag{Name: "depth", Value: 1}, &cli.StringFlag{Name: "at", Usage: "transaction id or integer UTC microseconds"}}, Before: validateHistoricalSelectorSyntax, Action: withArgumentCount(1, 1, "get needs exactly one entity", databaseAction(true, getAction))},
		{Name: "q", Usage: "run a canonical JSON query, optionally at a historical transaction", ArgsUsage: "<json|@file>", Flags: []cli.Flag{&cli.StringFlag{Name: "args", Usage: "JSON input bindings"}, &cli.StringFlag{Name: "at", Usage: "transaction id or integer UTC microseconds"}}, Before: validateHistoricalSelectorSyntax, Action: withArgumentCount(1, 1, "q needs exactly one query JSON argument or @file", databaseAction(true, queryAction))},
		{Name: "explain", Usage: "explain the actual bounded query plan without evaluating it", ArgsUsage: "<json|@file>", Flags: []cli.Flag{&cli.StringFlag{Name: "args", Usage: "JSON input bindings"}}, Action: withArgumentCount(1, 1, "explain needs exactly one query JSON argument or @file", databaseAction(true, explainAction))},
		{Name: "datoms", Usage: "page current or historical datoms by an indexed order", ArgsUsage: "[eavt|avet|vaet]", Flags: []cli.Flag{
			&cli.StringFlag{Name: "components", Value: "[]", Usage: "JSON index-prefix array"},
			&cli.StringFlag{Name: "source", Value: "current", Usage: "current or history"},
			&cli.IntFlag{Name: "limit", Value: 100}, &cli.StringFlag{Name: "cursor"},
		}, Action: withArgumentCount(0, 1, "datoms accepts at most one index name", databaseAction(true, datomsAction))},
		{Name: "search", Usage: "keyword or vector search", ArgsUsage: "[text]", DisableSliceFlagSeparator: true, Flags: []cli.Flag{
			&cli.StringFlag{Name: "text"}, &cli.StringFlag{Name: "vector", Usage: "JSON float array"},
			&cli.IntFlag{Name: "k", Value: 10}, &cli.IntFlag{Name: "expand"}, &cli.StringFlag{Name: "vector-attribute"},
			&cli.StringSliceFlag{Name: "text-attribute", Usage: "text attribute to search, repeatable"},
			&cli.StringSliceFlag{Name: "filter", Usage: "JSON [attribute,value], repeatable"},
			&cli.StringFlag{Name: "embed-cmd", Usage: "external embedding executable"},
		}, Action: databaseAction(true, searchAction)},
		{Name: "history", Usage: "show an entity fact timeline", ArgsUsage: "<entity> [attribute]", Action: withArgumentCount(1, 2, "history needs entity and optional attribute", databaseAction(true, historyAction))},
		{Name: "why", Usage: "explain current facts with provenance", ArgsUsage: "<entity> [attribute]", Action: withArgumentCount(1, 2, "why needs entity and optional attribute", databaseAction(true, whyAction))},
		{Name: "tx", Usage: "show one durable event receipt", ArgsUsage: "<transaction-id>", Action: withArgumentCount(1, 1, "tx needs exactly one transaction id", databaseAction(true, txAction))},
		{Name: "diff", Usage: "show facts changed between two transactions", ArgsUsage: "<from> <to>", Action: withArgumentCount(2, 2, "diff needs start and end transaction ids", databaseAction(true, diffAction))},
		{Name: "declare", Usage: "patch an attribute declaration", ArgsUsage: "<attribute>", Flags: []cli.Flag{
			&cli.StringFlag{Name: "type"}, &cli.BoolFlag{Name: "ref"}, &cli.BoolFlag{Name: "many"},
			&cli.BoolFlag{Name: "one", Usage: "disable cardinality many"},
			&cli.BoolFlag{Name: "unique"}, &cli.BoolFlag{Name: "not-unique", Usage: "disable uniqueness"},
			&cli.BoolFlag{Name: "nohistory"}, &cli.BoolFlag{Name: "history", Usage: "retain retracted history"},
			&cli.Int64Flag{Name: "dims"}, &cli.StringFlag{Name: "doc"}, &cli.StringFlag{Name: "vector-model"},
			&cli.StringFlag{Name: "operation-id"}, &cli.Int64Flag{Name: "if-basis-tx"},
		}, Action: withArgumentCount(1, 1, "declare needs exactly one attribute", databaseAction(false, declareAction))},
		{Name: "shape", Usage: "create or replace a required/allowed attribute shape", ArgsUsage: "<name>", DisableSliceFlagSeparator: true, Flags: []cli.Flag{
			&cli.StringSliceFlag{Name: "required", Usage: "required attribute, repeatable"},
			&cli.StringSliceFlag{Name: "allowed", Usage: "allowed attribute, repeatable"},
			&cli.BoolFlag{Name: "closed"}, &cli.BoolFlag{Name: "open"},
			&cli.StringFlag{Name: "operation-id"}, &cli.Int64Flag{Name: "if-basis-tx"},
		}, Action: withArgumentCount(1, 1, "shape needs exactly one shape name", databaseAction(false, shapeAction))},
		{Name: "validate", Usage: "validate one entity against its assigned shapes", ArgsUsage: "<entity>", Action: withArgumentCount(1, 1, "validate needs exactly one entity", databaseAction(true, validateAction))},
		{Name: "schema", Usage: "list effective application attribute schemas", ArgsUsage: "[prefix]", Flags: []cli.Flag{
			&cli.BoolFlag{Name: "system", Usage: "include fgraph system attributes"},
		}, Action: withArgumentCount(0, 1, "schema accepts at most one attribute prefix", databaseAction(true, schemaAction))},
		{Name: "schema-export", Usage: "export portable schema/1 declarations and shapes", Action: withArgumentCount(0, 0, "schema-export accepts no arguments", databaseAction(true, schemaExportAction))},
		{Name: "schema-check", Usage: "compare a schema/1 manifest with the database", ArgsUsage: "<json|@file|->", Action: withArgumentCount(1, 1, "schema-check needs one JSON argument, @file, or -", databaseAction(true, schemaCheckAction))},
		{Name: "schema-apply", Usage: "atomically apply a schema/1 manifest", ArgsUsage: "<json|@file|->", Flags: mutationFlags(), Action: withArgumentCount(1, 1, "schema-apply needs one JSON argument, @file, or -", databaseAction(false, schemaApplyAction))},
		{Name: "apply", Usage: "atomically apply portable event/1 NDJSON", ArgsUsage: "[file|-]", Action: withArgumentCount(0, 1, "apply accepts at most one input file", databaseAction(false, applyAction))},
		{Name: "snapshot", Usage: "write a portable retained-state snapshot to stdout", Action: withArgumentCount(0, 0, "snapshot writes to stdout and accepts no arguments", databaseAction(true, snapshotAction))},
		{Name: "restore", Usage: "atomically restore snapshot/1 NDJSON into a pristine database", ArgsUsage: "[file|-]", Action: withArgumentCount(0, 1, "restore accepts at most one input file", databaseAction(false, restoreAction))},
		{Name: "undo", Usage: "create a compensating transaction", ArgsUsage: "<tx>", Flags: mutationFlags(), Action: withArgumentCount(1, 1, "undo needs exactly one transaction id", databaseAction(false, undoAction))},
		{Name: "excise", Usage: "irreversibly erase one application entity with an idempotent CAS receipt", ArgsUsage: "<entity>", Flags: []cli.Flag{
			&cli.StringFlag{Name: "operation-id", Usage: "unique idempotency key (required)"},
			&cli.Int64Flag{Name: "if-basis-tx", Usage: "expected current basis transaction (required)"},
		}, Action: withArgumentCount(1, 1, "excise needs exactly one entity", databaseAction(false, exciseAction))},
		{Name: "tail", Usage: "stream portable event/1 records", Flags: []cli.Flag{&cli.Int64Flag{Name: "since", Value: GenesisTx}, &cli.BoolFlag{Name: "follow"}}, Action: withArgumentCount(0, 0, "tail accepts only --since and --follow", databaseAction(true, tailAction))},
		{Name: "backup", Usage: "create a safe hot backup", ArgsUsage: "<destination>", Action: withArgumentCount(1, 1, "backup needs exactly one destination", databaseAction(false, backupAction))},
		{Name: "doctor", Usage: "check database invariants without mutation", Flags: []cli.Flag{
			&cli.BoolFlag{Name: "repair", Usage: "transactionally rebuild FTS and remove orphaned blobs"},
		}, Action: withArgumentCount(0, 0, "doctor accepts no arguments", doctorCLIAction)},
		{Name: "mcp", Usage: "serve read-only MCP over stdio", Flags: []cli.Flag{&cli.BoolFlag{Name: "write", Usage: "opt in to remember, forget, and undo tools"}, &cli.BoolFlag{Name: "read-only", Usage: "deprecated; read-only is the default"}, &cli.StringFlag{Name: "embed-cmd"}}, Action: withArgumentCount(0, 0, "mcp accepts no arguments", mcpAction)},
		{Name: "version", Usage: "print the fgraph version", Action: withArgumentCount(0, 0, "version accepts no arguments", func(_ context.Context, cmd *cli.Command) error {
			_, err := fmt.Fprintln(cmd.Root().Writer, Version)
			return err
		})},
	}
	for _, subcommand := range command.Commands {
		subcommand.OnUsageError = usageError
	}
	return command
}

func RunCLI(ctx context.Context, args []string, reader io.Reader, writer, errWriter io.Writer) error {
	return NewCLI(reader, writer, errWriter).Run(ctx, args)
}

type dbAction func(context.Context, *cli.Command, *DB) error

const validatedCLIArgumentsKey = "fgraph.validated-arguments"

type validatedCLIArguments struct {
	firstTransaction  int64
	secondTransaction int64
}

func withArgumentCount(minimum, maximum int, message string, action cli.ActionFunc) cli.ActionFunc {
	return func(ctx context.Context, cmd *cli.Command) error {
		count := cmd.Args().Len()
		if count < minimum || count > maximum {
			return cli.Exit(message, 2)
		}
		return action(ctx, cmd)
	}
}

func databaseAction(readOnly bool, action dbAction) cli.ActionFunc {
	return func(ctx context.Context, cmd *cli.Command) (result error) {
		if err := validateCLIUsage(cmd); err != nil {
			return err
		}
		db, err := openCLI(cmd, readOnly)
		if err != nil {
			return err
		}
		defer func() { result = joinErrors(result, db.Close()) }()
		return action(ctx, cmd, db)
	}
}

func validateCLIUsage(cmd *cli.Command) error {
	switch cmd.Name {
	case "add":
		batchSize := cmd.Int("batch-size")
		if batchSize < 0 || batchSize > 10_000 || cmd.IsSet("batch-size") && batchSize == 0 {
			return usage("--batch-size must be between 1 and 10000")
		}
		if cmd.IsSet("operation-id") && cmd.IsSet("operation-id-prefix") {
			return usage("choose --operation-id for one transaction or --operation-id-prefix for batches")
		}
		if cmd.IsSet("operation-id-prefix") && batchSize == 0 {
			return usage("--operation-id-prefix requires --batch-size")
		}
	case "declare":
		if cmd.Bool("one") && cmd.IsSet("many") {
			return usage("declare cannot combine --many and --one")
		}
		if cmd.Bool("not-unique") && cmd.IsSet("unique") {
			return usage("declare cannot combine --unique and --not-unique")
		}
		if cmd.Bool("history") && cmd.IsSet("nohistory") {
			return usage("declare cannot combine --nohistory and --history")
		}
	case "diff":
		from, err := parseCLIInteger(cmd.Args().Get(0), "start transaction")
		if err != nil {
			return err
		}
		to, err := parseCLIInteger(cmd.Args().Get(1), "end transaction")
		if err != nil {
			return err
		}
		storeValidatedCLIArguments(cmd, from, to)
	case "excise":
		if !cmd.IsSet("operation-id") || !cmd.IsSet("if-basis-tx") {
			return usage("excise requires --operation-id and --if-basis-tx")
		}
	case "mcp":
		if cmd.Bool("write") && cmd.Bool("read-only") {
			return usage("mcp --write conflicts with --read-only")
		}
	case "shape":
		if cmd.Bool("closed") && cmd.Bool("open") {
			return usage("shape --closed and --open are mutually exclusive")
		}
	case "tx":
		tx, err := parseCLIInteger(cmd.Args().First(), "transaction id")
		if err != nil {
			return err
		}
		storeValidatedCLIArguments(cmd, tx, 0)
	case "undo":
		tx, err := parseCLIInteger(cmd.Args().First(), "transaction id")
		if err != nil {
			return err
		}
		storeValidatedCLIArguments(cmd, tx, 0)
	}
	return nil
}

func parseCLIInteger(raw, context string) (int64, error) {
	value, err := strconv.ParseInt(raw, 10, 64)
	if err == nil {
		return value, nil
	}
	if errors.Is(err, strconv.ErrSyntax) {
		return 0, usage("invalid %s %q", context, raw)
	}
	return 0, fail(ErrType, "%s %q is outside the signed 64-bit integer range", context, raw)
}

func storeValidatedCLIArguments(cmd *cli.Command, firstTransaction, secondTransaction int64) {
	if cmd.Metadata == nil {
		cmd.Metadata = map[string]any{}
	}
	// Carry parsed integers across the pre-open boundary so actions never need
	// to repeat user-input validation after SQLite has been selected.
	cmd.Metadata[validatedCLIArgumentsKey] = validatedCLIArguments{
		firstTransaction: firstTransaction, secondTransaction: secondTransaction,
	}
}

func loadValidatedCLIArguments(cmd *cli.Command) (validatedCLIArguments, error) {
	validated, ok := cmd.Metadata[validatedCLIArgumentsKey].(validatedCLIArguments)
	if !ok {
		return validatedCLIArguments{}, fail(
			ErrFormat,
			"CLI command %q reached database execution without validated transaction arguments",
			cmd.Name,
		)
	}
	return validated, nil
}

func openCLI(cmd *cli.Command, readOnly bool) (*DB, error) {
	selectedPath := cmd.String("db")
	if selectedPath == "" {
		return nil, fail(ErrFormat, "database path is empty; pass --db PATH or unset FGRAPH_DB to use %s", defaultDatabasePath)
	}
	if !cmd.IsSet("db") {
		var err error
		selectedPath, err = resolveImplicitDatabasePath()
		if err != nil {
			return nil, err
		}
	}
	options := []OpenOption{WithQueryBudget(cmd.Int("query-budget"))}
	if readOnly {
		options = append(options, WithReadOnly())
	}
	if raw := os.Getenv("FGRAPH_CLOCK"); raw != "" {
		base, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fail(ErrType, "FGRAPH_CLOCK %q is invalid; use integer microseconds", raw)
		}
		options = append(options, WithClock(func() int64 { return base }))
	}
	return Open(selectedPath, options...)
}

func resolveImplicitDatabasePath() (string, error) {
	legacyExists, err := pathEntryExists(legacyDefaultDatabasePath, "legacy")
	if err != nil || !legacyExists {
		return defaultDatabasePath, err
	}
	currentExists, err := pathEntryExists(defaultDatabasePath, "default")
	if err != nil {
		return "", err
	}
	if !currentExists {
		return legacyDefaultDatabasePath, nil
	}
	db, err := Open(defaultDatabasePath, WithReadOnly())
	if err != nil {
		return "", wrap(
			ErrFormat,
			err,
			"legacy default database %s exists and %s is not an initialized fgraph database; use --db %s to keep using the legacy file or explicitly pass --db %s to select the new default",
			legacyDefaultDatabasePath,
			defaultDatabasePath,
			legacyDefaultDatabasePath,
			defaultDatabasePath,
		)
	}
	if err := db.Close(); err != nil {
		return "", wrap(ErrFormat, err, "cannot finish validating default database %q", defaultDatabasePath)
	}
	return defaultDatabasePath, nil
}

func pathEntryExists(path, description string) (bool, error) {
	if _, err := os.Lstat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, wrap(ErrFormat, err, "cannot inspect %s database path %q; check directory permissions", description, path)
	}
	return true, nil
}

func outputResult(cmd *cli.Command, value any, err error) error {
	if err != nil {
		return err
	}
	if cmd.Bool("json") {
		encoded, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			return wrap(ErrFormat, marshalErr, "cannot encode CLI output as JSON")
		}
		decoded, decodeErr := decodeInternalDocumentJSON(bytes.NewReader(encoded))
		if decodeErr != nil {
			return wrap(ErrFormat, decodeErr, "cannot canonicalize CLI output")
		}
		canonical, canonicalErr := canonicalJSON(decoded)
		if canonicalErr != nil {
			return wrap(ErrFormat, canonicalErr, "cannot canonicalize CLI output")
		}
		if _, writeErr := cmd.Root().Writer.Write(append(canonical, '\n')); writeErr != nil {
			return wrap(ErrFormat, writeErr, "cannot write CLI output; check the destination stream")
		}
		return nil
	}
	encoder := json.NewEncoder(cmd.Root().Writer)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return wrap(ErrFormat, err, "cannot write CLI output; check the destination stream")
	}
	return nil
}

func usage(message string, args ...any) error { return cli.Exit(fmt.Sprintf(message, args...), 2) }

func mutationFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{Name: "operation-id", Usage: "unique idempotency key"},
		&cli.Int64Flag{Name: "if-basis-tx", Usage: "expected current basis transaction"},
	}
}

func addFlags() []cli.Flag {
	return append(mutationFlags(),
		&cli.IntFlag{Name: "batch-size", Usage: "group NDJSON lines into bounded transactions"},
		&cli.StringFlag{Name: "operation-id-prefix", Usage: "idempotency prefix for numbered batches"},
	)
}

func mutationOptions(cmd *cli.Command) []TxOption {
	options := []TxOption{}
	if cmd.IsSet("operation-id") {
		options = append(options, WithOperationID(cmd.String("operation-id")))
	}
	if cmd.IsSet("if-basis-tx") {
		options = append(options, IfBasis(cmd.Int64("if-basis-tx")))
	}
	return options
}

func addAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	batchSize := cmd.Int("batch-size")
	if batchSize > 0 {
		return addBatchesAction(ctx, cmd, db, batchSize)
	}
	data, err := readArgument(cmd, cmd.Args().First())
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return fail(ErrType, "add input is empty; provide one JSON transaction or non-empty NDJSON")
	}
	payloads, err := decodeAddPayloads(data)
	if err != nil {
		return err
	}
	if cmd.IsSet("operation-id") && len(payloads) > 1 && batchSize == 0 {
		return fail(ErrType, "--operation-id requires one JSON transaction, not NDJSON")
	}
	options := mutationOptions(cmd)
	if batchSize == 0 && len(payloads) == 1 {
		report, err := db.Transact(ctx, payloads[0], options...)
		return outputResult(cmd, report, err)
	}
	if cmd.IsSet("if-basis-tx") && len(payloads) > 1 {
		return fail(ErrType, "--if-basis-tx cannot span multiple batches; use idempotent batch operation ids")
	}
	reports := []TxReport{}
	for _, payload := range payloads {
		report, lineErr := db.Transact(ctx, payload, options...)
		if lineErr != nil {
			return lineErr
		}
		reports = append(reports, report)
	}
	return outputResult(cmd, reports, nil)
}

type addPayloadStream struct {
	reader     *bufio.Reader
	lineNumber int
	done       bool
}

func openAddPayloadStream(cmd *cli.Command, argument string) (*addPayloadStream, func() error, error) {
	if argument == "-" {
		return &addPayloadStream{reader: bufio.NewReader(cmd.Root().Reader)}, func() error { return nil }, nil
	}
	if strings.HasPrefix(argument, "@") {
		path := strings.TrimPrefix(argument, "@")
		directory, name := filepath.Split(filepath.Clean(path))
		if directory == "" {
			directory = "."
		}
		root, err := os.OpenRoot(directory)
		if err != nil {
			return nil, nil, wrap(ErrFormat, err, "cannot read %q", argument)
		}
		file, err := root.Open(name)
		if err != nil {
			return nil, nil, joinErrors(wrap(ErrFormat, err, "cannot read %q", argument), wrapClose(root.Close(), "add input root"))
		}
		closeInput := func() error {
			return joinErrors(wrapClose(file.Close(), "add input file"), wrapClose(root.Close(), "add input root"))
		}
		return &addPayloadStream{reader: bufio.NewReader(file)}, closeInput, nil
	}
	return &addPayloadStream{reader: bufio.NewReader(strings.NewReader(argument))}, func() error { return nil }, nil
}

func (stream *addPayloadStream) next() (any, bool, error) {
	for !stream.done {
		line, readErr := readPortableLine(stream.reader)
		stream.lineNumber++
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return nil, false, wrap(ErrFormat, readErr, "cannot read add input")
		}
		stream.done = errors.Is(readErr, io.EOF)
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		value, err := DecodeJSON(bytes.NewReader(line))
		if err != nil {
			return nil, false, wrap(ErrType, err, "add input line %d is invalid JSON", stream.lineNumber)
		}
		return value, true, nil
	}
	return nil, false, nil
}

func (stream *addPayloadStream) batch(size int) ([]any, error) {
	result := make([]any, 0, size)
	for len(result) < size {
		value, ok, err := stream.next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		result = append(result, value)
	}
	return result, nil
}

func addBatchesAction(ctx context.Context, cmd *cli.Command, db *DB, batchSize int) (resultErr error) {
	stream, closeStream, err := openAddPayloadStream(cmd, cmd.Args().First())
	if err != nil {
		return err
	}
	defer func() {
		resultErr = joinErrors(resultErr, wrapClose(closeStream(), "add input stream"))
	}()
	batch, err := stream.batch(batchSize)
	if err != nil {
		return err
	}
	if len(batch) == 0 {
		return fail(ErrType, "add input is empty; provide non-empty NDJSON")
	}
	if cmd.IsSet("operation-id") || cmd.IsSet("if-basis-tx") {
		if _, ok, nextErr := stream.next(); nextErr != nil {
			return nextErr
		} else if ok {
			option := "--if-basis-tx"
			if cmd.IsSet("operation-id") {
				option = "--operation-id"
			}
			return fail(ErrType, "%s cannot span multiple batches; use idempotent batch operation ids", option)
		}
	}

	type batchSummary struct {
		Tx             any   `json:"tx"`
		BasisTx        int64 `json:"basis_tx"`
		Batches        int   `json:"batches"`
		Items          int   `json:"items"`
		Applied        int   `json:"applied"`
		AlreadyApplied int   `json:"already_applied"`
		Noop           int   `json:"noop"`
	}
	summary := batchSummary{}
	options := mutationOptions(cmd)
	var last TxReport
	for len(batch) > 0 {
		batchIndex := summary.Batches
		batchOptions := options
		if cmd.IsSet("operation-id-prefix") {
			batchOptions = append([]TxOption{}, options...)
			batchOptions = append(batchOptions, WithOperationID(fmt.Sprintf("%s:%08d", cmd.String("operation-id-prefix"), batchIndex)))
		}
		payload := any(batch)
		if batchSize == 1 {
			payload = batch[0]
		}
		last, err = db.Transact(ctx, payload, batchOptions...)
		if err != nil {
			return err
		}
		summary.Batches = batchIndex + 1
		summary.Items += len(batch)
		switch last.Status {
		case "applied":
			summary.Applied++
		case "already_applied":
			summary.AlreadyApplied++
		case "noop":
			summary.Noop++
		}
		batch, err = stream.batch(batchSize)
		if err != nil {
			return err
		}
	}
	basis := last.BasisTx
	var tx any
	if last.Tx != 0 {
		basis = last.Tx
		tx = last.Tx
	}
	summary.BasisTx = basis
	summary.Tx = tx
	return outputResult(cmd, summary, nil)
}

func decodeAddPayloads(data []byte) ([]any, error) {
	decoded, err := DecodeJSON(bytes.NewReader(data))
	if err == nil {
		return []any{decoded}, nil
	}
	// Decode every NDJSON line before the first write so malformed input cannot
	// leave an earlier line committed.
	payloads := []any{}
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		value, lineErr := DecodeJSON(bytes.NewReader(line))
		if lineErr != nil {
			return nil, lineErr
		}
		payloads = append(payloads, value)
	}
	return payloads, nil
}

func retractAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	ref := parseRef(cmd.Args().Get(0))
	args := []any{}
	if cmd.Args().Len() >= 2 {
		args = append(args, cmd.Args().Get(1))
	}
	if cmd.Args().Len() == 3 {
		args = append(args, parseJSONOrText(cmd.Args().Get(2)))
	}
	op := append([]any{"retract", ref}, args...)
	report, err := db.Transact(ctx, op, mutationOptions(cmd)...)
	return outputResult(cmd, report, err)
}

func getAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	target, err := cliHistoricalView(ctx, cmd, db)
	if err != nil {
		return err
	}
	entity, err := target.Entity(ctx, parseRef(cmd.Args().First()), cmd.Int("depth"))
	return outputResult(cmd, entity, err)
}

func queryAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	data, err := readArgument(cmd, cmd.Args().First())
	if err != nil {
		return err
	}
	value, err := DecodeJSON(bytes.NewReader(data))
	if err != nil {
		return err
	}
	args := map[string]any{}
	if raw := cmd.String("args"); raw != "" {
		decoded, decodeErr := DecodeJSON(strings.NewReader(raw))
		if decodeErr != nil {
			return decodeErr
		}
		var objectOK bool
		args, objectOK = objectMap(decoded)
		if !objectOK {
			return fail(ErrType, "--args must be a JSON object of variable bindings")
		}
	}
	target, err := cliHistoricalView(ctx, cmd, db)
	if err != nil {
		return err
	}
	result, err := target.QueryJSON(ctx, value, args)
	return outputResult(cmd, result, err)
}

func explainAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	value, args, err := cliQueryInput(cmd)
	if err != nil {
		return err
	}
	result, err := db.ExplainJSON(ctx, value, args)
	return outputResult(cmd, result, err)
}

func cliQueryInput(cmd *cli.Command) (any, map[string]any, error) {
	data, err := readArgument(cmd, cmd.Args().First())
	if err != nil {
		return nil, nil, err
	}
	value, err := DecodeJSON(bytes.NewReader(data))
	if err != nil {
		return nil, nil, err
	}
	args := map[string]any{}
	if raw := cmd.String("args"); raw != "" {
		decoded, decodeErr := DecodeJSON(strings.NewReader(raw))
		if decodeErr != nil {
			return nil, nil, decodeErr
		}
		var objectOK bool
		args, objectOK = objectMap(decoded)
		if !objectOK {
			return nil, nil, fail(ErrType, "--args must be a JSON object of variable bindings")
		}
	}
	return value, args, nil
}

func datomsAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	decoded, err := DecodeJSON(strings.NewReader(cmd.String("components")))
	if err != nil {
		return err
	}
	components, ok := decoded.([]any)
	if !ok {
		return fail(ErrType, "datoms --components must be a JSON array")
	}
	page, err := db.Datoms(ctx, DatomOptions{
		Index: cmd.Args().First(), Source: cmd.String("source"), Components: components,
		Limit: cmd.Int("limit"), Cursor: cmd.String("cursor"),
	})
	return outputResult(cmd, page, err)
}

func cliHistoricalView(ctx context.Context, cmd *cli.Command, db *DB) (*DB, error) {
	if !cmd.IsSet("at") {
		return db, nil
	}
	raw := cmd.String("at")
	selector, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil, fail(ErrType, "--at value %q is outside signed 64-bit integer range; use a transaction id or integer UTC microseconds", raw)
	}
	return db.At(ctx, selector)
}

func validateHistoricalSelectorSyntax(ctx context.Context, cmd *cli.Command) (context.Context, error) {
	if !cmd.IsSet("at") {
		return ctx, nil
	}
	if _, err := strconv.ParseInt(cmd.String("at"), 10, 64); errors.Is(err, strconv.ErrSyntax) {
		return ctx, usage("--at value %q is not an integer", cmd.String("at"))
	}
	return ctx, nil
}

func searchAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	text := cmd.String("text")
	if text == "" && cmd.Args().Present() {
		text = strings.Join(cmd.Args().Slice(), " ")
	}
	if cmd.Int("k") < 1 {
		return fail(ErrType, "search k=%d is invalid; use a positive result count", cmd.Int("k"))
	}
	options := SearchOpts{
		Text: text, K: cmd.Int("k"), Expand: cmd.Int("expand"),
		VectorAttribute: cmd.String("vector-attribute"), TextAttributes: cmd.StringSlice("text-attribute"),
	}
	if raw := cmd.String("vector"); raw != "" {
		vector, err := parseVectorJSON(raw)
		if err != nil {
			return err
		}
		options.Vector = vector
	} else if embedCommand := cmd.String("embed-cmd"); embedCommand != "" && text != "" {
		vector, err := commandEmbedder(embedCommand)(ctx, text)
		if err != nil {
			return wrap(ErrType, err, "embedding command failed for search; correct --embed-cmd")
		}
		options.Vector = vector
	}
	for _, raw := range cmd.StringSlice("filter") {
		value, err := DecodeJSON(strings.NewReader(raw))
		if err != nil {
			return err
		}
		filter, ok := value.([]any)
		if !ok {
			return fail(ErrType, "--filter must be JSON [attribute,value]")
		}
		options.Filters = append(options.Filters, filter)
	}
	result, err := db.Search(ctx, options)
	return outputResult(cmd, result, err)
}

func historyAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	var (
		facts []Fact
		err   error
	)
	if cmd.Args().Len() == 2 {
		facts, err = db.History(ctx, parseRef(cmd.Args().Get(0)), cmd.Args().Get(1))
	} else {
		facts, err = db.History(ctx, parseRef(cmd.Args().Get(0)))
	}
	return outputResult(cmd, facts, err)
}

func whyAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	var (
		facts []Fact
		err   error
	)
	if cmd.Args().Len() == 2 {
		facts, err = db.Why(ctx, parseRef(cmd.Args().Get(0)), cmd.Args().Get(1))
	} else {
		facts, err = db.Why(ctx, parseRef(cmd.Args().Get(0)))
	}
	return outputResult(cmd, facts, err)
}

func diffAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	validated, err := loadValidatedCLIArguments(cmd)
	if err != nil {
		return err
	}
	result, err := db.Diff(ctx, validated.firstTransaction, validated.secondTransaction)
	return outputResult(cmd, result, err)
}

func declareAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	options := []DeclareOption{}
	if cmd.Bool("ref") {
		options = append(options, Ref())
	} else if cmd.IsSet("type") {
		options = append(options, Type(cmd.String("type")))
	}
	if cmd.IsSet("many") {
		value := cmd.Bool("many")
		options = append(options, func(o *declareOptions) { o.many = &value })
	}
	if cmd.Bool("one") {
		options = append(options, Many(false))
	}
	if cmd.IsSet("unique") {
		value := cmd.Bool("unique")
		options = append(options, func(o *declareOptions) { o.unique = &value })
	}
	if cmd.Bool("not-unique") {
		options = append(options, Unique(false))
	}
	if cmd.IsSet("nohistory") {
		options = append(options, NoHistory(cmd.Bool("nohistory")))
	}
	if cmd.Bool("history") {
		options = append(options, NoHistory(false))
	}
	if cmd.IsSet("dims") {
		options = append(options, Dims(cmd.Int64("dims")))
	}
	if cmd.IsSet("doc") {
		options = append(options, Doc(cmd.String("doc")))
	}
	if cmd.IsSet("vector-model") {
		options = append(options, VectorModel(cmd.String("vector-model")))
	}
	report, err := db.declareWithTxOptions(ctx, cmd.Args().First(), mutationOptions(cmd), options...)
	return outputResult(cmd, report, err)
}

func shapeAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	report, err := db.DeclareShape(ctx, cmd.Args().First(), ShapeDefinition{
		Required: cmd.StringSlice("required"), Allowed: cmd.StringSlice("allowed"), Closed: cmd.Bool("closed"),
	}, mutationOptions(cmd)...)
	return outputResult(cmd, report, err)
}

func validateAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	report, err := db.Validate(ctx, parseRef(cmd.Args().First()))
	return outputResult(cmd, report, err)
}

func schemaAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	snapshot, err := db.Schema(ctx, cmd.Args().First(), cmd.Bool("system"))
	return outputResult(cmd, snapshot, err)
}

func schemaExportAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	manifest, err := db.SchemaManifest(ctx)
	return outputResult(cmd, manifest, err)
}

func decodeSchemaManifest(cmd *cli.Command) (SchemaManifest, error) {
	raw, err := readArgument(cmd, cmd.Args().First())
	if err != nil {
		return SchemaManifest{}, err
	}
	decoded, err := DecodeJSON(bytes.NewReader(raw))
	if err != nil {
		return SchemaManifest{}, err
	}
	return schemaManifestFromWire(decoded)
}

func schemaManifestFromWire(decoded any) (SchemaManifest, error) {
	manifest, ok := objectMap(decoded)
	if !ok {
		return SchemaManifest{}, fail(ErrType, "schema manifest must be a JSON object")
	}
	if _, exists := manifest["fgraph"]; !exists {
		return SchemaManifest{}, fail(ErrSchema, "schema manifest is missing required field fgraph")
	}
	allowedTopLevel := map[string]bool{"fgraph": true, "digest": true, "attributes": true, "shapes": true}
	for field := range manifest {
		if !allowedTopLevel[field] {
			return SchemaManifest{}, fail(ErrSchema, "schema manifest has unknown field %q", field)
		}
	}
	if rawAttributes, exists := manifest["attributes"]; exists {
		attributes, isArray := rawAttributes.([]any)
		if !isArray {
			return SchemaManifest{}, fail(ErrSchema, "schema manifest attributes must be an array")
		}
		declarationFields := map[string]bool{
			"type": true, "many": true, "unique": true, "nohistory": true,
			"dims": true, "doc": true, "vector_model": true,
		}
		for index, rawAttribute := range attributes {
			attribute, isObject := objectMap(rawAttribute)
			if !isObject {
				return SchemaManifest{}, fail(ErrSchema, "schema manifest attribute %d must be an object", index)
			}
			if !exactKeys(attribute, "name", "declared") {
				return SchemaManifest{}, fail(ErrSchema, "schema manifest attribute %d needs exactly name and declared", index)
			}
			declared, declaredObject := objectMap(attribute["declared"])
			if !declaredObject {
				return SchemaManifest{}, fail(ErrSchema, "schema manifest attribute %d declared value must be an object", index)
			}
			for field := range declared {
				if !declarationFields[field] {
					return SchemaManifest{}, fail(ErrSchema, "schema manifest attribute %d has unknown declaration field %q", index, field)
				}
			}
		}
	}
	if rawShapes, exists := manifest["shapes"]; exists {
		shapes, isArray := rawShapes.([]any)
		if !isArray {
			return SchemaManifest{}, fail(ErrSchema, "schema manifest shapes must be an array")
		}
		for index, rawShape := range shapes {
			shape, isObject := objectMap(rawShape)
			if !isObject {
				return SchemaManifest{}, fail(ErrSchema, "schema manifest shape %d must be an object", index)
			}
			if !exactKeys(shape, "name", "required", "allowed", "closed") {
				return SchemaManifest{}, fail(ErrSchema, "schema manifest shape %d needs exactly name, required, allowed, and closed", index)
			}
			for _, field := range []string{"required", "allowed"} {
				if _, isArray := shape[field].([]any); !isArray {
					return SchemaManifest{}, fail(ErrSchema, "schema manifest shape %d field %s must be an array", index, field)
				}
			}
		}
	}
	delete(manifest, "digest") // Input digests are advisory; normalization always recomputes them.
	plain, err := plainJSONDepth(manifest, 0, MaxJSONDocumentDepth)
	if err != nil {
		return SchemaManifest{}, err
	}
	encoded, err := json.Marshal(plain)
	if err != nil {
		return SchemaManifest{}, wrap(ErrType, err, "cannot encode validated schema/1 manifest")
	}
	var result SchemaManifest
	if err := json.Unmarshal(encoded, &result); err != nil {
		return SchemaManifest{}, wrap(ErrType, err, "cannot decode schema/1 manifest")
	}
	return result, nil
}

func schemaCheckAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	manifest, err := decodeSchemaManifest(cmd)
	if err != nil {
		return err
	}
	result, err := db.CheckSchemaManifest(ctx, manifest)
	return outputResult(cmd, result, err)
}

func schemaApplyAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	manifest, err := decodeSchemaManifest(cmd)
	if err != nil {
		return err
	}
	report, err := db.ApplySchemaManifest(ctx, manifest, mutationOptions(cmd)...)
	return outputResult(cmd, report, err)
}

func portableInput(cmd *cli.Command, operation string) (io.Reader, func() error, error) {
	if !cmd.Args().Present() || cmd.Args().First() == "-" {
		return cmd.Root().Reader, func() error { return nil }, nil
	}
	file, err := os.Open(cmd.Args().First())
	if err != nil {
		return nil, nil, wrap(ErrFormat, err, "cannot open %s file %q", operation, cmd.Args().First())
	}
	return file, func() error { return wrapClose(file.Close(), operation+" file") }, nil
}

func applyAction(ctx context.Context, cmd *cli.Command, db *DB) (result error) {
	reader, closeInput, err := portableInput(cmd, "apply")
	if err != nil {
		return err
	}
	defer func() { result = joinErrors(result, closeInput()) }()
	summary, err := db.ApplySummary(ctx, reader)
	return outputResult(cmd, summary, err)
}

func snapshotAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	return db.Snapshot(ctx, cmd.Root().Writer)
}

func restoreAction(ctx context.Context, cmd *cli.Command, db *DB) (result error) {
	reader, closeInput, inputErr := portableInput(cmd, "restore")
	if inputErr != nil {
		return inputErr
	}
	defer func() { result = joinErrors(result, closeInput()) }()
	if restoreErr := db.Restore(ctx, reader); restoreErr != nil {
		return restoreErr
	}
	basis, basisErr := db.latestTx(ctx)
	return outputResult(cmd, map[string]any{"ok": true, "basis_tx": basis}, basisErr)
}

func undoAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	validated, err := loadValidatedCLIArguments(cmd)
	if err != nil {
		return err
	}
	report, err := db.Undo(ctx, validated.firstTransaction, mutationOptions(cmd)...)
	return outputResult(cmd, report, err)
}

func exciseAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	report, err := db.Excise(
		ctx,
		parseRef(cmd.Args().First()),
		WithOperationID(cmd.String("operation-id")),
		IfBasis(cmd.Int64("if-basis-tx")),
	)
	return outputResult(cmd, report, err)
}

func txAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	validated, err := loadValidatedCLIArguments(cmd)
	if err != nil {
		return err
	}
	receipt, err := db.Receipt(ctx, validated.firstTransaction)
	return outputResult(cmd, receipt, err)
}

func tailAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	since := cmd.Int64("since")
	if !cmd.Bool("follow") {
		return db.Tail(ctx, cmd.Root().Writer, since)
	}
	followCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for event := range db.Follow(followCtx, FollowOptions{Since: since}) {
		if event.Err != nil {
			return event.Err
		}
		line, err := canonicalJSON(event.Record)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintln(cmd.Root().Writer, string(line)); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func backupAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	if err := db.Backup(ctx, cmd.Args().First()); err != nil {
		return err
	}
	return outputResult(cmd, map[string]any{"path": cmd.Args().First()}, nil)
}

func doctorAction(ctx context.Context, cmd *cli.Command, db *DB) error {
	report, err := db.Doctor(ctx, cmd.Bool("repair"))
	return outputResult(cmd, report, err)
}

func doctorCLIAction(ctx context.Context, cmd *cli.Command) error {
	// The normal diagnostic path opens SQLite read-only; only an explicit
	// repair request is allowed to acquire a writer connection.
	return databaseAction(!cmd.Bool("repair"), doctorAction)(ctx, cmd)
}

func mcpAction(ctx context.Context, cmd *cli.Command) (result error) {
	if err := validateCLIUsage(cmd); err != nil {
		return err
	}
	write := cmd.Bool("write")
	db, err := openCLI(cmd, !write)
	if err != nil {
		return err
	}
	defer func() { result = joinErrors(result, db.Close()) }()
	options := MCPOptions{ReadOnly: !write, Write: write}
	if command := cmd.String("embed-cmd"); command != "" {
		options.Embed = commandEmbedder(command)
	}
	return RunMCP(ctx, db, options)
}

func commandEmbedder(command string) Embedder {
	return func(ctx context.Context, text string) ([]float32, error) {
		parts, err := parseEmbeddingCommand(command)
		if err != nil {
			return nil, err
		}
		runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		// #nosec G204 -- the user explicitly configures an executable and arguments;
		// CommandContext never invokes a shell, so input text cannot alter the command.
		cmd := exec.CommandContext(runCtx, parts[0], parts[1:]...)
		// A descendant may inherit stdout after the configured process exits;
		// bound pipe cleanup so that it cannot outlive the command timeout.
		cmd.WaitDelay = 100 * time.Millisecond
		cmd.Stdin = strings.NewReader(text)
		output := &limitedCommandOutput{limit: 1 << 20}
		cmd.Stdout = output
		err = cmd.Run()
		if err != nil {
			if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
				return nil, fail(ErrType, "embedding command %q timed out after 60 seconds; use a bounded local executable", parts[0])
			}
			return nil, wrap(ErrType, err, "embedding command %q failed; verify --embed-cmd is an executable that reads text and writes a JSON vector", parts[0])
		}
		return parseVectorJSON(output.String())
	}
}

func parseEmbeddingCommand(command string) ([]string, error) {
	trimmed := strings.TrimSpace(command)
	if trimmed == "" {
		return nil, fail(ErrType, "embedding command is empty; provide an executable path or JSON argv array")
	}
	if !strings.HasPrefix(trimmed, "[") {
		return []string{trimmed}, nil
	}
	decoded, err := DecodeJSON(strings.NewReader(trimmed))
	if err != nil {
		return nil, fail(ErrType, "embedding command JSON argv is invalid; provide [\"executable\",\"arg\",...]: %v", err)
	}
	items, ok := decoded.([]any)
	if !ok || len(items) == 0 {
		return nil, fail(ErrType, "embedding command JSON argv must be a non-empty string array")
	}
	parts := make([]string, len(items))
	for index, item := range items {
		part, ok := item.(string)
		if !ok {
			return nil, fail(ErrType, "embedding command JSON argv item %d must be text", index)
		}
		if index == 0 && part == "" {
			return nil, fail(ErrType, "embedding command JSON argv executable must be non-empty")
		}
		parts[index] = part
	}
	return parts, nil
}

type limitedCommandOutput struct {
	bytes.Buffer
	limit int
}

func (output *limitedCommandOutput) Write(data []byte) (int, error) {
	if len(data) > output.limit-output.Len() {
		return 0, fail(ErrType, "embedding output exceeds 1 MiB; emit one compact JSON vector")
	}
	return output.Buffer.Write(data)
}

func parseVectorJSON(raw string) ([]float32, error) {
	value, err := DecodeJSON(strings.NewReader(raw))
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fail(ErrType, "vector is %T; use a JSON number array", value)
	}
	wrapped, err := wrappedValue("vector", items)
	if err != nil {
		return nil, err
	}
	vector, ok := wrapped.logical.([]float32)
	if !ok {
		return nil, fail(ErrType, "embedding output decoded as %T; emit a JSON number array", wrapped.logical)
	}
	if err := validateCosineVector(vector); err != nil {
		return nil, err
	}
	return vector, nil
}

func readArgument(cmd *cli.Command, argument string) ([]byte, error) {
	if argument == "-" {
		data, err := io.ReadAll(cmd.Root().Reader)
		if err != nil {
			return nil, wrap(ErrFormat, err, "cannot read stdin")
		}
		return data, nil
	}
	if strings.HasPrefix(argument, "@") {
		data, err := os.ReadFile(strings.TrimPrefix(argument, "@"))
		if err != nil {
			return nil, wrap(ErrFormat, err, "cannot read %q", argument)
		}
		return data, nil
	}
	return []byte(argument), nil
}

func parseRef(text string) any {
	if id, err := strconv.ParseInt(text, 10, 64); err == nil {
		return id
	}
	return text
}

func parseJSONOrText(text string) any {
	value, err := DecodeJSON(strings.NewReader(text))
	if err == nil {
		return value
	}
	return text
}
