package fgraph

import (
	"errors"
	"fmt"
)

var (
	ErrNotFound    = errors.New("NotFound")
	ErrConflict    = errors.New("Conflict")
	ErrSchema      = errors.New("SchemaError")
	ErrType        = errors.New("TypeError")
	ErrQuery       = errors.New("QueryError")
	ErrFormat      = errors.New("FormatError")
	ErrReadOnly    = errors.New("ReadOnly")
	ErrTooLarge    = errors.New("TooLarge")
	ErrUnsupported = errors.New("Unsupported")
)

// Error gives every public failure a stable cross-language name while keeping
// errors.Is useful to Go callers.
type Error struct {
	Kind    error
	Cause   error
	Message string
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Kind, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Kind, e.Message)
}

func (e *Error) Unwrap() []error {
	if e.Cause == nil {
		return []error{e.Kind}
	}
	return []error{e.Kind, e.Cause}
}

func fail(kind error, format string, args ...any) error {
	return &Error{Kind: kind, Message: fmt.Sprintf(format, args...)}
}

func wrap(kind, cause error, format string, args ...any) error {
	return &Error{Kind: kind, Message: fmt.Sprintf(format, args...), Cause: cause}
}

func joinErrors(current, next error) error {
	if next == nil {
		return current
	}
	if current == nil {
		return next
	}
	return errors.Join(current, next)
}

func wrapClose(err error, description string) error {
	if err == nil {
		return nil
	}
	return wrap(ErrFormat, err, "cannot close %s", description)
}

// ErrorName is the normative error taxonomy name used by the CLI and tests.
func ErrorName(err error) string {
	for _, item := range []struct {
		err  error
		name string
	}{
		{ErrNotFound, "NotFound"},
		{ErrConflict, "Conflict"},
		{ErrSchema, "SchemaError"},
		{ErrType, "TypeError"},
		{ErrQuery, "QueryError"},
		{ErrFormat, "FormatError"},
		{ErrReadOnly, "ReadOnly"},
		{ErrTooLarge, "TooLarge"},
		{ErrUnsupported, "Unsupported"},
	} {
		if errors.Is(err, item.err) {
			return item.name
		}
	}
	return "Error"
}
