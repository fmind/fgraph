package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/urfave/cli/v3"

	fgraph "github.com/fmind/fgraph/go"
)

func main() {
	code := run(context.Background(), os.Args, os.Stdin, os.Stdout, os.Stderr)
	if code != 0 {
		os.Exit(code)
	}
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	err := fgraph.RunCLI(ctx, args, stdin, stdout, stderr)
	if err == nil {
		return 0
	}
	var exit cli.ExitCoder
	if errors.As(err, &exit) {
		if exit.Error() != "" {
			if _, writeErr := fmt.Fprintln(stderr, exit.Error()); writeErr != nil {
				return 1
			}
		}
		return 2
	}
	// Typed fgraph errors already render their taxonomy prefix exactly once.
	if _, writeErr := fmt.Fprintln(stderr, err); writeErr != nil {
		return 1
	}
	return 1
}
