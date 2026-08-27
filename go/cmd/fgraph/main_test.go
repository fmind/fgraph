package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
)

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

func TestRunExitCodesAndDiagnostics(t *testing.T) {
	ctx := context.Background()
	var stdout, stderr bytes.Buffer
	if code := run(ctx, []string{"fgraph", "version"}, strings.NewReader(""), &stdout, &stderr); code != 0 || stdout.Len() == 0 || stderr.Len() != 0 {
		t.Fatalf("version code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	usageDB := t.TempDir() + "/usage.db"
	usageArgs := []string{"fgraph", "--db", usageDB, "add"}
	if code := run(ctx, usageArgs, strings.NewReader(""), &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "add needs exactly one") {
		t.Fatalf("usage code=%d stderr=%q", code, stderr.String())
	}
	if code := run(ctx, usageArgs, strings.NewReader(""), &stdout, failingWriter{}); code != 1 {
		t.Fatalf("unwritable usage diagnostic code=%d", code)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run(ctx, []string{"fgraph", "--db", t.TempDir(), "info"}, strings.NewReader(""), &stdout, &stderr); code != 1 || stderr.Len() == 0 {
		t.Fatalf("typed failure code=%d stderr=%q", code, stderr.String())
	}
	if code := run(ctx, []string{"fgraph", "--db", t.TempDir(), "info"}, strings.NewReader(""), &stdout, failingWriter{}); code != 1 {
		t.Fatalf("unwritable typed diagnostic code=%d", code)
	}
}

func TestMainRunsSuccessfulCommand(t *testing.T) {
	reader, writer, pipeErr := os.Pipe()
	if pipeErr != nil {
		t.Fatal(pipeErr)
	}
	oldArgs, oldStdout := os.Args, os.Stdout
	os.Args, os.Stdout = []string{"fgraph", "version"}, writer
	defer func() {
		os.Args, os.Stdout = oldArgs, oldStdout
	}()

	main()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	output, readErr := io.ReadAll(reader)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(output), "1.0.2") {
		t.Fatalf("main version output = %q", output)
	}
}
