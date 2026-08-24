// A small project knowledge base with Datalog queries and hybrid search —
// usage mode 2 from SPEC.md §1 (the same database `fgraph mcp --read-only` exposes).
package main

import (
	"context"
	"fmt"
	"log"

	fgraph "github.com/fmind/fgraph/go"
)

func main() {
	ctx := context.Background()

	db, err := fgraph.Open("project-kb.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if _, err = db.Declare(ctx, "service/depends", fgraph.Ref(), fgraph.Many()); err != nil {
		log.Fatal(err)
	}
	for _, tx := range []fgraph.E{
		{"id": "api", "service/lang": "Go", "service/doc": "Public HTTP API, single binary."},
		{"id": "worker", "service/lang": "Python", "service/doc": "Async jobs and embeddings."},
		{"id": "store", "service/lang": "Go", "service/doc": "fgraph fact store shared by all services."},
		{"id": "api", "service/depends": fgraph.RefTo("store")},
		{"id": "worker", "service/depends": fgraph.RefTo("store")},
	} {
		if _, err = db.Transact(ctx, tx, fgraph.WithSource("architecture.md")); err != nil {
			log.Fatal(err)
		}
	}

	// Datalog: which Go services depend on the store?
	result, err := db.Query(ctx, fgraph.Q{
		Find: []any{"?s"},
		Where: []any{
			[]any{"?s", "service/lang", "Go"},
			[]any{"?s", "service/depends", fgraph.RefTo("store")},
		},
	}, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Go services on the store:", result.Rows) // [[{"ref": "api"}]]

	// Keyword search with one hop of graph expansion around the hits.
	hits, err := db.Search(ctx, fgraph.SearchOpts{Text: "embeddings jobs", K: 3, Expand: 1})
	if err != nil {
		log.Fatal(err)
	}
	for _, hit := range hits.Hits {
		fmt.Println(hit.Entity, hit.Score)
	}
}
