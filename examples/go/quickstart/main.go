// fgraph quickstart in Go: same semantics as the Python twin, same file format.
package main

import (
	"context"
	"fmt"
	"log"

	fgraph "github.com/fmind/fgraph/go"
)

func main() {
	ctx := context.Background()

	db, err := fgraph.Open(":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Assert facts with provenance; attributes need no declaration.
	_, err = db.Transact(ctx,
		fgraph.E{"id": "ada", "person/name": "Ada Lovelace", "person/city": "London"},
		fgraph.WithSource("quickstart"), fgraph.WithBy("example"))
	if err != nil {
		log.Fatal(err)
	}

	// Declare only special behavior, then link entities by reference.
	if _, err = db.Declare(ctx, "person/knows", fgraph.Ref(), fgraph.Many()); err != nil {
		log.Fatal(err)
	}
	if _, err = db.Transact(ctx, fgraph.E{"id": "grace", "person/name": "Grace Hopper"}); err != nil {
		log.Fatal(err)
	}
	if _, err = db.Transact(ctx, fgraph.E{"id": "ada", "person/knows": fgraph.RefTo("grace")}); err != nil {
		log.Fatal(err)
	}

	// Supersede: the old value is retracted in the same transaction, history kept.
	report, err := db.Transact(ctx, fgraph.E{"id": "ada", "person/city": "Lyon"})
	if err != nil {
		log.Fatal(err)
	}

	// Present, past, and provenance.
	entity, _ := db.Entity(ctx, "ada")
	fmt.Println("now:", entity["person/city"]) // Lyon

	past, _ := db.At(report.Tx - 1).Entity(ctx, "ada")
	fmt.Println("before:", past["person/city"]) // London

	history, _ := db.History(ctx, "ada", "person/city")
	for _, fact := range history {
		fmt.Printf("%v: tx %d -> %v\n", fact.V, fact.Tx, fact.Rx)
	}
}
