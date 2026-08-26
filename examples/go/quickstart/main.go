// fgraph quickstart in Go: the same semantics and file format as every peer.
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
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("close fgraph: %v", closeErr)
		}
	}()

	// Assert facts with provenance; attributes need no declaration.
	_, err = db.Transact(ctx,
		fgraph.E{"id": "ada", "person/name": "Ada Lovelace", "person/city": "London"},
		fgraph.WithSource("quickstart"), fgraph.WithBy("example"),
		fgraph.WithOperationID("quickstart:ada"), fgraph.IfBasis(fgraph.GenesisTx))
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
	if _, err = db.DeclareShape(ctx, "shape/person", fgraph.ShapeDefinition{
		Required: []string{"person/name"},
		Allowed:  []string{"person/city", "person/knows"},
		Closed:   true,
	}); err != nil {
		log.Fatal(err)
	}
	if _, err = db.Transact(ctx, fgraph.E{"id": "ada", "fgraph/shape": fgraph.RefTo("shape/person")}); err != nil {
		log.Fatal(err)
	}
	validation, err := db.Validate(ctx, "ada")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("shape valid:", validation.Valid)
	beforeMove, err := db.Transact(ctx, fgraph.E{"id": "ada", "person/knows": fgraph.RefTo("grace")})
	if err != nil {
		log.Fatal(err)
	}

	// Supersede: the old value is retracted in the same transaction, history kept.
	report, err := db.Transact(ctx, fgraph.E{"id": "ada", "person/city": "Lyon"})
	if err != nil {
		log.Fatal(err)
	}
	guarded, err := db.Transact(ctx, []any{"cas", "ada", "person/city", "Lyon", "Paris"})
	if err != nil {
		log.Fatal(err)
	}

	// Present, past, and provenance.
	entity, err := db.Entity(ctx, "ada")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("now:", entity["person/city"], "at tx", guarded.Tx) // Paris
	fmt.Println("superseded Lyon at tx", report.Tx)

	pastView, err := db.At(ctx, beforeMove.Tx)
	if err != nil {
		log.Fatal(err)
	}
	past, err := pastView.Entity(ctx, "ada")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("before:", past["person/city"]) // London

	history, err := db.History(ctx, "ada", "person/city")
	if err != nil {
		log.Fatal(err)
	}
	for _, fact := range history {
		until := any("now")
		if fact.Rx != nil {
			until = *fact.Rx
		}
		fmt.Printf("%v: tx %d -> %v\n", fact.V, fact.Tx, until)
	}
}
