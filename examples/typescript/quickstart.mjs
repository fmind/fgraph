import { GENESIS_TX, connect } from "../../typescript/dist/index.js";

const db = connect(":memory:");
try {
  const created = db.transact(
    { id: "ada", "person/name": "Ada Lovelace", "person/city": "London" },
    {
      source: "quickstart",
      by: "example",
      operationId: "quickstart:ada:1",
      ifBasisTx: GENESIS_TX,
    },
  );
  const retried = db.transact(
    { id: "ada", "person/name": "Ada Lovelace", "person/city": "London" },
    {
      source: "quickstart",
      by: "example",
      operationId: "quickstart:ada:1",
      ifBasisTx: GENESIS_TX,
    },
  );
  if (retried.status !== "already_applied" || retried.event !== created.event) {
    throw new Error("idempotent retry did not return the original receipt");
  }

  db.declare("person/name", { type: "text" });
  db.declare("person/knows", { ref: true, many: true });
  db.defineShape("shape/person", {
    required: ["person/name"],
    allowed: ["person/city", "person/knows"],
    closed: true,
  });
  db.transact({ id: "grace", "person/name": "Grace Hopper" });
  db.transact({ id: "ada", "fgraph/shape": { ref: "shape/person" } });
  const beforeMove = db.transact({
    id: "ada",
    "person/knows": { ref: "grace" },
  });
  const moved = db.transact({ id: "ada", "person/city": "Lyon" });
  const guarded = db.transact(["cas", "ada", "person/city", "Lyon", "Paris"]);
  if (db.entity("ada")["person/city"] !== "Paris") {
    throw new Error("compare-and-swap did not atomically replace the value");
  }

  const result = db.q({
    find: ["?friend"],
    where: [
      ["ada", "person/knows", "?friendEntity"],
      ["?friendEntity", "person/name", "?friend"],
    ],
  });
  if (JSON.stringify(result.rows) !== JSON.stringify([["Grace Hopper"]])) {
    throw new Error(`unexpected query result: ${JSON.stringify(result)}`);
  }
  if (db.at(beforeMove.tx).entity("ada")["person/city"] !== "London") {
    throw new Error("historical view did not preserve the superseded fact");
  }
  if (
    !db.search({ text: "Grace Hopper", textAttributes: ["person/name"] }).hits
      .length
  ) {
    throw new Error("bounded text search did not find Grace Hopper");
  }
  if (db.validate("ada").valid !== true) {
    throw new Error("shape validation unexpectedly failed");
  }

  console.log(db.receipt(moved.tx));
  console.log(db.receipt(guarded.tx));
  console.log(db.eventRecords(beforeMove.tx));
  console.log(
    db.explain({ find: ["?e"], where: [["?e", "person/name", "_"]] }),
  );
} finally {
  db.close();
}
