# /// script
# requires-python = ">=3.12"
# dependencies = ["fgraph"]
# ///
"""fgraph quickstart: facts in, history kept, questions answered."""

import fgraph

db = fgraph.connect(":memory:")  # a file path persists; :memory: is perfect for demos

# No schema needed — just assert facts, with provenance on the transaction.
db.transact(
    {"id": "ada", "person/name": "Ada Lovelace", "person/city": "London"},
    source="quickstart",
    by="example",
)

# Declare only special behavior: references and cardinality-many.
db.declare("person/knows", ref=True, many=True)
db.transact({"id": "grace", "person/name": "Grace Hopper"})
db.transact({"id": "ada", "person/knows": {"ref": "grace"}})

# Supersede a cardinality-one value: London is retracted, both moments kept.
report = db.transact({"id": "ada", "person/city": "Lyon"})

# Read the present.
assert db.entity("ada")["person/city"] == "Lyon"
result = db.q(
    find=["?friend"],
    where=[["ada", "person/knows", "?f"], ["?f", "person/name", "?friend"]],
)
assert result.rows == [["Grace Hopper"]]

# Read the past: the world before the move, and the timeline of the change.
assert db.at(report.tx - 1).entity("ada")["person/city"] == "London"
for fact in db.history("ada", "person/city"):
    print(fact["v"], "from tx", fact["tx"], "until", fact["rx"] or "now")

# Ask why a fact is believed: value + full transaction provenance.
print(db.why("ada", "person/city"))
