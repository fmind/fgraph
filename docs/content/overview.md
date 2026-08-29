---
title: Overview
weight: 1
url: /docs/
next: /docs/getting-started
description: A portable temporal fact graph and agent memory store in one SQLite file.
---

fgraph (Facts Graph) is an embedded temporal fact store: retained facts `⟨entity, attribute, value⟩` in one SQLite file, with durable receipts, basis-safe writes, schema introspection, bounded Datalog, attributable hybrid search, and native Python, Go, and TypeScript implementations. History is retained by default; `nohistory` is a storage policy, while audited `excise()` is the privacy-deletion path. Start with [Getting Started](getting-started), understand the model in [Concepts](concepts), then wire it into agents in [Integrations](integrations).

<!--more-->

{{< cards >}} {{< card link="getting-started" title="Getting Started" icon="sparkles" >}} {{< card link="concepts" title="Concepts" >}} {{< card link="modeling-time" title="Modeling time and uncertainty" >}} {{< card link="sharing-memory" title="Sharing and auditing memory" >}} {{< card link="rag" title="RAG with fgraph" >}} {{< card link="integrations" title="Integrations" >}} {{< card link="operations" title="Operations and safety boundaries" >}} {{< card link="cli" title="CLI Reference" >}} {{< card link="sdk" title="SDK Reference" >}} {{< card link="spec" title="fgraph v1 specification" >}} {{< /cards >}}
