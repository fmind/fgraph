# Releasing fgraph

One tag-triggered workflow proves the exact source, builds and smokes every archive, publishes the registries, and completes the GitHub release. A local green checkout is only a candidate.

## 1. Freeze the candidate

1. Confirm every runtime reports the same version with `mise run check:versions`.
1. Review `docs/content/docs/spec.md` and the matching shared conformance cases.
1. Move user-visible changes from `Unreleased` to the dated version in `CHANGELOG.md`.
1. Confirm package, file-format, event, snapshot, and minimum-runtime compatibility.

## 2. Prove source and benchmark provenance

Benchmarks must identify a clean source commit. Use two commits because measuring necessarily changes the tracked benchmark outputs:

1. Commit the source, dependency, tool, specification, and benchmark-harness candidate.
1. From that clean commit, run `mise run all`, then `mise run benchmark`.
1. Review `benchmarks/latest.ndjson`, both SVGs, and the README tables. Commit only those generated observations and documentation updates.
1. From a clean checkout of the final commit, run `mise run release:verify`.
1. Push `main` and require green CI, Security, and Pages for that exact SHA.

Any source, dependency, tool, specification, or benchmark-harness change after measurement invalidates the benchmark digest and requires a new clean-source run.

## 3. Configure registries once

- Keep the protected GitHub `release` environment and its required maintainer review.
- Configure PyPI trusted publishing for project `fgraph`, workflow `release.yml`, and environment `release`.
- For the first `@fmind-dev/fgraph` publication, add a short-lived read-write granular npm token with **Bypass 2FA** enabled as the `release` environment secret `NPM_TOKEN`. After the package exists, run `npm trust github @fmind-dev/fgraph --repository fmind/fgraph --file release.yml --environment release --allow-publish`, verify it with `npm trust list @fmind-dev/fgraph`, delete the GitHub secret, and revoke the token.

Never put a registry token in the repository or workflow source.

## 4. Publish

Create both annotated tags on the proved commit and push them atomically:

```bash
git tag -a v1.0.3 -m "fgraph v1.0.3"
git tag -a go/v1.0.3 -m "fgraph Go module v1.0.3"
candidate="$(git rev-parse HEAD)"
test "$(git rev-list -n 1 v1.0.3)" = "$candidate"
test "$(git rev-list -n 1 go/v1.0.3)" = "$candidate"
git push --atomic origin v1.0.3 go/v1.0.3
```

The root tag starts `.github/workflows/release.yml`. The workflow:

1. reruns the clean release gate and verifies both tags;
1. builds deterministic Python, npm, and five native Go archives plus checksums;
1. executes every Go archive on its native runner;
1. waits at the protected `release` environment;
1. creates a draft GitHub release with the exact assets;
1. publishes npm, then PyPI, then makes the GitHub release public.

Tags and registries are append-only. If publication partially fails, keep the GitHub release draft, document the state, and fix forward with a patch version; never move a public tag.

## 5. Verify as a consumer

Test outside the checkout:

```bash
uvx --from fgraph==1.0.3 fgraph version
go install github.com/fmind/fgraph/go/cmd/fgraph@v1.0.3
npx --yes @fmind-dev/fgraph@1.0.3 --version
```

Download the release assets, verify `SHA256SUMS`, then run `gh release verify v1.0.3` and `gh release verify-asset v1.0.3 <artifact>`. Create one database in each runtime, open it read-only from the other two, run `doctor`, and confirm the public docs and release links before announcing the release.
