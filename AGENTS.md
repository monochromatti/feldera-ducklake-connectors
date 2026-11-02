# AGENTS.md

Guidance for agents working in this repository.

## Scope

This file applies to the whole repository. It is self-contained: the parent
directory's instructions do **not** apply here.

## What this repository is

Two connectors that move data between a DuckLake catalog and a Feldera pipeline:
`DuckLake → Feldera` (input) and `Feldera → DuckLake` (output). The publication
protocol they implement is derived from a Lean 4 development vendored under
`formal/`.

The protocol exists because a Feldera pipeline and a DuckLake catalog cannot
commit together, so a naive implementation either loses output (the engine
reports a step processed, the writer dies before committing) or duplicates it
(the writer retries a commit that actually landed, silently corrupting
`SUM`/`COUNT` views). The fix is a single protected **guard row** per output
group, updated in the same transaction as the output it describes.

## Where the truth lives

| Question | Read |
|---|---|
| Why is the protocol shaped this way? | `formal/REPORT.md` (verdict, obligations, trust boundary) |
| Which theorem justifies which code path? | `docs/design.md` |
| What are the rules of the protocol? | `src/fd_connectors/protocol.py` — its docstrings carry the rationale, not just descriptions |
| What is the contract? | `src/fd_connectors/scenarios.py` + `tests/` |
| How do I run it? | `README.md` |

## Hard rules

1. **Never weaken a scenario or test expectation to make it pass.** Scenario and
   test names encode protocol requirements (see `docs/design.md` for the mapping
   to the formal witnesses). If a change makes one fail, the change is wrong. If
   you believe the requirement itself is wrong, say so explicitly and change the
   model in `formal/` in the same piece of work — do not quietly relax the check.
2. **`uv run fd-connectors validate-local` must pass before every commit.** It is
   the executable counterpart of the formal witnesses and runs without Feldera.
   Treat it as the gate; `pytest tests/` alone is not enough for protocol changes.
3. **Keep `protocol.py` pure.** It must contain no I/O, no connection objects and
   no HTTP. I/O belongs in `catalog.py`, `feldera.py` and `journal.py`. This is
   what makes the decision function testable in isolation.
4. **Every commit must be a working state**: importable, `ruff check` clean,
   `ruff format --check` clean, and the tests that exist at that commit pass.
5. **Never edit `formal/`.** It is vendored byte-for-byte and every file is
   checksummed in `formal/MANIFEST.json`. If the protocol changes, the model must
   change — record that as a deliberate act (see rule 1), not by rewriting the
   vendored artifact.
6. **Write empirical findings down where they can be found again.** Facts
   discovered by probing (the exact egress chunk shape, which edition features
   return `501`, which extension imports are required) belong in a module
   docstring or a regression test, in the same commit that depends on them. The
   wire format captured in `tests/test_wire_format.py` is the model to follow.
7. **Do not infer development history from `git log`.** The history was
   reconstructed as an atomic, linear split of an earlier two-commit history, and
   some intermediate `__init__.py` contents were derived mechanically rather
   than written by hand. Commit order here is a reading order, not a
   chronological record.
8. **Keep diffs minimal and task-focused.** No broad refactors, no reformatting
   of untouched files, no new dependencies without a reason recorded in the
   commit body.

## Commands

```bash
devenv shell                 # python 3.13, uv, duckdb CLI, psql, postgres, lean4, docker-client
uv sync --group dev          # or rely on devenv
uv run fd-connectors validate-local      # 18 protocol scenarios, no Feldera needed
uv run fd-connectors validate-local --only guard/     # one family
uv run pytest tests -q                    # unit + wire-format tests
feldera-up                                # start the pipeline-manager container on :18080
uv run pytest tests/test_end_to_end.py -q  # e2e; skips itself when no stack answers
nix fmt 2>/dev/null || uv run ruff format src tests
```

`devenv.nix` also exposes `fd-test`, `fd-scenarios`, `feldera-up` and `fd-e2e`
scripts, which the README documents.

## Environment notes

- The e2e module skips unless `$FELDERA_URL` (default `http://127.0.0.1:18080`)
  answers `/v0/config`, so a missing stack produces a *silent* skip — check that
  the tests actually ran rather than passing vacuously.
- The Feldera container accumulates compiled artifacts and leaked pipelines fast.
  See *Operating the local stack* in `README.md` before concluding a compile
  failure is a code problem.
- `pip install -e .` in this repo is a plain `.pth`; when verifying a commit in a
  separate checkout, set `PYTHONPATH=<tree>/src` **and** assert
  `fd_connectors.__file__` points into that tree, or you will test the wrong copy.

## Style

- Python: line length 100, target `py311`, `ruff` defaults with `E501` ignored
  (see `[tool.ruff]` in `pyproject.toml`). Prefer `StrEnum` over
  `str, Enum`; use `zip(..., strict=True)`, `contextlib.suppress` and
  `collections.abc` imports.
- Docstrings state **why**, especially where a plausible alternative was rejected.
  `protocol.decide` and `protocol.acquire_ownership` are the reference examples.
- Tests state the failure mode they prevent in the module docstring, so a future
  reader knows what regression they guard.