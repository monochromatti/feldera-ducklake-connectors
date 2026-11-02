# Feldera–DuckLake proof round

Read `REPORT.md` for conclusions, assumptions, source links and limitations.

Contents:

- `Connectors.lean`: 24 positive theorems and 14 counterexample theorems.
- `Audit.lean`: axiom audit of all theorem declarations.
- `lean-verification.log`, `lean-axiom-audit.log`: successful checked output.
- `sql_checks.py`, `sql-results.json`: nine SQL scenarios and recorded results.
- `MANIFEST.json`: pinned versions and file checksums.
- `runtime_compat.cpp`: environment-only executable-path shim used for this run.

With Lean 4.19.0 installed:

```bash
bash verify.sh
```

For the separate empirical SQL checks:

```bash
python -m pip install -r requirements.txt
python sql_checks.py
```

The SQL script downloads official DuckLake into a local `.tools` directory next
to the extracted bundle. Compare the installed extension version with the
recorded build. SQL checks do not run Feldera and do not prove SQL refinement.
