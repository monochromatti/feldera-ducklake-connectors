"""Command line entry points.

``validate-local`` runs the DuckLake protocol scenarios against throwaway
catalogs; it needs no Feldera instance and is the fast check to run before
touching the connectors.  The ``input-run`` / ``output-run`` commands wire the
connectors to a live pipeline-manager, and ``guard-show`` inspects a group's
protected row.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from .catalog import DuckLake
from .feldera import FelderaClient
from .input_connector import InputConnector, InputSpec
from .output_connector import OutputConnector, OutputSpec
from .scenarios import SCENARIOS, run_all


def _common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--catalog", required=True, help="DuckLake ATTACH target")
    parser.add_argument("--data-path", default=None, help="DuckLake DATA_PATH")
    parser.add_argument("--schema", default="main")
    parser.add_argument("--guard-table", default="connector_guard")


def _lake(args: argparse.Namespace, tables: Sequence[str], keys: dict[str, list[str]]) -> DuckLake:
    return DuckLake(
        catalog=args.catalog,
        data_path=args.data_path,
        tables=list(tables),
        key_columns=keys,
        schema=args.schema,
        guard_table=args.guard_table,
    )


def _key_columns(raw: Sequence[str] | None) -> dict[str, list[str]]:
    keys: dict[str, list[str]] = {}
    for item in raw or ():
        table, _, columns = item.partition("=")
        keys[table] = [c.strip() for c in columns.split(",") if c.strip()]
    return keys


def cmd_validate_local(args: argparse.Namespace) -> int:
    results = run_all(args.workdir, names=args.only)
    payload = [result.as_dict() for result in results]
    if args.json:
        print(json.dumps(payload, indent=2))
    elif not results:
        print(f"no scenarios match {args.only}", file=sys.stderr)
        print(f"known: {', '.join(name for name, _ in SCENARIOS)}", file=sys.stderr)
    else:
        width = max(len(result.name) for result in results)
        for result in results:
            mark = "PASS" if result.passed else "FAIL"
            print(f"{mark}  {result.name.ljust(width)}  {result.detail}")
        failed = [r for r in results if not r.passed]
        print(f"\n{len(results) - len(failed)}/{len(results)} scenarios passed")
        for result in failed:
            print(f"\n--- {result.name} ---\n{result.traceback}", file=sys.stderr)
    return 1 if any(not result.passed for result in results) else 0


def cmd_guard_show(args: argparse.Namespace) -> int:
    with _lake(args, tables=[], keys={}) as lake:
        guard = lake.read_guard(args.output_group)
        print(json.dumps(guard.to_row() | {"snapshot": lake.current_snapshot()}, indent=2))
        print(
            json.dumps(
                [
                    {"snapshot_id": snapshot, "time": str(time), "changes": changes}
                    for snapshot, time, changes in lake.snapshots()[-5:]
                ],
                indent=2,
            )
        )
    return 0


def cmd_input_run(args: argparse.Namespace) -> int:
    keys = _key_columns(args.key_column)
    with _lake(args, tables=[args.source_table], keys=keys) as lake:
        client = FelderaClient(args.feldera_url)
        connector = InputConnector(
            lake,
            client,
            pipeline=args.pipeline,
            generation=args.generation,
            specs=[
                InputSpec(
                    source_table=args.source_table,
                    feldera_table=args.feldera_table,
                    schema=args.schema,
                    key_columns=tuple(keys.get(args.source_table, ())),
                )
            ],
            state_dir=args.state_dir,
            batch_size=args.batch_size,
        )
        connector.recover(args.recover)
        try:
            for progress in connector.run(iterations=args.iterations, interval=args.interval):
                print(json.dumps([item.as_dict() for item in progress]))
        finally:
            client.close()
    return 0


def cmd_output_run(args: argparse.Namespace) -> int:
    keys = _key_columns(args.key_column)
    tables = [spec.split(":", 1)[1] for spec in args.view]
    specs = [
        OutputSpec(
            view=spec.split(":", 1)[0],
            table=spec.split(":", 1)[1],
            key_columns=tuple(keys.get(spec.split(":", 1)[1], ())),
        )
        for spec in args.view
    ]
    with _lake(args, tables=tables, keys=keys) as lake:
        client = FelderaClient(args.feldera_url)
        connector = OutputConnector(
            lake,
            client,
            pipeline=args.pipeline,
            output_group=args.output_group,
            generation=args.generation,
            specs=specs,
            epoch=args.epoch,
            send_snapshot=not args.no_snapshot,
        )
        try:
            status = connector.run(stop_after_idle=args.idle_timeout, max_batches=args.max_batches)
            print(json.dumps(status.as_dict(), indent=2))
        finally:
            client.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fd-connectors", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    validate = sub.add_parser("validate-local", help="run the DuckLake protocol scenarios")
    validate.add_argument("--workdir", type=Path, default=None)
    validate.add_argument("--only", action="append", default=None, help="scenario name prefix")
    validate.add_argument("--json", action="store_true")
    validate.set_defaults(func=cmd_validate_local)

    guard = sub.add_parser("guard-show", help="print an output group's guard row")
    _common(guard)
    guard.add_argument("--output-group", required=True)
    guard.set_defaults(func=cmd_guard_show)

    inp = sub.add_parser("input-run", help="run the input connector")
    _common(inp)
    inp.add_argument("--feldera-url", required=True)
    inp.add_argument("--pipeline", required=True)
    inp.add_argument("--generation", required=True)
    inp.add_argument("--source-table", required=True)
    inp.add_argument("--feldera-table", required=True)
    inp.add_argument("--state-dir", required=True)
    inp.add_argument("--key-column", action="append", default=None, help="table=col[,col]")
    inp.add_argument("--batch-size", type=int, default=1000)
    inp.add_argument("--iterations", type=int, default=None)
    inp.add_argument("--interval", type=float, default=1.0)
    inp.add_argument("--recover", choices=["fail", "rebootstrap"], default="fail")
    inp.set_defaults(func=cmd_input_run)

    out = sub.add_parser("output-run", help="run the output connector")
    _common(out)
    out.add_argument("--feldera-url", required=True)
    out.add_argument("--pipeline", required=True)
    out.add_argument("--output-group", required=True)
    out.add_argument("--generation", required=True)
    out.add_argument("--view", action="append", required=True, help="view:table")
    out.add_argument("--key-column", action="append", default=None, help="table=col[,col]")
    out.add_argument("--epoch", type=int, default=None)
    out.add_argument("--no-snapshot", action="store_true")
    out.add_argument("--idle-timeout", type=float, default=5.0)
    out.add_argument("--max-batches", type=int, default=None)
    out.set_defaults(func=cmd_output_run)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result: Any = args.func(args)
    return int(result or 0)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
