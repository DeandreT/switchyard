#!/usr/bin/env python3
"""Run selected exact ignored SDK gates serially, retaining every outcome."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys


CELLS = {
    f"{pin}_{backend}_{transport}": (
        "amqp_dotnet_current" if transport == "tcp" else "amqp_dotnet_websockets"
    )
    for pin in ("declared_current", "previous")
    for backend in ("memory", "fjall")
    for transport in ("tcp", "wss")
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cell", action="append", choices=CELLS, help="repeat to select; default all eight")
    parser.add_argument("--output", type=Path, required=True, help="new external evidence directory (also holds scratch)")
    parser.add_argument("--jobs", type=int, choices=(1, 2), default=2)
    parser.add_argument("--cargo-wrapper", type=Path, help="optional Bash build-lease launcher, taking Cargo arguments")
    args = parser.parse_args()
    if any(name in os.environ for name in ("SWITCHYARD_SDK_TEST_TIMER_LAG_MS", "SWITCHYARD_SDK_DIAG_DELAY_RECEIVE")):
        parser.error("deliberate delay probes cannot be recorded as matrix qualification; run their exact Cargo control separately")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    scratch = output / "tmp"
    scratch.mkdir()
    selected = set(args.cell or CELLS)
    env = os.environ.copy()
    env.update(TMPDIR=str(scratch), DOTNET_ADD_GLOBAL_TOOLS_TO_PATH="false", DOTNET_PROCESSOR_COUNT="2")
    repository = Path(__file__).resolve().parents[2]
    rows = []
    for cell, target in CELLS.items():
        if cell not in selected:
            rows.append(dict(cell=cell, outcome="unselected (ignored)"))
            continue
        launcher = ["bash", str(args.cargo_wrapper.resolve())] if args.cargo_wrapper else ["cargo"]
        command = [*launcher, "test", "--locked", "-p", "server", "--test", target, f"-j{args.jobs}", "--", "--ignored", "--exact", cell, "--nocapture", "--test-threads=1"]
        log = output / f"{cell}.log"
        try:
            with log.open("wb") as stream:
                result = subprocess.run(command, cwd=repository, env=env, stdout=stream, stderr=subprocess.STDOUT, check=False)
            code = result.returncode
            # Cargo succeeding with no matching test is not an executed cell.
            text = log.read_text(errors="replace")
            passed = code == 0 and "test result: ok. 1 passed; 0 failed; 0 ignored;" in text
            row = dict(cell=cell, outcome="passed" if passed else "failed", exit_code=code, command=command, log=str(log))
        except OSError as error:
            log.write_text(str(error) + "\n")
            row = dict(cell=cell, outcome="failed", error=str(error), command=command, log=str(log))
        rows.append(row)
        print(f"{cell}: {row['outcome']} ({log})", flush=True)
        (output / "results.json").write_text(json.dumps(rows, indent=2) + "\n")
    (output / "results.json").write_text(json.dumps(rows, indent=2) + "\n")
    return int(any(row["outcome"] == "failed" for row in rows))


if __name__ == "__main__":
    sys.exit(main())
