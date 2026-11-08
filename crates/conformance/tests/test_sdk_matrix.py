"""Runner controls without Cargo, .NET or network prerequisites."""

import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("sdk_matrix", Path(__file__).parents[1] / "run-sdk-matrix.py")
matrix = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(matrix)


class RunnerControls(unittest.TestCase):
    def run_matrix(self, selected, failures=(), empty=()):
        called = []

        def run(command, **kwargs):
            cell = command[command.index("--exact") + 1]
            called.append(cell)
            code = int(cell in failures)
            count = 0 if cell in empty else 1
            kwargs["stdout"].write(f"test result: ok. {count} passed; 0 failed; 0 ignored;\n".encode())
            return subprocess.CompletedProcess(command, code)

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "evidence"
            argv = ["runner", "--output", str(output), "--jobs", "1"]
            for cell in selected:
                argv.extend(["--cell", cell])
            with patch("sys.argv", argv), patch.object(matrix.subprocess, "run", side_effect=run):
                code = matrix.main()
            return code, called, json.loads((output / "results.json").read_text())

    def test_all_eight_run_even_when_first_middle_and_last_fail(self):
        cells = list(matrix.CELLS)
        failures = (cells[0], cells[3], cells[-1])
        code, called, rows = self.run_matrix([], failures)
        self.assertEqual(code, 1)
        self.assertEqual(called, cells)
        self.assertEqual([row["cell"] for row in rows if row["outcome"] == "failed"], list(failures))

    def test_selected_cell_only_leaves_other_gates_ignored(self):
        selected = ["previous_fjall_wss"]
        code, called, rows = self.run_matrix(selected)
        self.assertEqual((code, called), (0, selected))
        self.assertEqual(sum(row["outcome"] == "unselected (ignored)" for row in rows), 7)

    def test_no_matching_gate_is_not_success(self):
        selected = ["declared_current_memory_tcp"]
        code, _, rows = self.run_matrix(selected, empty=selected)
        self.assertEqual(code, 1)
        self.assertEqual(rows[0]["outcome"], "failed")

    def test_missing_cargo_fails_every_selected_cell(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "evidence"
            with patch("sys.argv", ["runner", "--output", str(output)]), patch.object(
                matrix.subprocess, "run", side_effect=FileNotFoundError("missing Cargo")
            ):
                self.assertEqual(matrix.main(), 1)
            rows = json.loads((output / "results.json").read_text())
            self.assertEqual(len(rows), 8)
            self.assertTrue(all(row["outcome"] == "failed" for row in rows))

    def test_deliberately_delayed_probe_cannot_be_recorded_as_qualification(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "evidence"
            with patch("sys.argv", ["runner", "--output", str(output)]), patch.dict(
                matrix.os.environ, {"SWITCHYARD_SDK_TEST_TIMER_LAG_MS": "3500"}
            ), patch.object(matrix.subprocess, "run") as run:
                with self.assertRaises(SystemExit) as error:
                    matrix.main()
                self.assertEqual(error.exception.code, 2)
                run.assert_not_called()
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
