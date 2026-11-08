import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock


TOOL = Path(__file__).resolve().parents[1] / "bounded_test.py"
spec = importlib.util.spec_from_file_location("bounded_test", TOOL)
bounded_test = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bounded_test)


class ArtifactTests(unittest.TestCase):
    def test_exact_test_artifact_only(self):
        artifact = {
            "reason": "compiler-artifact",
            "target": {"name": "durable", "kind": ["test"]},
            "profile": {"test": True},
            "executable": sys.executable,
        }
        lines = [json.dumps({"reason": "build-finished", "success": True}),
                 json.dumps(artifact), json.dumps(artifact)]
        self.assertEqual(bounded_test.cargo_test_executable(lines, "durable"), sys.executable)
        for update in ({"profile": {"test": False}},
                       {"target": {"name": "durable", "kind": ["bin"]}},
                       {"executable": "relative-test"}):
            with self.subTest(update=update), self.assertRaises(ValueError):
                bounded_test.cargo_test_executable([json.dumps(artifact | update)], "durable")

    def test_missing_malformed_or_ambiguous_refuses(self):
        for lines in ([], ["not json"], [json.dumps({"reason": "build-finished"})]):
            with self.subTest(lines=lines), self.assertRaises(ValueError):
                bounded_test.cargo_test_executable(lines, "durable")
        artifact = {"reason": "compiler-artifact", "target": {"name": "durable", "kind": ["test"]},
                    "profile": {"test": True}, "executable": sys.executable}
        with self.assertRaises(ValueError):
            bounded_test.cargo_test_executable(
                [json.dumps(artifact), json.dumps(artifact | {"executable": "/bin/sh"})], "durable")


@unittest.skipUnless(os.name == "posix", "POSIX group ownership")
class ProcessTests(unittest.TestCase):
    def invoke(self, code, timeout=0.4):
        return subprocess.run(
            [sys.executable, str(TOOL), "--timeout", str(timeout), "--",
             sys.executable, "-c", code],
            capture_output=True, text=True, timeout=8,
        )

    def test_success_and_failure_keep_original_status(self):
        for status in (0, 7):
            with self.subTest(status=status):
                result = self.invoke(f"print('phase done', flush=True); raise SystemExit({status})", 2)
                self.assertEqual(result.returncode, status)
                self.assertIn("phase done", result.stdout)

    def test_stalled_operation_fails_with_phase_and_does_not_reopen(self):
        with tempfile.TemporaryDirectory() as temp:
            reopened = Path(temp) / "reopened"
            result = self.invoke(
                "import time; from pathlib import Path; "
                "print('PHASE blocked original send', flush=True); time.sleep(60); "
                f"Path({str(reopened)!r}).touch()"
            )
            self.assertEqual(result.returncode, 124)
            self.assertIn("PHASE blocked original send", result.stdout)
            self.assertIn("not successful drain", result.stderr)
            self.assertFalse(reopened.exists())

    def test_synchronous_destructor_is_inside_process_deadline(self):
        result = self.invoke(
            "import time\n"
            "class Owner:\n"
            " def __del__(self):\n"
            "  print('PHASE synchronous destructor', flush=True)\n"
            "  time.sleep(60)\n"
            "owner = Owner()\n"
            "del owner\n"
            "print('reopened')\n"
        )
        self.assertEqual(result.returncode, 124)
        self.assertIn("PHASE synchronous destructor", result.stdout)
        self.assertNotIn("reopened", result.stdout)

    def test_owned_descendant_cannot_continue_after_deadline(self):
        with tempfile.TemporaryDirectory() as temp:
            continued = Path(temp) / "continued"
            descendant = f"import time; from pathlib import Path; time.sleep(1); Path({str(continued)!r}).touch()"
            result = self.invoke(
                "import subprocess, sys, time; "
                f"subprocess.Popen([sys.executable, '-c', {descendant!r}]); "
                "print('PHASE descendant owned', flush=True); time.sleep(60)"
            )
            self.assertEqual(result.returncode, 124)
            time.sleep(1.1)
            self.assertFalse(continued.exists())

    def test_invalid_deadlines_do_not_launch(self):
        for timeout in (0, -1, float("inf"), float("nan")):
            with self.subTest(timeout=timeout):
                result = self.invoke("print('launched')", timeout)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn("launched", result.stdout)

    def test_diagnostics_run_before_original_is_killed_or_reaped(self):
        captured = []

        def capture(pid):
            os.kill(pid, 0)
            captured.append(pid)

        with mock.patch.object(bounded_test, "capture_stacks", side_effect=capture):
            self.assertEqual(bounded_test.run(
                [sys.executable, "-c", "import time; time.sleep(60)"], 0.1, stacks=True), 124)
        self.assertEqual(len(captured), 1)
        with self.assertRaises(ChildProcessError):
            os.waitpid(captured[0], os.WNOHANG)

    def test_diagnostic_failure_still_terminates_and_reaps_original(self):
        captured = []

        def fail(pid):
            captured.append(pid)
            raise RuntimeError("diagnostic failed")

        with mock.patch.object(bounded_test, "capture_stacks", side_effect=fail):
            with self.assertRaisesRegex(RuntimeError, "diagnostic failed"):
                bounded_test.run([sys.executable, "-c", "import time; time.sleep(60)"], 0.1, stacks=True)
        with self.assertRaises(ChildProcessError):
            os.waitpid(captured[0], os.WNOHANG)


if __name__ == "__main__":
    unittest.main()
