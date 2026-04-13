"""Tests for the script runner."""

import os
import tempfile

import pytest

from backend.core.script_runner import run_script, ScriptResult


class TestRunScript:
    """Test subprocess script execution."""

    def test_successful_script(self, tmp_path):
        script = '''
import argparse, json, os
parser = argparse.ArgumentParser()
parser.add_argument("--output-dir", required=True)
parser.add_argument("--row-count", type=int, default=10)
args = parser.parse_args()
os.makedirs(args.output_dir, exist_ok=True)
# Write a simple CSV
with open(os.path.join(args.output_dir, "test.csv"), "w") as f:
    f.write("id,name\\n")
    for i in range(args.row_count):
        f.write(f"{i},name_{i}\\n")
print(json.dumps({"status": "success", "tables": {"test": {"rows": args.row_count, "file": "test.csv"}}}))
'''
        output_dir = str(tmp_path / "output")
        result = run_script(script, output_dir, row_count=5)
        assert result.success is True
        assert result.error == ""
        assert result.tables_generated is not None
        assert "test" in result.tables_generated
        # Verify the CSV was created
        assert os.path.exists(os.path.join(output_dir, "test.csv"))

    def test_script_syntax_error(self, tmp_path):
        script = "def broken(\n"
        output_dir = str(tmp_path / "output")
        result = run_script(script, output_dir)
        assert result.success is False
        assert "SyntaxError" in result.error

    def test_script_runtime_error(self, tmp_path):
        script = '''
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--output-dir", required=True)
parser.add_argument("--row-count", type=int, default=10)
args = parser.parse_args()
raise ValueError("Something went wrong")
'''
        output_dir = str(tmp_path / "output")
        result = run_script(script, output_dir)
        assert result.success is False
        assert "ValueError" in result.error

    def test_script_timeout(self, tmp_path):
        script = '''
import argparse, time
parser = argparse.ArgumentParser()
parser.add_argument("--output-dir", required=True)
parser.add_argument("--row-count", type=int, default=10)
args = parser.parse_args()
time.sleep(60)
'''
        output_dir = str(tmp_path / "output")
        result = run_script(script, output_dir, timeout=2)
        assert result.success is False
        assert "timed out" in result.error.lower()

    def test_script_no_json_output(self, tmp_path):
        script = '''
import argparse, os
parser = argparse.ArgumentParser()
parser.add_argument("--output-dir", required=True)
parser.add_argument("--row-count", type=int, default=10)
args = parser.parse_args()
os.makedirs(args.output_dir, exist_ok=True)
print("done")
'''
        output_dir = str(tmp_path / "output")
        result = run_script(script, output_dir)
        assert result.success is True
        assert result.tables_generated is None  # No JSON status found

    def test_result_dataclass(self):
        r = ScriptResult(success=True, stdout="ok", error="")
        assert r.success is True
        assert r.tables_generated is None

    def test_row_count_passed(self, tmp_path):
        script = '''
import argparse, json, os
parser = argparse.ArgumentParser()
parser.add_argument("--output-dir", required=True)
parser.add_argument("--row-count", type=int, default=10)
args = parser.parse_args()
os.makedirs(args.output_dir, exist_ok=True)
print(json.dumps({"status": "success", "tables": {"t": {"rows": args.row_count}}}))
'''
        output_dir = str(tmp_path / "output")
        result = run_script(script, output_dir, row_count=42)
        assert result.success is True
        assert result.tables_generated["t"]["rows"] == 42
