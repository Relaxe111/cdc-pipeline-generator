from __future__ import annotations

import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
EXCLUDE_DIRS = {
    ".git",
    ".venv",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    "cdc_pipeline_generator.egg-info",
}

py_files: list[Path] = []
for path in ROOT.rglob("*.py"):
    if any(part in EXCLUDE_DIRS for part in path.parts):
        continue
    py_files.append(path)

records: list[dict[str, object]] = []
func_records: list[dict[str, object]] = []

for path in sorted(py_files):
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        text = path.read_text(encoding="latin-1")

    file_len = len(text.splitlines())
    rel = path.relative_to(ROOT).as_posix()
    records.append({"path": rel, "lines": file_len})

    try:
        tree = ast.parse(text)
    except SyntaxError:
        continue

    class Visitor(ast.NodeVisitor):
        def __init__(self) -> None:
            self.stack: list[str] = []

        def visit_ClassDef(self, node: ast.ClassDef) -> None:
            self.stack.append(node.name)
            self.generic_visit(node)
            self.stack.pop()

        def _record(self, node: ast.AST, name: str) -> None:
            start = getattr(node, "lineno", None)
            end = getattr(node, "end_lineno", None)
            if start is None or end is None:
                return
            qname = ".".join([*self.stack, name]) if self.stack else name
            func_records.append(
                {
                    "path": rel,
                    "name": qname,
                    "start": start,
                    "end": end,
                    "lines": end - start + 1,
                    "is_method": bool(self.stack),
                }
            )

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            self._record(node, node.name)
            self.generic_visit(node)

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            self._record(node, node.name)
            self.generic_visit(node)

    Visitor().visit(tree)

FILE_MAX = 600
FUNC_MAX = 100

files_over_max = [entry for entry in records if int(entry["lines"]) > FILE_MAX]
functions_over_max = [entry for entry in func_records if int(entry["lines"]) > FUNC_MAX]

summary = {
    "total_py_files": len(records),
    "total_functions_methods": len(func_records),
    "file_max": FILE_MAX,
    "func_max": FUNC_MAX,
    "files_over_max_count": len(files_over_max),
    "functions_over_max_count": len(functions_over_max),
    "top_30_largest_files": sorted(records, key=lambda item: int(item["lines"]), reverse=True)[:30],
    "top_60_largest_functions": sorted(func_records, key=lambda item: int(item["lines"]), reverse=True)[:60],
    "files_over_max": sorted(files_over_max, key=lambda item: int(item["lines"]), reverse=True),
    "functions_over_max": sorted(functions_over_max, key=lambda item: int(item["lines"]), reverse=True),
}

output_path = ROOT / "_docs" / "development" / "_stats" / "repo_py_size_audit.json"
output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

print(output_path.as_posix())
print(f"total_py_files={summary['total_py_files']}")
print(f"files_over_max_count={summary['files_over_max_count']}")
print(f"functions_over_max_count={summary['functions_over_max_count']}")
