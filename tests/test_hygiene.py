"""Small AST/text guards for the repository's safety boundaries."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
import re
from urllib.parse import urlsplit
import unittest


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
RULE_ENV = "live-credential-or-environment-read"
RULE_KIWOOM = "direct-kiwoom-live-mutation-endpoint"
RULE_RESEARCH = "research-broker-mutation-dependency"
RULE_LAYER = "absolute-import-layer-boundary"
RULE_SYNTAX = "python-syntax"

_ENV_MODULES = {"decouple", "dotenv", "dynaconf", "environs"}
_ENV_ATTRIBUTES = {"environ", "getenv", "putenv", "unsetenv"}
_ENV_FILE = re.compile(r"(?:^|[\\/])\.env(?:$|[\\/])")
_KIWOOM_HOST = re.compile(
    r"(?:https?://)?(?:api|mockapi|openapi)\.kiwoom\.com", re.IGNORECASE
)
_KIWOOM_PATHS = frozenset({"/oauth2/token", "/api/us/acnt"})
_KIWOOM_API_IDS = frozenset({"au10001", "ust21070", "ust21110", "ust21150"})
_KIWOOM_API_ID = re.compile(r"(?:au|ust)\d+", re.IGNORECASE)
_BROKER_METHODS = {
    "cancel",
    "cancel_order",
    "emergency_flatten",
    "flatten",
    "place_order",
    "reduce_only",
    "replace",
    "replace_order",
    "submit",
}

_FORBIDDEN_LAYERS = {
    "domain": {
        "trader.application",
        "trader.adapters",
        "trader.entrypoints",
        "trader.ports",
    },
    "application": {"trader.adapters", "trader.entrypoints"},
    "ports": {"trader.adapters", "trader.application", "trader.entrypoints"},
    "adapters": {"trader.application", "trader.entrypoints"},
}
_DOMAIN_EXTERNAL_IMPORTS = {"http", "openai", "os", "requests", "sqlite3", "urllib", "websocket"}


@dataclass(frozen=True)
class Violation:
    """One source hygiene violation with a useful location."""

    rule: str
    path: Path
    line: int
    detail: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.rule}: {self.detail}"


def python_files(root: Path = ROOT) -> tuple[Path, ...]:
    """Return only source and test Python files; documentation is out of scope."""
    paths: list[Path] = []
    for directory_name in ("src", "tests"):
        directory = root / directory_name
        if directory.exists():
            paths.extend(directory.rglob("*.py"))
    return tuple(sorted(paths))


def _package_for(path: Path, source_root: Path) -> str:
    parts = list(path.relative_to(source_root).with_suffix("").parts)
    if parts:
        parts.pop()
    return ".".join(parts)


def _module_for_import(node: ast.Import | ast.ImportFrom, package: str) -> str:
    if isinstance(node, ast.Import):
        return node.names[0].name
    module = node.module or ""
    if not node.level:
        return module
    package_parts = package.split(".") if package else []
    keep = max(0, len(package_parts) - node.level + 1)
    prefix = package_parts[:keep]
    return ".".join((*prefix, module))


def _modules_for_import(node: ast.Import | ast.ImportFrom, package: str) -> tuple[str, ...]:
    """Return the base and explicitly imported names for one import statement."""
    if isinstance(node, ast.Import):
        return tuple(alias.name for alias in node.names)
    base = _module_for_import(node, package)
    imported = [base]
    if base:
        imported.extend(f"{base}.{alias.name}" for alias in node.names if alias.name != "*")
    return tuple(imported)


def _dotted_name(node: ast.AST) -> tuple[str, ...]:
    if isinstance(node, ast.Name):
        return (node.id,)
    if isinstance(node, ast.Attribute):
        return (*_dotted_name(node.value), node.attr)
    return ()


def _target_names(node: ast.AST) -> tuple[str, ...]:
    if isinstance(node, ast.Name):
        return (node.id,)
    if isinstance(node, (ast.Tuple, ast.List)):
        names: list[str] = []
        for item in node.elts:
            names.extend(_target_names(item))
        return tuple(names)
    if isinstance(node, ast.Attribute):
        return (node.attr,)
    return ()


def _append(violations: list[Violation], violation: Violation) -> None:
    if violation not in violations:
        violations.append(violation)


def _environment_violations(tree: ast.AST, source: str, path: Path) -> list[Violation]:
    violations: list[Violation] = []
    os_names = {"os"}
    environment_names: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "os":
                    os_names.add(alias.asname or "os")
                if alias.name.split(".")[0] in _ENV_MODULES:
                    _append(
                        violations,
                        Violation(RULE_ENV, path, node.lineno, f"forbidden environment module {alias.name}"),
                    )
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module == "os":
                for alias in node.names:
                    bound_name = alias.asname or alias.name
                    if alias.name == "environ":
                        environment_names.add(bound_name)
                    if alias.name in _ENV_ATTRIBUTES:
                        _append(
                            violations,
                            Violation(
                                RULE_ENV,
                                path,
                                node.lineno,
                                f"forbidden os import {alias.name}",
                            ),
                        )
            elif module.split(".")[0] in _ENV_MODULES:
                _append(
                    violations,
                    Violation(RULE_ENV, path, node.lineno, f"forbidden environment module {module}"),
                )

    for node in ast.walk(tree):
        chain = _dotted_name(node)
        if len(chain) >= 2 and chain[0] in os_names and chain[1] in _ENV_ATTRIBUTES:
            _append(
                violations,
                Violation(RULE_ENV, path, node.lineno, ".".join(chain[:2])),
            )
        elif chain and chain[0] in environment_names:
            _append(
                violations,
                Violation(RULE_ENV, path, node.lineno, "environment mapping access"),
            )
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and _ENV_FILE.search(node.value):
            _append(
                violations,
                Violation(RULE_ENV, path, node.lineno, "direct .env file reference"),
            )

    return violations


def _constant_bindings(tree: ast.Module) -> dict[str, tuple[ast.AST, ast.AST]]:
    """Collect module-level names and their expressions for small static evaluation."""
    bindings: dict[str, tuple[ast.AST, ast.AST]] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        else:
            continue
        for target in targets:
            for name in _target_names(target):
                bindings[name] = (node.value, node)
    return bindings


def _constant_value(
    node: ast.AST,
    bindings: dict[str, tuple[ast.AST, ast.AST]],
    resolving: frozenset[str] = frozenset(),
) -> str | None:
    """Evaluate only string constants, name references, addition, and f-strings."""
    if isinstance(node, ast.Constant):
        return node.value if isinstance(node.value, str) else None
    if isinstance(node, ast.Name):
        if node.id in resolving or node.id not in bindings:
            return None
        expression, _ = bindings[node.id]
        return _constant_value(expression, bindings, resolving | {node.id})
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _constant_value(node.left, bindings, resolving)
        right = _constant_value(node.right, bindings, resolving)
        return left + right if left is not None and right is not None else None
    if isinstance(node, ast.JoinedStr):
        pieces: list[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                pieces.append(value.value)
            elif isinstance(value, ast.FormattedValue):
                formatted = _constant_value(value.value, bindings, resolving)
                if formatted is None:
                    return None
                pieces.append(formatted)
            else:
                return None
        return "".join(pieces)
    return None


def _kiwoom_path(value: str) -> str | None:
    if _KIWOOM_HOST.search(value):
        candidate = value if "://" in value else f"https://{value}"
        return urlsplit(candidate).path or None
    if value.startswith(("/api/us/", "/oauth2/")):
        return value.split("?", 1)[0]
    return None


def _kiwoom_violations(tree: ast.AST, source: str, path: Path) -> list[Violation]:
    """Reject unknown Kiwoom routes and IDs, including split/reordered constants."""
    violations: list[Violation] = []
    is_kiwoom_file = "kiwoom" in {part.casefold() for part in path.parts}
    module = tree if isinstance(tree, ast.Module) else ast.Module(body=[], type_ignores=[])
    bindings = _constant_bindings(module)

    def report(node: ast.AST, detail: str) -> None:
        _append(violations, Violation(RULE_KIWOOM, path, node.lineno, detail))

    def check_value(node: ast.AST, *, require_path: bool = False) -> str | None:
        value = _constant_value(node, bindings)
        if value is None:
            if require_path:
                report(node, "dynamic or unknown Kiwoom endpoint/API ID")
            return None
        route = _kiwoom_path(value)
        if route is not None:
            if route not in _KIWOOM_PATHS:
                report(node, f"Kiwoom endpoint is not allowlisted: {route}")
        elif require_path:
            report(node, "unknown Kiwoom endpoint/API ID")
        elif is_kiwoom_file and value.startswith("/") and value != "/":
            report(node, f"Kiwoom endpoint is not allowlisted: {value}")
        return value

    def check_api_id(node: ast.AST, *, required: bool = True) -> str | None:
        value = _constant_value(node, bindings)
        if value is None:
            if required:
                report(node, "dynamic or unknown Kiwoom API ID")
            return None
        if value and value.casefold() not in _KIWOOM_API_IDS:
            report(node, f"Kiwoom API ID is not allowlisted: {value}")
        return value

    for expression, assignment in bindings.values():
        value = _constant_value(expression, bindings)
        if value is not None:
            check_value(expression)
        for name in _target_names(assignment.targets[0] if isinstance(assignment, ast.Assign) else assignment.target):
            if "api" in name.casefold() and "id" in name.casefold() and (
                value is None or value.casefold() not in _KIWOOM_API_IDS
            ):
                report(assignment, f"Kiwoom API ID is not allowlisted: {value!r}")

    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            value = node.value
            route = _kiwoom_path(value)
            if route is not None and route not in _KIWOOM_PATHS:
                report(node, f"Kiwoom endpoint is not allowlisted: {route}")
            if _KIWOOM_API_ID.fullmatch(value) and value.casefold() not in _KIWOOM_API_IDS:
                report(node, f"Kiwoom API ID is not allowlisted: {value}")

        if isinstance(node, ast.Call):
            function_name = _dotted_name(node.func)
            method = function_name[-1].casefold() if function_name else ""
            if method in {"_send", "_pages"} and node.args:
                if method == "_send":
                    check_value(node.args[0], require_path=True)
                else:
                    check_api_id(node.args[0])

        if isinstance(node, ast.Dict):
            for key, value_node in zip(node.keys, node.values):
                if (
                    isinstance(key, ast.Constant)
                    and key.value == "api-id"
                ):
                    check_api_id(value_node, required=False)

        if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Subscript):
            target = node.targets[0]
            if (
                isinstance(target.slice, ast.Constant)
                and target.slice.value == "api-id"
            ):
                check_api_id(node.value, required=False)
    return violations


def _research_violations(
    tree: ast.AST, package: str, path: Path
) -> list[Violation]:
    if "research" not in {part.lower() for part in package.split(".") if part}:
        return []
    violations: list[Violation] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = {alias.name.lower() for alias in node.names}
            broker_name = any("broker" in name or name in _BROKER_METHODS for name in names)
            for imported_module in _modules_for_import(node, package):
                module = imported_module.lower()
                broker_module = ".broker" in f".{module}" or module.endswith("broker")
                mutation_module = module in {
                    "trader.application.execution",
                    "trader.adapters.kiwoom",
                }
                if broker_module or mutation_module or broker_name:
                    _append(
                        violations,
                        Violation(RULE_RESEARCH, path, node.lineno, f"research imports {module}"),
                    )
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr.lower() in _BROKER_METHODS:
                _append(
                    violations,
                    Violation(
                        RULE_RESEARCH,
                        path,
                        node.lineno,
                        f"research calls broker mutation {node.func.attr}",
                    ),
                )
    return violations


def _layer_violations(tree: ast.AST, package: str, path: Path, source_root: Path) -> list[Violation]:
    relative = path.relative_to(source_root)
    if len(relative.parts) < 2 or relative.parts[0] != "trader":
        return []
    layer = relative.parts[1]
    violations: list[Violation] = []
    forbidden = _FORBIDDEN_LAYERS.get(layer, set())
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Import, ast.ImportFrom)):
            continue
        for module in _modules_for_import(node, package):
            if any(module == prefix or module.startswith(f"{prefix}.") for prefix in forbidden):
                _append(
                    violations,
                    Violation(RULE_LAYER, path, node.lineno, f"{layer} imports {module}"),
                )
            if layer == "domain" and module.split(".")[0] in _DOMAIN_EXTERNAL_IMPORTS:
                _append(
                    violations,
                    Violation(RULE_LAYER, path, node.lineno, f"domain imports {module}"),
                )
            if layer == "application" and module.split(".")[0] == "sqlite3":
                _append(
                    violations,
                    Violation(RULE_LAYER, path, node.lineno, "application imports sqlite3"),
                )
    return violations


def scan_repository(root: Path = ROOT) -> tuple[Violation, ...]:
    """Scan only ``src`` and ``tests`` for the safety boundaries."""
    violations: list[Violation] = []
    source_root = root / "src"
    for path in python_files(root):
        source = path.read_text(encoding="utf-8")
        try:
            tree = ast.parse(source, filename=str(path))
        except SyntaxError as error:
            violations.append(Violation(RULE_SYNTAX, path, error.lineno or 1, str(error)))
            continue
        package = _package_for(path, source_root) if path.is_relative_to(source_root) else ""
        violations.extend(_environment_violations(tree, source, path))
        violations.extend(_research_violations(tree, package, path))
        if path.is_relative_to(source_root):
            violations.extend(_kiwoom_violations(tree, source, path))
            violations.extend(_layer_violations(tree, package, path, source_root))
    return tuple(sorted(set(violations), key=lambda item: (str(item.path), item.line, item.rule)))


class HygieneTests(unittest.TestCase):
    def assert_no_rule(self, rule: str) -> None:
        violations = [item for item in scan_repository() if item.rule == rule]
        self.assertEqual([], violations, "\n".join(map(str, violations)))

    def assert_kiwoom_rejected(self, source: str) -> None:
        path = Path("kiwoom_fixture.py")
        violations = _kiwoom_violations(ast.parse(source), source, path)
        self.assertTrue(violations, source)

    def test_source_and_tests_do_not_read_live_credentials_or_environment(self) -> None:
        self.assert_no_rule(RULE_ENV)

    def test_source_has_no_direct_kiwoom_live_mutation_endpoint(self) -> None:
        self.assert_no_rule(RULE_KIWOOM)

    def test_current_readonly_kiwoom_source_is_allowlisted(self) -> None:
        path = SRC / "trader" / "adapters" / "kiwoom" / "account.py"
        source = path.read_text(encoding="utf-8")
        self.assertEqual([], _kiwoom_violations(ast.parse(source), source, path))

    def test_kiwoom_endpoint_constant_propagation_rejects_split_reversed_and_joined(self) -> None:
        sources = (
            'HOST = "https://api.kiwoom.com"\nPATH = "/api/us/ordr"\nURL = HOST + PATH',
            'URL = HOST + PATH\nPATH = "/api/us/ordr"\nHOST = "https://api.kiwoom.com"',
            'HOST = "https://api.kiwoom.com"\nPATH = "/api/us/ordr"\nURL = f"{HOST}{PATH}"',
        )
        for source in sources:
            with self.subTest(source=source):
                self.assert_kiwoom_rejected(source)

    def test_kiwoom_mutation_and_unknown_api_ids_are_rejected(self) -> None:
        sources = (
            'PATH = "/api/us/ordr"',
            'API_ID = "ust20000"',
            'def request():\n    return _pages("unknown-api-id")',
        )
        for source in sources:
            with self.subTest(source=source):
                self.assert_kiwoom_rejected(source)

    def test_research_has_no_broker_mutation_dependency(self) -> None:
        self.assert_no_rule(RULE_RESEARCH)

    def test_source_layers_have_no_forbidden_absolute_imports(self) -> None:
        self.assert_no_rule(RULE_LAYER)

    def test_scan_scope_excludes_documentation_and_env_examples(self) -> None:
        scanned = python_files()
        self.assertTrue(scanned)
        self.assertNotIn(ROOT / "docs" / ".env.example", scanned)
        self.assertFalse(any("docs" in path.relative_to(ROOT).parts for path in scanned))


if __name__ == "__main__":
    unittest.main()
