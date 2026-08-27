"""Regression tests for the conservative repository secret scanner."""

from __future__ import annotations

from pathlib import Path
import subprocess
import tempfile
import unittest

from scripts.secret_scan import scan_repository, scan_text


class SecretScanTests(unittest.TestCase):
    def test_assignment_variants_and_concatenation_are_detected(self) -> None:
        cases = (
            (Path("settings.py"), 'APP_SECRET = "left" + "right#inside"  # comment\n'),
            (Path("settings.json"), '{"appSecret": "actual-value"}\n'),
            (Path("settings.toml"), 'secret_key = "actual-value"\n'),
            (Path("settings.yaml"), "app-secret: actual-value\n"),
        )
        for path, source in cases:
            with self.subTest(path=path):
                findings = scan_text(source, path)
                self.assertTrue(findings)
                self.assertIn("credential-assignment", {finding.kind for finding in findings})

    def test_case_insensitive_bearer_and_provider_literals_are_detected(self) -> None:
        bearer = scan_text('headers = {"Authorization": "bEaReR abc12345"}\n', Path("headers.py"))
        self.assertIn("bearer-credential", {finding.kind for finding in bearer})

        provider = scan_text("token = ghp_123456789012345678901234\n", Path("settings.env"))
        self.assertIn("high-risk-credential", {finding.kind for finding in provider})

    def test_private_key_and_url_credentials_are_detected(self) -> None:
        source = """-----BEGIN PRIVATE KEY-----
https://user:actual-password@example.test/api
"""
        findings = scan_text(source, Path("credentials.md"))
        self.assertEqual({"private-key", "high-risk-credential"}, {finding.kind for finding in findings})

    def test_placeholders_dynamic_values_and_account_numbers_are_allowed(self) -> None:
        source = """APP_SECRET = "<app-secret>"
APP_KEY = "${APP_KEY}"
TOKEN = "your-token"
Authorization = f"Bearer {token}"
ACCOUNT_NUMBER = "123456789012"
"""
        self.assertEqual((), scan_text(source, Path("example.py")))

    def test_git_scan_covers_tracked_ignored_paths_without_reading_local_secrets(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            (root / "src").mkdir()
            (root / "tests").mkdir()
            (root / "fixtures").mkdir()
            (root / "backups").mkdir()
            (root / "runtime").mkdir()
            (root / "cache").mkdir()
            (root / "generated").mkdir()
            (root / "scripts").mkdir()
            (root / "src" / "unsafe.py").write_text('APP_SECRET = "actual-value"\n', encoding="utf-8")
            (root / "tests" / "leak.py").write_text(
                'APP_SECRET = "pR7!mQ2#vL8@xN4$zK6"\n', encoding="utf-8"
            )
            (root / "fixtures" / "leak.env").write_text(
                "API_TOKEN=tok_live_7f3a9c2d1e8b4a6c\n", encoding="utf-8"
            )
            (root / "backups" / "leak.txt").write_text(
                "PASSWORD=backup-live-value-7f3a9c\n", encoding="utf-8"
            )
            (root / "runtime" / "leak.cfg").write_text(
                "SECRET=runtime-live-value-7f3a9c\n", encoding="utf-8"
            )
            (root / "fixtures" / "placeholders.env").write_text(
                'API_TOKEN="your-token"\nACCOUNT_NUMBER="123456789012"\n', encoding="utf-8"
            )
            (root / "cache" / "cache.env").write_text(
                'APP_SECRET="cache-live-value-7f3a9c"\n', encoding="utf-8"
            )
            (root / "generated" / "generated.env").write_text(
                'APP_SECRET="generated-live-value-7f3a9c"\n', encoding="utf-8"
            )
            (root / "binary.dat").write_bytes(
                b'APP_SECRET="binary-live-value-7f3a9c"\x00not text\n'
            )
            (root / "scripts" / "secret_scan.py").write_text(
                'APP_SECRET = "scanner-pattern"\n', encoding="utf-8"
            )
            (root / "uv.lock").write_text('APP_SECRET = "lock-value"\n', encoding="utf-8")
            (root / ".gitignore").write_text(
                "local.secret\nbackups/\nruntime/\ncache/\ngenerated/\n",
                encoding="utf-8",
            )
            (root / "local.secret").write_text(
                'APP_SECRET="local-ignored-value-7f3a9c"\n', encoding="utf-8"
            )
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            subprocess.run(
                [
                    "git",
                    "add",
                    "-f",
                    ".gitignore",
                    "src/unsafe.py",
                    "tests/leak.py",
                    "fixtures/leak.env",
                    "fixtures/placeholders.env",
                    "backups/leak.txt",
                    "runtime/leak.cfg",
                    "cache/cache.env",
                    "generated/generated.env",
                    "binary.dat",
                    "scripts/secret_scan.py",
                    "uv.lock",
                ],
                cwd=root,
                check=True,
            )

            findings = scan_repository(root)
            finding_paths = {finding.path.relative_to(root).as_posix() for finding in findings}
            self.assertEqual(
                {"src/unsafe.py", "tests/leak.py", "fixtures/leak.env", "backups/leak.txt", "runtime/leak.cfg"},
                finding_paths,
            )
            rendered = "\n".join(str(finding) for finding in findings)
            for secret in (
                "pR7!mQ2#vL8@xN4$zK6",
                "tok_live_7f3a9c2d1e8b4a6c",
                "backup-live-value-7f3a9c",
                "runtime-live-value-7f3a9c",
                "local-ignored-value-7f3a9c",
            ):
                self.assertNotIn(secret, rendered)


if __name__ == "__main__":
    unittest.main()
