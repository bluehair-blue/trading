"""Adversarial tests for the offline credential and token boundary."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import tempfile
import unittest

from trader.adapters.kiwoom.credentials import (
    MAX_CREDENTIAL_FILE_BYTES,
    CredentialError,
    TokenHealthError,
    TokenHealthGate,
    TokenLease,
    TokenLeaseError,
    load_credential_profile,
)
from trader.ports.account import AccountEnvironment


UTC = timezone.utc
NOW = datetime(2026, 8, 27, 12, 0, tzinfo=UTC)


def document() -> dict[str, object]:
    return {
        "profiles": {
            "paper": {
                "environment": "MOCK",
                "account_number": "1234567890",
                "app_key": "app-key-secret",
                "secret_key": "secret-key-secret",
            },
            "live": {
                "environment": "LIVE",
                "account_number": "0987654321",
                "app_key": "live-app-key",
                "secret_key": "live-secret-key",
            },
        }
    }


class CredentialLoaderTests(unittest.TestCase):
    def write(self, value: str | bytes) -> Path:
        handle = tempfile.NamedTemporaryFile(delete=False)
        try:
            path = Path(handle.name)
            handle.write(value.encode() if isinstance(value, str) else value)
            handle.close()
            if os.name == "posix":
                path.chmod(0o600)
            return path
        except BaseException:
            handle.close()
            raise

    def tearDown(self) -> None:
        for path in getattr(self, "paths", ()):
            path.unlink(missing_ok=True)

    def setUp(self) -> None:
        self.paths: list[Path] = []

    def saved(self, value: str | bytes) -> Path:
        path = self.write(value)
        self.paths.append(path)
        return path

    def load(self, path: Path, **changes: object):
        return load_credential_profile(
            path,
            "paper",
            expected_environment=changes.pop("expected_environment", AccountEnvironment.MOCK),
            **changes,
        )

    def test_explicit_profile_maps_only_alias_to_public_account_profile(self) -> None:
        profile = self.load(self.saved(json.dumps(document())))
        public = profile.to_account_profile()

        self.assertEqual(profile.account_number, "1234567890")
        self.assertEqual(public.account_id, "paper")
        self.assertEqual(public.environment, AccountEnvironment.MOCK)
        self.assertNotIn("1234567890", repr(public))
        self.assertNotIn("app-key-secret", repr(public))
        self.assertNotIn("secret-key-secret", repr(public))
        self.assertNotIn("1234567890", repr(profile))
        self.assertNotIn("app-key-secret", repr(profile))
        self.assertNotIn("secret-key-secret", repr(profile))

    def test_environment_mismatch_is_generic_and_fail_closed(self) -> None:
        with self.assertRaises(CredentialError) as raised:
            self.load(self.saved(json.dumps(document())), expected_environment=AccountEnvironment.LIVE)
        self.assertEqual(str(raised.exception), "credential profile unavailable")
        self.assertNotIn("1234567890", str(raised.exception))
        self.assertNotIn("secret-key-secret", str(raised.exception))

    def test_duplicate_and_unknown_schema_are_rejected(self) -> None:
        duplicate = b'{"profiles":{},"profiles":{}}'
        with self.assertRaises(CredentialError):
            self.load(self.saved(duplicate))

        malformed = document()
        malformed["unexpected"] = True
        with self.assertRaises(CredentialError):
            self.load(self.saved(json.dumps(malformed)))

        malformed_profile = document()
        profile = malformed_profile["profiles"]["paper"]  # type: ignore[index]
        profile["unexpected"] = True  # type: ignore[index]
        with self.assertRaises(CredentialError):
            self.load(self.saved(json.dumps(malformed_profile)))

    def test_oversize_nonregular_and_insecure_files_are_rejected(self) -> None:
        with self.assertRaises(CredentialError):
            self.load(self.saved(b"x" * (MAX_CREDENTIAL_FILE_BYTES + 1)))

        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(CredentialError):
                self.load(Path(directory))

        if os.name == "posix":
            path = self.saved(json.dumps(document()))
            path.chmod(0o644)
            with self.assertRaises(CredentialError):
                self.load(path)

            path.chmod(0o700)
            with self.assertRaises(CredentialError):
                self.load(path)

    @unittest.skipUnless(hasattr(os, "symlink"), "symlink is unavailable")
    def test_symlink_is_rejected_when_supported(self) -> None:
        target = self.saved(json.dumps(document()))
        link = target.with_name(target.name + "-link")
        try:
            link.symlink_to(target)
        except OSError:
            self.skipTest("creating symlinks is not permitted")
        self.paths.append(link)
        with self.assertRaises(CredentialError):
            self.load(link)


class TokenHealthTests(unittest.TestCase):
    def lease(self, **changes: object) -> TokenLease:
        values: dict[str, object] = {
            "token": "token-secret-value",
            "profile_alias": "paper",
            "environment": AccountEnvironment.MOCK,
            "issued_at": NOW,
            "expires_at": NOW + timedelta(hours=1),
        }
        values.update(changes)
        return TokenLease(**values)  # type: ignore[arg-type]

    def gate(self, **changes: object) -> TokenHealthGate:
        values: dict[str, object] = {
            "profile_alias": "paper",
            "environment": AccountEnvironment.MOCK,
            "minimum_remaining": timedelta(minutes=5),
        }
        values.update(changes)
        return TokenHealthGate(**values)  # type: ignore[arg-type]

    def test_health_evidence_has_fingerprint_only(self) -> None:
        lease = self.lease()
        evidence = self.gate().evaluate(lease, now=NOW + timedelta(minutes=1))

        self.assertTrue(evidence.healthy)
        self.assertEqual(len(evidence.token_fingerprint), 64)
        self.assertNotIn("token-secret-value", repr(lease))
        self.assertNotIn("token-secret-value", repr(evidence))

    def test_expired_future_and_minimum_remaining_fail_closed(self) -> None:
        cases = (
            (self.lease(expires_at=NOW + timedelta(minutes=1)), NOW, "INSUFFICIENT_REMAINING"),
            (self.lease(expires_at=NOW + timedelta(minutes=1)), NOW + timedelta(hours=1), "EXPIRED"),
            (self.lease(issued_at=NOW + timedelta(minutes=1)), NOW, "FUTURE_ISSUE"),
        )
        for lease, now, code in cases:
            with self.subTest(code):
                with self.assertRaises(TokenHealthError) as raised:
                    self.gate().evaluate(lease, now=now)
                self.assertEqual(raised.exception.code, code)

    def test_provenance_clock_rollback_and_type_fail_closed(self) -> None:
        with self.assertRaises(TokenHealthError) as raised:
            self.gate().evaluate(self.lease(profile_alias="other"), now=NOW)
        self.assertEqual(raised.exception.code, "PROVENANCE_MISMATCH")

        gate = self.gate()
        gate.evaluate(self.lease(), now=NOW + timedelta(minutes=1))
        with self.assertRaises(TokenHealthError) as raised:
            gate.evaluate(self.lease(), now=NOW)
        self.assertEqual(raised.exception.code, "CLOCK_ROLLBACK")

        with self.assertRaises(TokenHealthError) as raised:
            self.gate().evaluate(self.lease(), now=NOW.replace(tzinfo=None))
        self.assertEqual(raised.exception.code, "CLOCK_TYPE")

    def test_lease_requires_utc_aware_validity_and_explicit_expiry_timezone(self) -> None:
        with self.assertRaises(TokenLeaseError):
            self.lease(issued_at=NOW.replace(tzinfo=None))
        with self.assertRaises(TokenLeaseError):
            self.lease(expires_at=NOW)

        lease = TokenLease.from_expires_dt(
            token="token-secret-value",
            profile_alias="paper",
            environment=AccountEnvironment.MOCK,
            issued_at=NOW,
            expires_dt="20260827220000",
            expires_timezone=timezone(timedelta(hours=9)),
        )
        self.assertEqual(lease.expires_at, datetime(2026, 8, 27, 13, 0, tzinfo=UTC))


if __name__ == "__main__":
    unittest.main()
