"""Offline Kiwoom credential profiles and token health checks.

This module deliberately has no environment, keyring, filesystem discovery, or
network integration.  A caller must provide the credential file explicitly.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone, tzinfo
import hashlib
import json
import os
from pathlib import Path
import stat
from typing import Any

from trader.ports.account import AccountEnvironment, AccountProfile


MAX_CREDENTIAL_FILE_BYTES = 64 * 1024
_PROFILE_KEYS = frozenset({"environment", "account_number", "app_key", "secret_key"})
_ROOT_KEYS = frozenset({"profiles"})


class CredentialError(ValueError):
    """A credential file or profile failed closed without including secrets."""


class TokenLeaseError(ValueError):
    """A token lease is malformed or cannot be used safely."""


class TokenHealthError(ValueError):
    """A token failed the health/provenance gate."""

    def __init__(self, code: str) -> None:
        if type(code) is not str or not code or not code.isascii():
            code = "TOKEN_UNHEALTHY"
        self.code = code
        super().__init__(code)


def _valid_text(value: object, name: str, *, max_length: int = 256) -> str:
    if type(value) is not str:
        raise ValueError(f"{name} has invalid type")
    stripped = value.strip()
    if not stripped or len(value) > max_length or any(ord(char) < 32 for char in value):
        raise ValueError(f"{name} has invalid value")
    return value


def _environment(value: object) -> AccountEnvironment:
    if type(value) is AccountEnvironment:
        return value
    if type(value) is str:
        try:
            return AccountEnvironment(value)
        except ValueError:
            pass
    raise ValueError("environment has invalid value")


def _safe_error() -> CredentialError:
    return CredentialError("credential profile unavailable")


def _secure_file_mode(mode: int) -> bool:
    return os.name != "posix" or not mode & 0o177


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_nonfinite(value: str) -> None:
    del value
    raise ValueError("invalid number")


def _read_credential_bytes(path: str | os.PathLike[str], max_bytes: int) -> bytes:
    if type(max_bytes) is not int or not 0 < max_bytes <= MAX_CREDENTIAL_FILE_BYTES:
        raise _safe_error()
    try:
        candidate = Path(path)
        metadata = os.lstat(candidate)
    except (OSError, TypeError, ValueError):
        raise _safe_error() from None

    if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise _safe_error()
    if not _secure_file_mode(metadata.st_mode):
        raise _safe_error()
    if metadata.st_size > max_bytes:
        raise _safe_error()

    flags = os.O_RDONLY
    flags |= getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(candidate, flags)
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or stat.S_ISLNK(opened.st_mode):
            raise _safe_error()
        if not _secure_file_mode(opened.st_mode):
            raise _safe_error()
        if opened.st_size > max_bytes:
            raise _safe_error()
        data = os.read(descriptor, max_bytes + 1)
        if len(data) > max_bytes:
            raise _safe_error()
        return data
    except CredentialError:
        raise
    except (OSError, OverflowError):
        raise _safe_error() from None
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass


@dataclass(frozen=True)
class CredentialProfile:
    """A selected profile; the account number never crosses its public mapping."""

    profile_alias: str
    environment: AccountEnvironment
    account_number: str = field(repr=False, compare=False)
    app_key: str = field(repr=False, compare=False)
    secret_key: str = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        try:
            _valid_text(self.profile_alias, "profile alias", max_length=64)
            _environment(self.environment)
            _valid_text(self.account_number, "account number", max_length=128)
            _valid_text(self.app_key, "app key")
            _valid_text(self.secret_key, "secret key")
        except ValueError:
            raise CredentialError("credential profile unavailable") from None

    def to_account_profile(self) -> AccountProfile:
        """Convert to the public port using the non-sensitive alias only."""

        return AccountProfile(
            self.profile_alias, self.environment, self.app_key, self.secret_key
        )

    @property
    def alias(self) -> str:
        """Compatibility spelling for the non-sensitive profile alias."""

        return self.profile_alias


def load_credential_profile(
    path: str | os.PathLike[str],
    profile_alias: str,
    *,
    expected_environment: AccountEnvironment | str,
    max_bytes: int = MAX_CREDENTIAL_FILE_BYTES,
) -> CredentialProfile:
    """Load one explicitly selected, strictly shaped offline JSON profile."""

    try:
        alias = _valid_text(profile_alias, "profile alias", max_length=64)
        expected = _environment(expected_environment)
        raw = _read_credential_bytes(path, max_bytes)
        document = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_nonfinite,
        )
        if type(document) is not dict or set(document) != _ROOT_KEYS:
            raise ValueError("unknown credential schema")
        profiles = document["profiles"]
        if type(profiles) is not dict or not profiles:
            raise ValueError("invalid profiles")
        for candidate_alias, candidate in profiles.items():
            _valid_text(candidate_alias, "profile alias", max_length=64)
            if type(candidate) is not dict or set(candidate) != _PROFILE_KEYS:
                raise ValueError("unknown profile schema")
            _environment(candidate["environment"])
            _valid_text(candidate["account_number"], "account number", max_length=128)
            _valid_text(candidate["app_key"], "app key")
            _valid_text(candidate["secret_key"], "secret key")
        selected = profiles.get(alias)
        if selected is None:
            raise ValueError("profile not found")
        selected_environment = _environment(selected["environment"])
        if selected_environment is not expected:
            raise ValueError("profile environment mismatch")
        return CredentialProfile(
            alias,
            selected_environment,
            selected["account_number"],
            selected["app_key"],
            selected["secret_key"],
        )
    except CredentialError:
        raise
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        RecursionError,
        TypeError,
        ValueError,
        KeyError,
    ):
        raise _safe_error() from None


def load_profile(
    path: str | os.PathLike[str],
    profile_alias: str,
    *,
    expected_environment: AccountEnvironment | str,
    max_bytes: int = MAX_CREDENTIAL_FILE_BYTES,
) -> CredentialProfile:
    """Short alias for :func:`load_credential_profile`."""

    return load_credential_profile(
        path,
        profile_alias,
        expected_environment=expected_environment,
        max_bytes=max_bytes,
    )


def _utc(value: object) -> datetime:
    if type(value) is not datetime:
        raise TokenLeaseError("token lease invalid")
    try:
        offset = value.utcoffset()
        if value.tzinfo is None or offset is None:
            raise TokenLeaseError("token lease invalid")
        return value.astimezone(timezone.utc)
    except Exception:
        raise TokenLeaseError("token lease invalid") from None


@dataclass(frozen=True)
class TokenLease:
    """A caller-supplied token plus UTC validity and provenance metadata."""

    token: str = field(repr=False, compare=False)
    profile_alias: str
    environment: AccountEnvironment
    issued_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        if type(self.token) is not str or not self.token:
            raise TokenLeaseError("token lease invalid")
        try:
            _valid_text(self.profile_alias, "profile alias", max_length=64)
            _environment(self.environment)
        except ValueError:
            raise TokenLeaseError("token lease invalid") from None
        issued = _utc(self.issued_at)
        expires = _utc(self.expires_at)
        if expires <= issued:
            raise TokenLeaseError("token lease invalid")
        object.__setattr__(self, "issued_at", issued)
        object.__setattr__(self, "expires_at", expires)

    @property
    def token_fingerprint(self) -> str:
        return hashlib.sha256(self.token.encode("utf-8")).hexdigest()

    @classmethod
    def from_expires_dt(
        cls,
        *,
        token: str,
        profile_alias: str,
        environment: AccountEnvironment,
        issued_at: datetime,
        expires_dt: str,
        expires_timezone: tzinfo,
        date_format: str = "%Y%m%d%H%M%S",
    ) -> "TokenLease":
        """Parse a broker expiry only when the caller supplies its timezone."""

        if type(expires_dt) is not str or not isinstance(expires_timezone, tzinfo):
            raise TokenLeaseError("token lease invalid")
        try:
            parsed = datetime.strptime(expires_dt, date_format).replace(tzinfo=expires_timezone)
        except (TypeError, ValueError, OverflowError):
            raise TokenLeaseError("token lease invalid") from None
        return cls(token, profile_alias, environment, issued_at, parsed)


@dataclass(frozen=True)
class TokenHealthEvidence:
    """Safe health evidence; it contains a fingerprint, never the token."""

    healthy: bool
    profile_alias: str
    environment: AccountEnvironment
    issued_at: datetime
    expires_at: datetime
    remaining: timedelta
    token_fingerprint: str


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class TokenHealthGate:
    """Fail-closed token gate with explicit profile/environment provenance."""

    def __init__(
        self,
        *,
        profile_alias: str,
        environment: AccountEnvironment,
        minimum_remaining: timedelta,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        try:
            self._profile_alias = _valid_text(profile_alias, "profile alias", max_length=64)
            self._environment = _environment(environment)
        except ValueError:
            raise TokenHealthError("GATE_CONFIGURATION") from None
        if type(minimum_remaining) is not timedelta or minimum_remaining < timedelta(0):
            raise TokenHealthError("GATE_CONFIGURATION")
        if not callable(clock):
            raise TokenHealthError("GATE_CONFIGURATION")
        self._minimum_remaining = minimum_remaining
        self._clock = clock
        self._last_now: datetime | None = None

    def evaluate(
        self, lease: TokenLease, *, now: datetime | None = None
    ) -> TokenHealthEvidence:
        if type(lease) is not TokenLease:
            raise TokenHealthError("LEASE_TYPE")
        current = self._now(now)
        if self._last_now is not None and current < self._last_now:
            raise TokenHealthError("CLOCK_ROLLBACK")
        self._last_now = current
        if lease.profile_alias != self._profile_alias or lease.environment is not self._environment:
            raise TokenHealthError("PROVENANCE_MISMATCH")
        if lease.issued_at > current:
            raise TokenHealthError("FUTURE_ISSUE")
        remaining = lease.expires_at - current
        if remaining <= timedelta(0):
            raise TokenHealthError("EXPIRED")
        if remaining < self._minimum_remaining:
            raise TokenHealthError("INSUFFICIENT_REMAINING")
        return TokenHealthEvidence(
            True,
            lease.profile_alias,
            lease.environment,
            lease.issued_at,
            lease.expires_at,
            remaining,
            lease.token_fingerprint,
        )

    def check(
        self, lease: TokenLease, *, now: datetime | None = None
    ) -> TokenHealthEvidence:
        """Compatibility spelling for :meth:`evaluate`; both fail closed."""

        return self.evaluate(lease, now=now)

    def require_healthy(
        self, lease: TokenLease, *, now: datetime | None = None
    ) -> TokenHealthEvidence:
        return self.evaluate(lease, now=now)

    def validate(
        self, lease: TokenLease, *, now: datetime | None = None
    ) -> TokenHealthEvidence:
        """Compatibility spelling for callers that use validation terminology."""

        return self.evaluate(lease, now=now)

    def _now(self, supplied: datetime | None) -> datetime:
        try:
            value = self._clock() if supplied is None else supplied
            return _utc(value)
        except Exception:
            raise TokenHealthError("CLOCK_TYPE") from None


__all__ = [
    "CredentialError",
    "CredentialProfile",
    "MAX_CREDENTIAL_FILE_BYTES",
    "TokenHealthError",
    "TokenHealthEvidence",
    "TokenHealthGate",
    "TokenLease",
    "TokenLeaseError",
    "load_credential_profile",
    "load_profile",
]
