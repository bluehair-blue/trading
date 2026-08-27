"""Cross-platform account-scoped exclusive process lock."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import errno
import hashlib
import json
import os
from pathlib import Path
import sys
import threading
from collections.abc import Iterator
from typing import Self

if sys.platform == "win32":
    import msvcrt

    def _platform_lock(fd: int) -> None:
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)

    def _platform_unlock(fd: int) -> None:
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
else:
    import fcntl

    def _platform_lock(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _platform_unlock(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_UN)


class ProcessLockError(RuntimeError):
    """The process lock could not be acquired or used."""


class ProcessLockBusy(ProcessLockError):
    """Another process currently owns the account-scoped lock."""


class AccountProcessLock:
    """A non-blocking, exclusive lock for one internal account alias.

    The alias is used only as input to a SHA-256 filename derivation. It is
    never written to the lock file or used as a path component.
    """

    def __init__(
        self,
        runtime_db_path: str | os.PathLike[str],
        account_alias: str,
        deployment_version: str,
    ) -> None:
        if not isinstance(account_alias, str) or not account_alias.strip():
            raise ValueError("account_alias must be a non-empty string")
        if not isinstance(deployment_version, str) or not deployment_version.strip():
            raise ValueError("deployment_version must be a non-empty string")

        runtime_path = Path(runtime_db_path).resolve()
        self._runtime_identity = os.path.normcase(str(runtime_path))
        self.directory = runtime_path.parent
        digest = hashlib.sha256(account_alias.encode("utf-8")).hexdigest()
        self._account_digest = digest
        self.path = runtime_path.with_name(f".{runtime_path.name}.account-{digest}.lock")
        self.deployment_version = deployment_version
        self._fd: int | None = None
        self._mutex = threading.Lock()

    @property
    def acquired(self) -> bool:
        with self._mutex:
            return self._fd is not None

    def protects(self, account_alias: str) -> bool:
        if not isinstance(account_alias, str) or not account_alias.strip():
            return False
        digest = hashlib.sha256(account_alias.encode("utf-8")).hexdigest()
        return digest == self._account_digest

    def protects_runtime(self, runtime_identity: str) -> bool:
        if not isinstance(runtime_identity, str) or not runtime_identity.strip():
            return False
        normalized = os.path.normcase(str(Path(runtime_identity).resolve()))
        return normalized == self._runtime_identity

    @contextmanager
    def hold(self, account_alias: str) -> Iterator[None]:
        with self._mutex:
            if self._fd is None or not self.protects(account_alias):
                raise ProcessLockError("account process lock is not held for this alias")
            yield

    def acquire(self) -> None:
        with self._mutex:
            self._acquire()

    def _acquire(self) -> None:
        if self._fd is not None:
            raise ProcessLockError("process lock is already acquired")

        self.directory.mkdir(parents=True, exist_ok=True)
        flags = os.O_RDWR | os.O_CREAT
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOINHERIT", 0)
        try:
            fd = os.open(self.path, flags, 0o600)
        except OSError as error:
            raise ProcessLockError("could not open process lock") from error

        try:
            self._lock_fd(fd)
        except OSError as error:
            os.close(fd)
            if self._is_busy(error):
                raise ProcessLockBusy("account process lock is already held") from error
            raise ProcessLockError("could not acquire process lock") from error

        try:
            self._write_metadata(fd)
        except BaseException as error:
            try:
                self._unlock_fd(fd)
            finally:
                os.close(fd)
            raise ProcessLockError("could not persist process lock metadata") from error

        self._fd = fd

    def release(self) -> None:
        with self._mutex:
            self._release()

    def _release(self) -> None:
        fd = self._fd
        self._fd = None
        if fd is None:
            return
        try:
            self._unlock_fd(fd)
        finally:
            os.close(fd)

    def __enter__(self) -> Self:
        self.acquire()
        return self

    def __exit__(self, unused_type: object, unused_value: object, unused_traceback: object) -> None:
        self.release()

    def _write_metadata(self, fd: int) -> None:
        metadata = {
            "deployment_version": self.deployment_version,
            "pid": os.getpid(),
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        payload = json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        encoded = payload.encode("utf-8")
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        written = 0
        while written < len(encoded):
            written += os.write(fd, encoded[written:])
        os.fsync(fd)

    @staticmethod
    def _lock_fd(fd: int) -> None:
        _platform_lock(fd)

    @staticmethod
    def _unlock_fd(fd: int) -> None:
        _platform_unlock(fd)

    @staticmethod
    def _is_busy(error: OSError) -> bool:
        return error.errno in {errno.EACCES, errno.EAGAIN, errno.EBUSY, errno.EDEADLK} or getattr(
            error, "winerror", None
        ) in {33, 36}
