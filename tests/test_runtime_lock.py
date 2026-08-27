import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest

from trader.adapters.process_lock import (
    AccountProcessLock,
    ProcessLockBusy,
    ProcessLockError,
)


def _read_fd(fd: int) -> bytes:
    position = os.lseek(fd, 0, os.SEEK_CUR)
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        return os.read(fd, os.fstat(fd).st_size)
    finally:
        os.lseek(fd, position, os.SEEK_SET)


def _hold_lock_until_exit(runtime_path: str, alias: str, ready, terminate, snapshot) -> None:
    try:
        lock = AccountProcessLock(runtime_path, alias, "deploy-child")
        lock.acquire()
        snapshot.put(_read_fd(lock._fd))
        snapshot.close()
        snapshot.join_thread()
        ready.set()
        terminate.wait(10)
        os._exit(0)
    except BaseException:
        ready.set()
        os._exit(2)


def _try_lock_with_probe(runtime_path: str, alias: str, result) -> None:
    probe_called = False
    lock = AccountProcessLock(runtime_path, alias, "deploy-second")

    def probe() -> None:
        nonlocal probe_called
        probe_called = True

    try:
        lock.acquire()
        probe()
        result.put(("acquired", probe_called))
    except ProcessLockBusy:
        result.put(("busy", probe_called))
    finally:
        lock.release()


class AccountProcessLockTests(unittest.TestCase):
    def test_filename_is_hashed_and_metadata_excludes_alias(self):
        with tempfile.TemporaryDirectory() as temporary:
            alias = "real/account/12345678"
            runtime_path = Path(temporary) / "ledger.db"
            lock = AccountProcessLock(runtime_path, alias, "deploy-v1")
            expected = hashlib.sha256(alias.encode("utf-8")).hexdigest()
            self.assertEqual(
                lock.path, Path(temporary) / f".ledger.db.account-{expected}.lock"
            )
            self.assertNotIn(alias, lock.path.name)
            self.assertTrue(lock.protects(alias))
            self.assertFalse(lock.protects("other-account"))
            self.assertFalse(lock.protects(""))
            self.assertTrue(lock.protects_runtime(str(runtime_path)))
            self.assertFalse(lock.protects_runtime(str(Path(temporary) / "other.db")))
            self.assertNotIn(alias, vars(lock).values())

            with lock:
                metadata = json.loads(_read_fd(lock._fd))
                self.assertEqual(metadata["deployment_version"], "deploy-v1")
                self.assertEqual(metadata["pid"], os.getpid())
                self.assertNotIn(alias, metadata)
                self.assertTrue(metadata["started_at"].endswith("+00:00"))

            self.assertTrue(lock.path.exists())

    def test_second_process_fails_without_probe_or_file_mutation_and_exit_releases_lock(self):
        context = multiprocessing.get_context("spawn")
        with tempfile.TemporaryDirectory() as temporary:
            alias = "account-alias"
            runtime_path = str(Path(temporary) / "ledger.db")
            lock = AccountProcessLock(runtime_path, alias, "deploy-parent")
            ready = context.Event()
            terminate = context.Event()
            snapshot = context.Queue()
            holder = context.Process(
                target=_hold_lock_until_exit,
                args=(runtime_path, alias, ready, terminate, snapshot),
            )
            holder.start()
            contender = None
            try:
                self.assertTrue(ready.wait(10))
                self.assertEqual(holder.exitcode, None)

                lock_path = lock.path
                before = snapshot.get(timeout=2)
                result = context.Queue()
                contender = context.Process(
                    target=_try_lock_with_probe,
                    args=(runtime_path, alias, result),
                )
                contender.start()
                contender.join(10)
                self.assertEqual(contender.exitcode, 0)
                self.assertEqual(result.get(timeout=2), ("busy", False))
                self.assertTrue(lock_path.exists())

                terminate.set()
                holder.join(10)
                self.assertEqual(holder.exitcode, 0)
                self.assertEqual(lock_path.read_bytes(), before)

                with lock:
                    metadata = json.loads(_read_fd(lock._fd))
                    self.assertEqual(metadata["pid"], os.getpid())
                    self.assertEqual(metadata["deployment_version"], "deploy-parent")
                    self.assertTrue(metadata["started_at"].endswith("+00:00"))
            finally:
                if contender is not None:
                    contender.join(2)
                    if contender.is_alive():
                        contender.terminate()
                        contender.join(2)
                terminate.set()
                holder.join(2)
                if holder.is_alive():
                    holder.terminate()
                    holder.join(2)

    def test_release_is_idempotent_and_reacquisition_keeps_lock_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            lock = AccountProcessLock(Path(temporary) / "ledger.db", "alias", "deploy-v1")
            lock.release()
            lock.acquire()
            first = json.loads(_read_fd(lock._fd))
            lock.release()
            lock.release()
            self.assertTrue(lock.path.exists())
            lock.acquire()
            self.assertTrue(lock.path.exists())
            second = json.loads(_read_fd(lock._fd))
            self.assertEqual(second["pid"], os.getpid())
            self.assertEqual(second["deployment_version"], "deploy-v1")
            self.assertNotEqual(second["started_at"], first["started_at"])
            lock.release()

    def test_hold_blocks_release_and_second_owner_until_mutation_finishes(self):
        with tempfile.TemporaryDirectory() as temporary:
            alias = "account-alias"
            runtime_path = Path(temporary) / "ledger.db"
            lock = AccountProcessLock(runtime_path, alias, "deploy-v1")
            contender = AccountProcessLock(runtime_path, alias, "deploy-v2")
            lock.acquire()
            holding = threading.Event()
            finish = threading.Event()
            released = threading.Event()

            def mutation():
                with lock.hold(alias):
                    holding.set()
                    finish.wait(5)

            def release():
                lock.release()
                released.set()

            worker = threading.Thread(target=mutation)
            worker.start()
            self.assertTrue(holding.wait(2))
            releaser = threading.Thread(target=release)
            releaser.start()
            time.sleep(0.05)
            self.assertFalse(released.is_set())
            with self.assertRaises(ProcessLockBusy):
                contender.acquire()
            finish.set()
            worker.join(2)
            releaser.join(2)
            self.assertTrue(released.is_set())
            contender.acquire()
            contender.release()

    def test_invalid_configuration_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(ValueError):
                AccountProcessLock(Path(temporary) / "ledger.db", "", "deploy-v1")
            with self.assertRaises(ValueError):
                AccountProcessLock(Path(temporary) / "ledger.db", "alias", "")
            lock = AccountProcessLock(
                Path(temporary) / "ledger.db", "alias", "deploy-v1"
            )
            lock.acquire()
            with self.assertRaises(ProcessLockError):
                lock.acquire()
            lock.release()


if __name__ == "__main__":
    unittest.main()
