from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol

from trader.domain.observations import AccountObservation


class AccountEnvironment(StrEnum):
    LIVE = "LIVE"
    MOCK = "MOCK"


@dataclass(frozen=True)
class AccountProfile:
    account_id: str
    environment: AccountEnvironment
    app_key: str = field(repr=False)
    secret_key: str = field(repr=False)

    def __post_init__(self) -> None:
        for name in ("account_id", "app_key", "secret_key"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        if type(self.environment) is not AccountEnvironment:
            raise ValueError("environment must be AccountEnvironment")


class ReadonlyAccount(Protocol):
    def observe_account(
        self,
        *,
        timeout_seconds: float,
        page_cap: int,
        max_component_skew_seconds: float,
    ) -> AccountObservation: ...
