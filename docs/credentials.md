# Offline credential profiles

`trader.adapters.kiwoom.credentials` accepts only a path explicitly supplied by
the caller. It does not read environment variables, keyring entries, or the
network, and it never discovers a credential file.

The file has one strict schema:

```json
{
  "profiles": {
    "paper": {
      "environment": "MOCK",
      "account_number": "<account-number>",
      "app_key": "<app-key>",
      "secret_key": "<secret-key>"
    }
  }
}
```

Load a profile with an explicit expected environment:

```python
from trader.adapters.kiwoom.credentials import load_credential_profile
from trader.ports.account import AccountEnvironment

profile = load_credential_profile(
    credential_path,
    "paper",
    expected_environment=AccountEnvironment.MOCK,
)
public_profile = profile.to_account_profile()
```

The loader rejects duplicate JSON keys, unknown fields, malformed values,
oversized files, non-regular files, symlinks where the platform can identify
them, and group/world-readable POSIX files. All loader errors are generic. The
selected profile retains the real account number only inside the adapter
boundary; `to_account_profile()` passes the alias as `account_id`, never the
real account number.

## Token health

`TokenLease` is caller-created. Its issued and expiry times must be timezone
aware and are normalized to UTC. `TokenHealthGate` requires the expected profile
alias, environment, and a caller-selected minimum remaining duration. It rejects
expired or future-issued leases, clock rollback, invalid types, provenance
mismatches, and insufficient remaining time. `TokenHealthEvidence` exposes only
UTC timestamps, provenance, remaining duration, and the SHA-256 token
fingerprint; it never exposes the token itself.

When parsing Kiwoom-style `expires_dt` text, use
`TokenLease.from_expires_dt(..., expires_timezone=...)`. No timezone or official
token lifetime is assumed by the parser, and this slice does not issue or
refresh tokens.
