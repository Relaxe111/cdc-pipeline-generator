# Runtime Command Stats

This folder stores runtime command usage counters per git user.

## Files

- `_docs/_stats/{git-user}.txt` — per-user counters (tab-separated `command<TAB>count`)
- `_docs/_stats/stats.md` — aggregated report (machine + human readable)

## How counting works

- On each `cdc ...` invocation, the CLI increments one normalized command key.
- Command normalization is order-insensitive for flags/values.
- User key is derived from git config:
  - `user.email` local part (preferred), else
  - `user.name`

## Generate aggregated report

```bash
cdc generate-usage-stats
```

This reads all `_docs/_stats/*.txt` files and rebuilds `_docs/_stats/stats.md`.
