# Command Stats

Generate per-user CDC command counts from git-authored command lines and write `_docs/command-stats/stats.md`.

## Run

```bash
python _docs/command-stats/build_command_stats.py
```

## What it does

- Uses git author info to group counts by user (email local-part key, e.g. `igor.efrem`)
- Normalizes command signatures so flag/argument order does not matter
- Includes both machine-readable JSON and human-readable tables in `stats.md`
- Includes a `total` section with summed command counts across users

## Notes

- Counts are derived from git-added command lines, not shell runtime telemetry.
- Placeholder/template commands are filtered out to focus on concrete invocations.
