# Contributing

Thanks for helping improve Hull.

## Local setup

1. Install Rust stable.
2. Install Node.js 20+.
3. Run checks:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings -D dead_code
cargo test
```

## Fixture tests

Fixture snapshots live under `tests/fixtures`.

- Run fixture tests:

```bash
cargo test fixture_golden
```

- Update snapshots after intentional generator changes:

```bash
HULL_UPDATE_FIXTURES=1 cargo test fixture_golden
```

## Pull request expectations

- Keep changes focused and small when possible.
- Add tests or fixtures for behavior changes.
- Keep Clippy clean with warnings denied.
- Update docs when behavior or flags change.
