# Fixture Tests

Each fixture case should have this structure:

```text
tests/fixtures/<case>/
  convex/
    schema.ts
    validators.ts
    ...function modules...
  return_types.json
  expected/
    swift/
      ConvexSupport.swift
      ConvexTypes.swift
      ConvexAPI.swift
    rust/
      convex_types.rs
      convex_api.rs
```

How to run fixture tests:

```bash
cargo test fixture_golden
```

How to refresh expected snapshots after intentional generator changes:

```bash
HULL_UPDATE_FIXTURES=1 cargo test fixture_golden
```
