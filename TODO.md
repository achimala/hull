# Hull Open Source TODO

This document is a practical cleanup and launch checklist for this repository.

## Current State Snapshot

- The crate is now named `hull` in `Cargo.toml`.
- The command is now `hull` in `src/main.rs`.
- The command now requires explicit `--convex-dir`, `--swift-out`, `--rust-out`, and `--rust-api-out` flags (no personal default paths).
- A `README.md` is now present.
- Dual license files are now present (`LICENSE-MIT` and `LICENSE-APACHE`).
- There is now a fixture based golden test harness with one baseline fixture case.
- The main logic is all in one very large file: `src/typegen/mod.rs` (about 3600 lines).
- The return type extractor JavaScript is now folded into the main source tree and embedded via `include_str!` (`src/typegen/extract_convex_function_return_types.mjs`).
- Function discovery now scans nested Convex folders and uses relative module paths.
- Build and tests now run locally (`cargo check`, `cargo test`).

## Priority 0: Define Project Shape

- [x] Decide the first supported targets for open source release.
  - Chosen: Swift and Rust.
- [x] Decide whether the JavaScript extractor stays as part of the design or gets replaced.
  - Chosen: fold into main project structure (done by embedding script and moving it into `src/typegen`).
- [x] Decide the minimum required toolchain versions.
  - Chosen: Rust stable, Node.js 20+, TypeScript 5+.

## Priority 1: Repository Basics

- [x] Rename package and command names to Hull.
  - `Cargo.toml` package name updated to `hull`.
  - Command name updated to `hull`.
  - Internal crate references updated.
- [x] Replace non portable default paths in `src/main.rs`.
  - Done: flags are now required with no baked in defaults.
- [x] Add a `README.md`.
  - Done with quick start and requirements.
- [x] Add a license file.
  - Done as dual license: MIT and Apache 2.0.
- [x] Add `.gitignore` and clean repository metadata files as needed.
- [ ] Update `Cargo.toml` metadata for open source.
  - description
  - license
  - repository
  - keywords
  - Status: description, license, readme, and keywords are done. repository is still pending.

## Priority 2: Make The Codebase Maintainable

- [ ] Break up `src/typegen/mod.rs` into focused modules.
  - parser
  - schema and function model
  - evaluator
  - Swift generator
  - Rust generator
  - naming helpers
- [ ] Define one internal intermediate model for schema and function types.
  - Language generators should read this model instead of mixing parse and emit logic together.
- [ ] Make external dependencies explicit and validated at runtime.
  - Clear error if `node` is missing.
  - Clear error if TypeScript cannot be loaded from the target Convex project.
- [ ] Remove project specific assumptions from generated code where possible.
  - Current generated Rust API assumes paths like `crate::convex` and `crate::generated`.
  - Make this configurable or generate more neutral code.
- [x] Improve function and module discovery.
  - Done: nested folders are now scanned.
  - Done: relative module path keys avoid basename collisions.

## Priority 3: Add A Real Test System (High Priority)

- [x] Create fixture based golden tests.
  - Suggested structure:
  - `tests/fixtures/<case>/convex/...`
  - `tests/fixtures/<case>/expected/swift/...`
  - `tests/fixtures/<case>/expected/rust/...`
  - `tests/fixtures/<case>/config.json` (optional per case options)
- [x] Add a test runner that:
  - Runs Hull on each fixture.
  - Compares generated files against expected files.
  - Shows a readable diff on failure.
  - Supports an explicit “update expected output” mode.
- [ ] Add unit tests for pure helper logic.
  - Name conversion.
  - Keyword escaping.
  - Optional and nullable normalization.
  - Union handling.
  - Status: name conversion coverage started (`pascal_case` delimiter behavior test).
- [ ] Add fixtures for Convex schema features and edge cases.
  - Primitive validators.
  - `v.literal`, `v.id`, arrays, records, unions.
  - optional and nullable combinations.
  - object spreads and reusable validators.
  - discriminated unions.
  - table names and singularization edge cases.
  - reserved words in field names.
  - internal and public queries, mutations, and actions.
  - unsupported patterns with expected clear error messages.
- [ ] Add fixtures for return type extraction behavior.
  - Promise and async handlers.
  - document and id return shapes.
  - nested object and union return types.
  - multiple files and nested directories.

## Priority 4: Documentation For Users And Contributors

- [ ] Write contributor guide.
  - How to run tests.
  - How to add a new fixture.
  - How to update expected outputs.
  - How to add a new target language generator.
- [ ] Document current limitations clearly.
  - Which Convex patterns are supported.
  - Which patterns are not supported yet.
- [ ] Add architecture notes.
  - Parse flow.
  - Type model flow.
  - Per language generation flow.

## Priority 5: Release And Quality Guardrails

- [ ] Add continuous integration checks.
  - format
  - lint
  - tests
  - fixture tests
- [ ] Add at least one smoke test project in the repository or in continuous integration.
  - Run Hull end to end against a small sample Convex project.
- [ ] Add release checklist.
  - version bump
  - changelog update
  - tag and release notes

## Suggested Order Of Execution

- [x] Step 1: Repository rename and basic documentation.
- [x] Step 2: Fixture test harness and first fixture set.
- [ ] Step 3: Module split and internal cleanup behind tests.
- [ ] Step 4: Continuous integration and release workflow.
