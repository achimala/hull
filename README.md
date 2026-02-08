# Hull

Hull is a compiler that reads a Convex schema and Convex function definitions, then generates strongly typed client code.

Right now Hull generates:

- Swift types and API wrappers
- Rust types and API wrappers

The goal is to define schema and function types once in your Convex project, then generate code for each target language without drift.

## Requirements

- Rust stable toolchain
- Node.js 20 or newer
- A Convex project with:
  - `schema.ts`
  - `validators.ts`
  - function modules (`query`, `mutation`, `action`)
- TypeScript available from your Convex project root (`typescript` package)

## Quick Start

Run Hull with explicit input and output paths:

```bash
cargo run -- \
  --convex-dir ./convex \
  --swift-out ./generated/swift \
  --rust-out ./generated/convex_types.rs \
  --rust-api-out ./generated/convex_api.rs
```

This writes:

- `./generated/swift/ConvexSupport.swift`
- `./generated/swift/ConvexTypes.swift`
- `./generated/swift/ConvexAPI.swift`
- `./generated/convex_types.rs`
- `./generated/convex_api.rs`

## How It Works

- Hull parses `schema.ts` and `validators.ts` using a Rust TypeScript parser.
- Hull extracts function argument validators from exported Convex functions.
- Hull extracts function return types using the TypeScript checker through Node.js.
- Hull generates target language code from that combined type model.

## Project Status

This is in active cleanup for open source release. The current work list is in `TODO.md`.

## License

Hull is dual licensed under:

- MIT (`LICENSE-MIT`)
- Apache 2.0 (`LICENSE-APACHE`)

You can choose either license.
