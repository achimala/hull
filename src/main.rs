use anyhow::Result;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "convex_typegen")]
struct Args {
    #[arg(long, default_value = "../../convex/convex")]
    convex_dir: String,
    #[arg(long, default_value = "../../../Kasana/Kasana/Generated/Convex")]
    swift_out: String,
    #[arg(long, default_value = "../../src/generated/convex_types.rs")]
    rust_out: String,
    #[arg(long, default_value = "../../src/generated/convex_api.rs")]
    rust_api_out: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    convex_swift_typegen::typegen::run(convex_swift_typegen::typegen::TypegenConfig {
        convex_dir: args.convex_dir.into(),
        swift_out: args.swift_out.into(),
        rust_out: args.rust_out.into(),
        rust_api_out: args.rust_api_out.into(),
    })
}
