use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "hull")]
struct Args {
    #[arg(long)]
    convex_dir: PathBuf,
    #[arg(long)]
    swift_out: PathBuf,
    #[arg(long)]
    rust_out: PathBuf,
    #[arg(long)]
    rust_api_out: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    hull::typegen::run(hull::typegen::TypegenConfig {
        convex_dir: args.convex_dir,
        swift_out: args.swift_out,
        rust_out: args.rust_out,
        rust_api_out: args.rust_api_out,
    })
}
