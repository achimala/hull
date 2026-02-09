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
    #[arg(long, default_value = "crate::convex::ConvexValue")]
    rust_convex_value_path: String,
    #[arg(long, default_value = "crate::convex")]
    rust_api_client_module_path: String,
    #[arg(long, default_value = "crate::generated::convex_types")]
    rust_api_types_module_path: String,
    #[arg(long, default_value_t = false)]
    include_internal_functions: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    hull::typegen::run(hull::typegen::TypegenConfig {
        convex_dir: args.convex_dir,
        swift_out: args.swift_out,
        rust_out: args.rust_out,
        rust_api_out: args.rust_api_out,
        rust_convex_value_path: args.rust_convex_value_path,
        rust_api_client_module_path: args.rust_api_client_module_path,
        rust_api_types_module_path: args.rust_api_types_module_path,
        include_internal_functions: args.include_internal_functions,
    })
}
