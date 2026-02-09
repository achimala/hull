use super::*;
use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn fixture_golden() -> Result<()> {
    let mut cases = Vec::new();
    for entry in fs::read_dir(fixtures_root()).context("failed to read fixtures directory")? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            cases.push(path);
        }
    }
    cases.sort();

    if cases.is_empty() {
        return Err(anyhow!("no fixture cases found under tests/fixtures"));
    }

    let update = should_update_fixtures();
    for case_dir in cases {
        run_fixture_case(&case_dir, update)?;
    }
    Ok(())
}

#[test]
fn pascal_case_splits_module_path_delimiters() {
    assert_eq!(pascal_case("admin/users.getById"), "AdminUsersGetByID");
}

fn run_fixture_case(case_dir: &Path, update: bool) -> Result<()> {
    let case_name = case_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("invalid fixture case directory: {:?}", case_dir))?;
    let convex_dir = case_dir.join("convex");
    let return_types_path = case_dir.join("return_types.json");
    let expected_dir = case_dir.join("expected");

    if !convex_dir.is_dir() {
        return Err(anyhow!(
            "fixture case '{}' is missing convex/ directory",
            case_name
        ));
    }
    if !return_types_path.is_file() {
        return Err(anyhow!(
            "fixture case '{}' is missing return_types.json",
            case_name
        ));
    }

    let run_dir = fixture_run_dir(case_name)?;
    fs::create_dir_all(&run_dir).with_context(|| format!("failed to create {:?}", run_dir))?;

    let config = TypegenConfig {
        convex_dir: convex_dir.clone(),
        swift_out: run_dir.join("swift"),
        rust_out: run_dir.join("convex_types.rs"),
        rust_api_out: run_dir.join("convex_api.rs"),
        rust_convex_value_path: "crate::convex::ConvexValue".to_string(),
        rust_api_client_module_path: "crate::convex".to_string(),
        rust_api_types_module_path: "crate::generated::convex_types".to_string(),
        include_internal_functions: false,
    };

    run_with_return_type_extractor(config, |_, table_key_map| {
        load_fixture_return_types(&return_types_path, table_key_map)
    })
    .with_context(|| format!("fixture '{}' failed to generate output", case_name))?;

    sync_or_assert(
        update,
        &run_dir.join("swift").join("ConvexSupport.swift"),
        &expected_dir.join("swift").join("ConvexSupport.swift"),
        case_name,
    )?;
    sync_or_assert(
        update,
        &run_dir.join("swift").join("ConvexTypes.swift"),
        &expected_dir.join("swift").join("ConvexTypes.swift"),
        case_name,
    )?;
    sync_or_assert(
        update,
        &run_dir.join("swift").join("ConvexAPI.swift"),
        &expected_dir.join("swift").join("ConvexAPI.swift"),
        case_name,
    )?;
    sync_or_assert(
        update,
        &run_dir.join("convex_types.rs"),
        &expected_dir.join("rust").join("convex_types.rs"),
        case_name,
    )?;
    sync_or_assert(
        update,
        &run_dir.join("convex_api.rs"),
        &expected_dir.join("rust").join("convex_api.rs"),
        case_name,
    )?;

    let _ = fs::remove_dir_all(&run_dir);
    Ok(())
}

fn load_fixture_return_types(
    path: &Path,
    table_key_map: &HashMap<String, String>,
) -> Result<HashMap<String, TypeExpr>> {
    let json = fs::read_to_string(path).with_context(|| format!("failed to read {:?}", path))?;
    let reprs: HashMap<String, ReturnTypeRepr> =
        serde_json::from_str(&json).with_context(|| format!("invalid JSON in {:?}", path))?;
    let mut out = HashMap::new();
    for (key, repr) in reprs {
        out.insert(key, return_type_repr_to_type_expr(&repr, table_key_map)?);
    }
    Ok(out)
}

fn sync_or_assert(update: bool, generated: &Path, expected: &Path, case_name: &str) -> Result<()> {
    let generated_contents =
        fs::read_to_string(generated).with_context(|| format!("failed to read {:?}", generated))?;

    if update {
        if let Some(parent) = expected.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create fixture dir {:?}", parent))?;
        }
        fs::write(expected, generated_contents)
            .with_context(|| format!("failed to write fixture snapshot {:?}", expected))?;
        return Ok(());
    }

    let expected_contents = fs::read_to_string(expected).with_context(|| {
        format!(
            "missing expected snapshot {:?} for fixture '{}'. Re-run with HULL_UPDATE_FIXTURES=1",
            expected, case_name
        )
    })?;

    if expected_contents != generated_contents {
        return Err(anyhow!(
            "fixture '{}' mismatch in {:?}. Re-run with HULL_UPDATE_FIXTURES=1 to refresh snapshots",
            case_name,
            expected
        ));
    }

    Ok(())
}

fn fixtures_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn fixture_run_dir(case_name: &str) -> Result<PathBuf> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time is before UNIX_EPOCH")?
        .as_nanos();
    Ok(std::env::temp_dir().join(format!(
        "hull_fixture_{}_{}_{}",
        case_name,
        std::process::id(),
        timestamp
    )))
}

fn should_update_fixtures() -> bool {
    std::env::var("HULL_UPDATE_FIXTURES")
        .map(|value| {
            let value = value.trim().to_ascii_lowercase();
            value == "1" || value == "true" || value == "yes"
        })
        .unwrap_or(false)
}
