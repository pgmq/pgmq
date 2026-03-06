use anyhow::anyhow;
use std::path::{Path, PathBuf};
use std::str::FromStr;

const EXT_DIR: &str = "pgmq-extension";
const CONTROL_FILE: &str = "pgmq.control";
const SQL_DIR: &str = "sql";

pub fn main() -> anyhow::Result<()> {
    let ext_dir = PathBuf::from_str(env!("CARGO_MANIFEST_DIR"))?
        .parent()
        .ok_or_else(|| anyhow::anyhow!("Unable to open crate's parent directory"))?
        .join(EXT_DIR);

    let out_dir = PathBuf::from_str(&std::env::var("OUT_DIR")?)?;

    copy_control_file(&ext_dir, &out_dir)?;
    copy_sql_files(&ext_dir, &out_dir)?;

    Ok(())
}

/// Copy the extension's control file to the output dir.
fn copy_control_file(ext_dir: &Path, out_dir: &Path) -> anyhow::Result<()> {
    let from = ext_dir.join(CONTROL_FILE);
    println!(
        "cargo:rerun-if-changed={}",
        from.to_str()
            .ok_or_else(|| anyhow!("Unable to convert path to str"))?
    );

    let to = out_dir.join(CONTROL_FILE);
    std::fs::copy(from, to)?;

    Ok(())
}

/// Copy the extension's SQL files to a sub-dir of the output dir.
fn copy_sql_files(ext_dir: &Path, out_dir: &Path) -> anyhow::Result<()> {
    let from = ext_dir.join(SQL_DIR);
    println!(
        "cargo:rerun-if-changed={}",
        from.to_str()
            .ok_or_else(|| anyhow!("Unable to convert path to str"))?
    );

    let to = out_dir.join(SQL_DIR);
    dircpy::CopyBuilder::new(from, to)
        .overwrite_if_newer(true)
        .with_include_filter(".sql")
        .run()?;

    Ok(())
}
