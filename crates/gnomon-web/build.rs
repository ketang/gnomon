use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let fallback_dir = manifest_dir.join("fallback");
    let ui_dist_dir = manifest_dir.join("ui").join("dist");
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("out dir"));

    println!("cargo:rerun-if-changed={}", fallback_dir.display());
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("ui").display()
    );

    let assets_dir = out_dir.join("embedded_ui");
    fs::create_dir_all(&assets_dir).expect("create embedded ui dir");

    let using_built_assets = copy_asset_set(&ui_dist_dir, &assets_dir).unwrap_or(false);
    if !using_built_assets {
        copy_asset_set(&fallback_dir, &assets_dir).expect("copy fallback ui assets");
    }

    let generated = out_dir.join("embedded_assets.rs");
    let generated_source = format!(
        concat!(
            "pub const INDEX_HTML: &str = include_str!(r#\"{}\"#);\n",
            "pub const APP_JS: &str = include_str!(r#\"{}\"#);\n",
            "pub const APP_CSS: &str = include_str!(r#\"{}\"#);\n",
            "pub const USING_BUILT_ASSETS: bool = {};\n"
        ),
        assets_dir.join("index.html").display(),
        assets_dir.join("app.js").display(),
        assets_dir.join("app.css").display(),
        using_built_assets
    );
    fs::write(generated, generated_source).expect("write embedded assets module");
}

fn copy_asset_set(source_dir: &Path, target_dir: &Path) -> Result<bool, std::io::Error> {
    let index_html = source_dir.join("index.html");
    let app_js = source_dir.join("app.js");
    let app_css = source_dir.join("app.css");

    if !(index_html.exists() && app_js.exists() && app_css.exists()) {
        return Ok(false);
    }

    fs::copy(index_html, target_dir.join("index.html"))?;
    fs::copy(app_js, target_dir.join("app.js"))?;
    fs::copy(app_css, target_dir.join("app.css"))?;
    Ok(true)
}
