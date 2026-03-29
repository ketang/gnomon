use std::env;

use anyhow::{Context, Result};
use gnomon_tui::{SunburstBenchmarkOptions, run_sunburst_benchmark};

fn main() -> Result<()> {
    let options = parse_options()?;
    let report = run_sunburst_benchmark(options)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).context("serialize benchmark report")?
    );
    Ok(())
}

fn parse_options() -> Result<SunburstBenchmarkOptions> {
    let mut options = SunburstBenchmarkOptions::default();
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--iterations" => {
                let value = args.next().context("missing value for --iterations")?;
                options.iterations = value
                    .parse::<usize>()
                    .context("invalid integer for --iterations")?;
            }
            flag => anyhow::bail!("unsupported flag: {flag}"),
        }
    }

    Ok(options)
}
