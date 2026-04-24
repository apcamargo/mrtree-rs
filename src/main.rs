mod cli;
mod logging;

use std::fs::File;
use std::io::{self, IsTerminal, Read, Write};

use anyhow::Context;
use clap::{CommandFactory, Parser};

use cli::Cli;

fn main() {
    let cli = Cli::parse();

    if cli.input == "-" && io::stdin().is_terminal() {
        Cli::command().print_help().expect("help text should print");
        std::process::exit(0);
    }

    if let Err(error) = logging::init(cli.runtime.verbose) {
        eprintln!("Error: failed to initialize logging: {error}");
        std::process::exit(1);
    }

    if let Err(error) = run_cli(&cli) {
        tracing::error!("{error:#}");
        std::process::exit(1);
    }
}

fn run_cli(cli: &Cli) -> anyhow::Result<()> {
    let output_target = if cli.output == "-" {
        "stdout"
    } else {
        cli.output.as_str()
    };
    let input_reader: Box<dyn Read> = if cli.input == "-" {
        Box::new(io::stdin())
    } else {
        Box::new(File::open(&cli.input).with_context(|| format!("Unable to open {}", cli.input))?)
    };
    let input =
        mrtree::io::read_tsv(input_reader, cli.header).context("Failed to read input table")?;
    let result = mrtree::reconcile_input(
        input,
        &mrtree::RunOptions {
            preprocess: mrtree::RunPreprocessOptions {
                max_k: cli.preprocess.max_k,
                consensus: cli.preprocess.consensus,
            },
            scoring: mrtree::RunScoringOptions {
                sample_weighting: cli.scoring.sample_weighting,
                augment_path: cli.scoring.augment_path,
            },
            runtime: mrtree::RunRuntimeOptions {
                seed: cli.runtime.seed,
                threads: cli.runtime.threads,
            },
        },
    )?;

    let output_writer: Box<dyn Write> = if cli.output == "-" {
        Box::new(io::stdout())
    } else {
        Box::new(
            File::create(&cli.output)
                .with_context(|| format!("Unable to create {}", cli.output))?,
        )
    };
    mrtree::io::write_tsv(output_writer, cli.header, &result.effective, &result.paths)
        .context("Failed to write output")?;
    tracing::info!(output = %output_target, "Finished writing output");
    Ok(())
}
