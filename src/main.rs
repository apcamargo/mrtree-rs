mod cli;

use std::fs::File;
use std::io::{self, IsTerminal, Read, Write};

use anyhow::Context;
use clap::{CommandFactory, Parser};

use cli::Cli;

fn main() {
    let cli = Cli::parse();

    if cli.io.input == "-" && io::stdin().is_terminal() {
        Cli::command().print_help().expect("help text should print");
        std::process::exit(0);
    }

    if let Err(error) = run_cli(&cli) {
        eprintln!("error: {error}");
        for cause in error.chain().skip(1) {
            eprintln!("caused by: {cause}");
        }
        std::process::exit(1);
    }
}

fn run_cli(cli: &Cli) -> anyhow::Result<()> {
    let input_reader: Box<dyn Read> = if cli.io.input == "-" {
        Box::new(io::stdin())
    } else {
        Box::new(
            File::open(&cli.io.input)
                .with_context(|| format!("unable to open {}", cli.io.input))?,
        )
    };
    let input =
        mrtree::io::read_tsv(input_reader, cli.io.header).context("failed to read input table")?;
    let result = mrtree::reconcile_input(
        input,
        &mrtree::RunOptions {
            preprocess: mrtree::RunPreprocessOptions {
                max_k: cli.preprocess.max_k,
                consensus: cli.preprocess.consensus,
            },
            scoring: mrtree::RunScoringOptions {
                sample_weighted: cli.scoring.sample_weighted,
                augment_path: cli.scoring.augment_path,
            },
            runtime: mrtree::RunRuntimeOptions {
                seed: cli.runtime.seed,
                threads: cli.runtime.threads,
                verbose: cli.runtime.verbose,
            },
        },
    )?;

    let output_writer: Box<dyn Write> = if cli.io.output == "-" {
        Box::new(io::stdout())
    } else {
        Box::new(
            File::create(&cli.io.output)
                .with_context(|| format!("unable to create {}", cli.io.output))?,
        )
    };
    mrtree::io::write_tsv(
        output_writer,
        cli.io.header,
        &result.effective,
        &result.paths,
    )
    .context("failed to write output")?;
    Ok(())
}
