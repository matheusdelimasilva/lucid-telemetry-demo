//! The job binary's command line (`stream-rs/jobs/README.md`). A job's `main`
//! is `common::cli::main::<MyProcessor>(MY_SPEC, encode)`.
//!
//! ```text
//! <job> --out DIR --fixture <job>=<path>     fixture mode
//! <job> --out DIR <suite-dir>...             suite mode (examples/probes)
//! <job> --kafka                              consume/produce (config from env)
//! ```

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{bail, Context, Result};

use crate::artifacts::{case_dirs, read_json, write_case_artifacts, write_fixture_artifacts};
use crate::config::Config;
use crate::job::JobSpec;
use crate::processor::Processor;
use crate::record::JsonRecord;
use crate::replay::run_fixture;

pub type Encode<O> = fn(&O) -> (String, Vec<u8>);

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Args {
    pub out: Option<PathBuf>,
    pub fixtures: Vec<(String, PathBuf)>,
    pub suites: Vec<PathBuf>,
    pub kafka: bool,
}

pub fn usage(job: &str) -> String {
    format!(
        "usage:\n  {job} --out DIR --fixture {job}=parity/replay/{job}.jsonl\n  \
         {job} --out DIR parity/examples parity/probes\n  \
         {job} --kafka   (KAFKA_BROKER, INPUT_TOPIC, OUTPUT_TOPIC, GROUP_ID from the environment)"
    )
}

pub fn parse_args<I>(args: I) -> Result<Args>
where
    I: IntoIterator<Item = String>,
{
    let mut parsed = Args::default();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--out" => {
                let value = args.next().context("--out needs a directory")?;
                parsed.out = Some(PathBuf::from(value));
            }
            "--fixture" => {
                let value = args.next().context("--fixture needs <job>=<path>")?;
                let (job, path) = value
                    .split_once('=')
                    .with_context(|| format!("--fixture {value:?} is not <job>=<path>"))?;
                parsed.fixtures.push((job.to_string(), PathBuf::from(path)));
            }
            "--kafka" => parsed.kafka = true,
            other if other.starts_with('-') => bail!("unknown option {other}"),
            suite => parsed.suites.push(PathBuf::from(suite)),
        }
    }
    if parsed.kafka {
        anyhow::ensure!(
            parsed.out.is_none() && parsed.fixtures.is_empty() && parsed.suites.is_empty(),
            "--kafka takes no replay arguments"
        );
    } else {
        anyhow::ensure!(
            parsed.out.is_some(),
            "--out DIR is required in replay modes"
        );
        anyhow::ensure!(
            !parsed.fixtures.is_empty() || !parsed.suites.is_empty(),
            "nothing to run: give --fixture <job>=<path> and/or suite directories"
        );
    }
    Ok(parsed)
}

/// Parses `std::env::args`, runs, and maps the outcome to an exit code:
/// 0 when every fixture/case passed (or the Kafka loop ended cleanly), 1 otherwise.
pub fn main<P>(spec: JobSpec<P::Event>, encode: Encode<P::Output>) -> ExitCode
where
    P: Processor + Default,
    P::Output: JsonRecord,
{
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("{error:#}\n{}", usage(spec.name));
            return ExitCode::from(2);
        }
    };
    match run::<P>(&args, spec, encode) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(error) => {
            eprintln!("{}: {error:#}", spec.name);
            ExitCode::from(1)
        }
    }
}

/// Returns whether everything passed. Errors are environment/fixture problems,
/// not parity verdicts.
pub fn run<P>(args: &Args, spec: JobSpec<P::Event>, encode: Encode<P::Output>) -> Result<bool>
where
    P: Processor + Default,
    P::Output: JsonRecord,
{
    if args.kafka {
        let config = Config::from_lookup(|key| {
            std::env::var(key).ok().or_else(|| match key {
                "INPUT_TOPIC" => Some(spec.input_topic.to_string()),
                "OUTPUT_TOPIC" => Some(spec.output_topic.to_string()),
                "GROUP_ID" => Some(spec.name.to_string()),
                _ => None,
            })
        })?;
        tokio::runtime::Runtime::new()
            .context("starting the async runtime")?
            .block_on(crate::kafka::run(&config, spec, P::default(), encode))?;
        return Ok(true);
    }

    let out = args.out.as_deref().context("--out DIR is required")?;
    let mut all_passed = true;
    for (job, fixture) in &args.fixtures {
        anyhow::ensure!(
            job == spec.name,
            "--fixture names job {job:?}; this binary is {}",
            spec.name
        );
        let run = run_fixture(fixture, spec, P::default())?;
        let status = write_fixture_artifacts(&run, &spec, fixture, out)?;
        println!(
            "{status} {} fixture {} -> {}/{}.*",
            spec.name,
            fixture.display(),
            out.display(),
            spec.name
        );
        for failure in &run.failures {
            println!("  - {failure}");
        }
        all_passed &= status == "PASS";
    }

    let (mut ran, mut skipped) = (0, 0);
    for suite in &args.suites {
        let suite_name = suite
            .file_name()
            .with_context(|| format!("suite directory {} has no name", suite.display()))?;
        for case_dir in case_dirs(suite)? {
            let case_id = format!(
                "{}/{}",
                Path::new(suite_name).display(),
                case_dir.file_name().unwrap_or_default().to_string_lossy()
            );
            let expected = read_json(&case_dir.join("expected.json"))?;
            if expected.get("job").and_then(|job| job.as_str()) != Some(spec.name) {
                skipped += 1;
                continue;
            }
            let run = run_fixture(&case_dir.join("input.jsonl"), spec, P::default())?;
            let outcome = write_case_artifacts(&run, &expected, &out.join(&case_id))?;
            println!("{} {case_id}", outcome.status);
            for failure in &outcome.failures {
                println!("  - {failure}");
            }
            ran += 1;
            all_passed &= outcome.status == "PASS";
        }
    }
    if !args.suites.is_empty() {
        println!(
            "{} cases ran for {}, {skipped} skipped (other job); informational, tools/suite_check.py decides",
            ran, spec.name
        );
    }
    Ok(all_passed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Args> {
        parse_args(args.iter().map(|arg| arg.to_string()))
    }

    #[test]
    fn parses_the_two_replay_modes_and_kafka() {
        let fixture = parse(&["--out", "build/x", "--fixture", "j=parity/replay/j.jsonl"]).unwrap();
        assert_eq!(fixture.out.as_deref(), Some(Path::new("build/x")));
        assert_eq!(
            fixture.fixtures,
            vec![("j".to_string(), PathBuf::from("parity/replay/j.jsonl"))]
        );
        let suite = parse(&["--out", "build/x", "parity/examples", "parity/probes"]).unwrap();
        assert_eq!(suite.suites.len(), 2);
        assert!(parse(&["--kafka"]).unwrap().kafka);
    }

    #[test]
    fn rejects_incomplete_or_mixed_invocations() {
        assert!(parse(&[]).is_err());
        assert!(parse(&["--out", "build/x"]).is_err());
        assert!(parse(&["--fixture", "j=path"]).is_err());
        assert!(parse(&["--fixture", "nopath"]).is_err());
        assert!(parse(&["--kafka", "--out", "x"]).is_err());
        assert!(parse(&["--bogus"]).is_err());
    }
}
