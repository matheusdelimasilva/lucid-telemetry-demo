#![allow(dead_code)]

use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use common::event::InputEvent;
use common::job::RESERVED_VIN;
use common::processor::Processor;
use common::record::JsonRecord;
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "call")]
pub enum Receipt {
    #[serde(rename = "on_event")]
    Event {
        vin: String,
        event_id: String,
        ts: i64,
    },
    #[serde(rename = "on_watermark")]
    Watermark { vin: String, watermark: i64 },
}

impl JsonRecord for Receipt {
    fn to_json(&self) -> Value {
        serde_json::to_value(self).expect("Receipt serializes")
    }
}

#[derive(Clone, Debug)]
pub struct SeenEvent {
    pub vin: String,
    pub event_id: String,
    pub ts: i64,
}

pub struct Recorder<E> {
    marker: PhantomData<E>,
}

impl<E> Default for Recorder<E> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<E: InputEvent> Processor for Recorder<E> {
    type Event = E;
    type State = Vec<SeenEvent>;
    type Output = Receipt;

    fn on_event(&mut self, event: &Self::Event, state: &mut Self::State) -> Vec<Self::Output> {
        assert_ne!(
            event.vin(),
            RESERVED_VIN,
            "reserved control event must remain buffered"
        );
        state.push(SeenEvent {
            vin: event.vin().to_owned(),
            event_id: event.event_id().to_owned(),
            ts: event.ts(),
        });
        vec![Receipt::Event {
            vin: event.vin().to_owned(),
            event_id: event.event_id().to_owned(),
            ts: event.ts(),
        }]
    }

    fn on_watermark(
        &mut self,
        vin: &str,
        watermark_ms: i64,
        _state: &mut Self::State,
    ) -> Vec<Self::Output> {
        vec![Receipt::Watermark {
            vin: vin.to_owned(),
            watermark: watermark_ms,
        }]
    }

    fn is_open(&self, _state: &Self::State) -> bool {
        false
    }
}

pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

pub fn json_lines(path: &Path) -> Result<Vec<Value>> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("reading JSONL file {}", path.display()))?;
    content
        .lines()
        .enumerate()
        .map(|(index, line)| {
            serde_json::from_str(line)
                .with_context(|| format!("parsing {} line {}", path.display(), index + 1))
        })
        .collect()
}

pub fn case_dirs(root: &Path, suite: &str) -> Result<Vec<std::path::PathBuf>> {
    let mut dirs = std::fs::read_dir(root.join("parity").join(suite))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .filter(|path| {
            path.is_dir()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.len() >= 4
                            && name.as_bytes()[0].is_ascii_digit()
                            && name.as_bytes()[1].is_ascii_digit()
                            && name.as_bytes()[2] == b'-'
                    })
        })
        .collect::<Vec<_>>();
    dirs.sort();
    Ok(dirs)
}

pub fn parse_expected(path: &Path) -> Result<Value> {
    serde_json::from_slice(&std::fs::read(path)?)
        .with_context(|| format!("parsing expected values in {}", path.display()))
}
