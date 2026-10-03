use anyhow::{Context, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub kafka_broker: String,
    pub input_topic: String,
    pub output_topic: String,
    pub group_id: String,
    pub input_partition: i32,
    pub batch_max_events: usize,
    pub batch_poll_timeout_ms: u64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    pub fn from_lookup<F>(mut lookup: F) -> Result<Self>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let kafka_broker = lookup("KAFKA_BROKER").unwrap_or_else(|| "localhost:9092".to_string());
        let input_topic = required(&mut lookup, "INPUT_TOPIC")?;
        let output_topic = required(&mut lookup, "OUTPUT_TOPIC")?;
        let group_id = required(&mut lookup, "GROUP_ID")?;
        let input_partition = parse(&mut lookup, "INPUT_PARTITION", 0)?;
        let batch_max_events = parse(&mut lookup, "BATCH_MAX_EVENTS", 500)?;
        let batch_poll_timeout_ms = parse(&mut lookup, "BATCH_POLL_TIMEOUT_MS", 1000)?;
        anyhow::ensure!(
            batch_max_events > 0,
            "BATCH_MAX_EVENTS must be greater than zero"
        );
        anyhow::ensure!(
            batch_poll_timeout_ms > 0,
            "BATCH_POLL_TIMEOUT_MS must be greater than zero"
        );

        Ok(Self {
            kafka_broker,
            input_topic,
            output_topic,
            group_id,
            input_partition,
            batch_max_events,
            batch_poll_timeout_ms,
        })
    }
}

fn required<F>(lookup: &mut F, key: &str) -> Result<String>
where
    F: FnMut(&str) -> Option<String>,
{
    lookup(key)
        .filter(|value| !value.is_empty())
        .with_context(|| format!("required environment variable {key} is missing or empty"))
}

fn parse<F, T>(lookup: &mut F, key: &str, default: T) -> Result<T>
where
    F: FnMut(&str) -> Option<String>,
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match lookup(key) {
        Some(value) => value
            .parse()
            .map_err(|error| anyhow::anyhow!("invalid {key} value {value:?}: {error}")),
        None => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn config(values: &[(&str, &str)]) -> Result<Config> {
        let values = values
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect::<HashMap<_, _>>();
        Config::from_lookup(|key| values.get(key).cloned())
    }

    #[test]
    fn parses_defaults_and_required_values() {
        let parsed = config(&[
            ("INPUT_TOPIC", "input"),
            ("OUTPUT_TOPIC", "output"),
            ("GROUP_ID", "group"),
        ])
        .unwrap();
        assert_eq!(parsed.kafka_broker, "localhost:9092");
        assert_eq!(parsed.input_partition, 0);
        assert_eq!(parsed.batch_max_events, 500);
        assert_eq!(parsed.batch_poll_timeout_ms, 1000);
    }

    #[test]
    fn parses_overrides() {
        let parsed = config(&[
            ("KAFKA_BROKER", "broker:9092"),
            ("INPUT_TOPIC", "input"),
            ("OUTPUT_TOPIC", "output"),
            ("GROUP_ID", "group"),
            ("INPUT_PARTITION", "3"),
            ("BATCH_MAX_EVENTS", "15"),
            ("BATCH_POLL_TIMEOUT_MS", "42"),
        ])
        .unwrap();
        assert_eq!(parsed.kafka_broker, "broker:9092");
        assert_eq!(parsed.input_partition, 3);
        assert_eq!(parsed.batch_max_events, 15);
        assert_eq!(parsed.batch_poll_timeout_ms, 42);
    }

    #[test]
    fn errors_clearly_for_required_or_invalid_values() {
        assert!(config(&[]).unwrap_err().to_string().contains("INPUT_TOPIC"));
        assert!(config(&[
            ("INPUT_TOPIC", "input"),
            ("OUTPUT_TOPIC", "output"),
            ("GROUP_ID", "group"),
            ("INPUT_PARTITION", "nope"),
        ])
        .unwrap_err()
        .to_string()
        .contains("INPUT_PARTITION"));
        assert!(config(&[
            ("INPUT_TOPIC", "input"),
            ("OUTPUT_TOPIC", "output"),
            ("GROUP_ID", "group"),
            ("BATCH_POLL_TIMEOUT_MS", "0"),
        ])
        .unwrap_err()
        .to_string()
        .contains("BATCH_POLL_TIMEOUT_MS"));
    }
}
