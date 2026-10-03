use std::time::Duration;

use anyhow::{Context, Result};
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::Message as KafkaMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};

use crate::config::Config;
use crate::job::JobSpec;
use crate::processor::Processor;
use crate::runner::Runner;

pub async fn run<P>(
    config: &Config,
    spec: JobSpec<P::Event>,
    processor: P,
    encode: fn(&P::Output) -> (String, Vec<u8>),
) -> Result<()>
where
    P: Processor,
{
    let _ = run_batches(config, spec, processor, encode, None).await?;
    Ok(())
}

pub async fn run_batches<P>(
    config: &Config,
    spec: JobSpec<P::Event>,
    processor: P,
    encode: fn(&P::Output) -> (String, Vec<u8>),
    max_batches: Option<usize>,
) -> Result<Runner<P>>
where
    P: Processor,
{
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_broker)
        .set("group.id", &config.group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .context("creating Kafka consumer")?;
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_broker)
        .set("message.timeout.ms", "30000")
        .create()
        .context("creating Kafka producer")?;

    let mut committed_request = TopicPartitionList::new();
    committed_request.add_partition(&config.input_topic, config.input_partition);
    let committed = loop {
        match consumer.committed_offsets(
            committed_request.clone(),
            Duration::from_millis(config.batch_poll_timeout_ms),
        ) {
            Ok(offsets) => break offsets,
            Err(error) => eprintln!("transient committed-offset error, retrying: {error}"),
        }
    };
    let start_offset = committed
        .find_partition(&config.input_topic, config.input_partition)
        .map(|partition| partition.offset())
        .filter(|offset| matches!(offset, Offset::Offset(_)))
        .unwrap_or(Offset::Beginning);
    let mut assignments = TopicPartitionList::new();
    assignments.add_partition_offset(&config.input_topic, config.input_partition, start_offset)?;
    consumer
        .assign(&assignments)
        .context("assigning input partition")?;

    let mut runner = Runner::new(spec, processor);
    let mut batches_run = 0;
    while max_batches.is_none_or(|limit| batches_run < limit) {
        let mut batch = Vec::new();
        let mut last_offset = None;
        let mut messages_in_batch = 0;
        while messages_in_batch < config.batch_max_events {
            match consumer.poll(Duration::from_millis(config.batch_poll_timeout_ms)) {
                Some(Ok(message)) => {
                    messages_in_batch += 1;
                    let offset = message.offset();
                    last_offset = Some(offset);
                    match P::Event::decode(message.payload().unwrap_or_default()) {
                        Ok(event) => batch.push((offset as u64, event)),
                        Err(error) => {
                            let vin = message.key().and_then(|key| std::str::from_utf8(key).ok());
                            runner.record_decode_rejection(vin);
                            eprintln!("undecodable Kafka payload at offset {offset}: {error}");
                        }
                    }
                }
                Some(Err(error)) => {
                    eprintln!("transient consumer error, retrying: {error}");
                }
                None => break,
            }
        }

        let outcome = runner.process_batch(batch);
        for (vin, output) in outcome.emitted {
            let (key, payload) = encode(&output);
            producer
                .send(
                    FutureRecord::to(&config.output_topic)
                        .key(&key)
                        .payload(&payload),
                    Duration::from_secs(0),
                )
                .await
                .map_err(|(error, _)| error)
                .with_context(|| format!("producing output for VIN {vin}"))?;
        }
        if let Some(offset) = last_offset {
            let mut commit = TopicPartitionList::new();
            commit.add_partition_offset(
                &config.input_topic,
                config.input_partition,
                Offset::Offset(offset + 1),
            )?;
            consumer
                .commit(&commit, CommitMode::Sync)
                .context("committing input offset")?;
        }
        batches_run += 1;
    }
    Ok(runner)
}
