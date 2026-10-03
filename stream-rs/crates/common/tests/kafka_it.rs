mod support;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use common::config::Config;
use common::job::CHARGING;
use common::kafka::run_batches;
use common::proto::charging::{ChargingEvent, ChargingEventType};
use prost::Message as ProstMessage;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};
use serde_json::Value;
use support::{Receipt, Recorder};

#[tokio::test]
#[ignore]
async fn kafka_batches_deliver_outputs_commit_offsets_and_resume() -> Result<()> {
    let broker = std::env::var("KAFKA_BROKER").context("KAFKA_BROKER is required")?;
    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let suffix = format!("{}-{unique}", std::process::id());
    let input_topic = format!("stage3b-input-{suffix}");
    let output_topic = format!("stage3b-output-{suffix}");
    let group_id = format!("stage3b-group-{suffix}");
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .create()
        .context("creating admin client")?;
    for result in admin
        .create_topics(
            &[
                NewTopic::new(&input_topic, 1, TopicReplication::Fixed(1)),
                NewTopic::new(&output_topic, 1, TopicReplication::Fixed(1)),
            ],
            &AdminOptions::new(),
        )
        .await?
    {
        result.map_err(|(name, code)| anyhow::anyhow!("creating topic {name}: {code}"))?;
    }

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .create()
        .context("creating test producer")?;
    let valid = charging_event(
        "00000000-0000-4000-8000-000000000101",
        1_600_000_000_000,
        ChargingEventType::PlugIn,
    );
    let progress = charging_event(
        "00000000-0000-4000-8000-000000000102",
        valid.ts + 700_000,
        ChargingEventType::Progress,
    );
    let mut invalid = charging_event(
        "00000000-0000-4000-8000-000000000103",
        valid.ts + 100_000,
        ChargingEventType::Progress,
    );
    invalid.event_id = "invalid-uuid".to_string();
    for event in [&valid, &valid, &invalid, &progress] {
        let payload = event.encode_to_vec();
        producer
            .send(
                FutureRecord::to(&input_topic)
                    .partition(0)
                    .key(&event.vin)
                    .payload(&payload),
                Duration::from_secs(0),
            )
            .await
            .map_err(|(error, _)| error)
            .context("producing test event")?;
    }

    let config = Config {
        kafka_broker: broker.clone(),
        input_topic: input_topic.clone(),
        output_topic: output_topic.clone(),
        group_id: group_id.clone(),
        input_partition: 0,
        batch_max_events: 20,
        batch_poll_timeout_ms: 200,
    };
    let runner = run_batches(
        &config,
        CHARGING,
        Recorder::<ChargingEvent>::default(),
        encode_receipt,
        Some(10),
    )
    .await?;
    let input_offset = committed_offset(&broker, &input_topic, 0, &group_id)?;
    assert_eq!(input_offset, 4);
    assert_eq!(runner.counters().rejected, 1);
    assert_eq!(runner.counters().duplicate_events, 1);

    let output_consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .set("group.id", format!("stage3b-output-reader-{suffix}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?;
    let mut output_assignment = TopicPartitionList::new();
    output_assignment.add_partition_offset(&output_topic, 0, Offset::Beginning)?;
    output_consumer.assign(&output_assignment)?;
    let first_outputs = read_available(&output_consumer);
    assert!(
        first_outputs.iter().any(|value| {
            value["call"] == "on_event"
                && value["event_id"] == "00000000-0000-4000-8000-000000000101"
        }),
        "expected Recorder event output, got {first_outputs:?}"
    );

    let second_run = run_batches(
        &config,
        CHARGING,
        Recorder::<ChargingEvent>::default(),
        encode_receipt,
        Some(1),
    )
    .await?;
    assert_eq!(second_run.counters().rejected, 0);
    assert_eq!(second_run.counters().duplicate_events, 0);
    assert!(read_available(&output_consumer).is_empty());
    assert_eq!(committed_offset(&broker, &input_topic, 0, &group_id)?, 4);

    for result in admin
        .delete_topics(&[&input_topic, &output_topic], &AdminOptions::new())
        .await?
    {
        result.map_err(|(name, code)| anyhow::anyhow!("deleting topic {name}: {code}"))?;
    }
    Ok(())
}

fn charging_event(id: &str, ts: i64, event: ChargingEventType) -> ChargingEvent {
    ChargingEvent {
        event_id: id.to_string(),
        vin: "TST00000000000001".to_string(),
        ts,
        event: event as i32,
        energy_wh: 0,
        lat: (event == ChargingEventType::PlugIn).then_some(37.4),
        lon: (event == ChargingEventType::PlugIn).then_some(-122.1),
        charger_type: (event == ChargingEventType::PlugIn).then(|| "dc_fast".to_string()),
    }
}

fn encode_receipt(receipt: &Receipt) -> (String, Vec<u8>) {
    let vin = match receipt {
        Receipt::Event { vin, .. } | Receipt::Watermark { vin, .. } => vin.clone(),
    };
    (vin, serde_json::to_vec(receipt).unwrap())
}

fn committed_offset(broker: &str, topic: &str, partition: i32, group_id: &str) -> Result<i64> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .create()?;
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition(topic, partition);
    let offsets = consumer.committed_offsets(assignment, Duration::from_secs(3))?;
    offsets
        .find_partition(topic, partition)
        .and_then(|partition| match partition.offset() {
            Offset::Offset(offset) => Some(offset),
            _ => None,
        })
        .context("partition has no committed offset")
}

fn read_available(consumer: &BaseConsumer) -> Vec<Value> {
    let mut values = Vec::new();
    let mut idle_polls = 0;
    while idle_polls < 3 {
        match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(message)) => {
                values.push(serde_json::from_slice(message.payload().unwrap_or_default()).unwrap());
                idle_polls = 0;
            }
            Some(Err(_)) => continue,
            None => idle_polls += 1,
        }
    }
    values
}
