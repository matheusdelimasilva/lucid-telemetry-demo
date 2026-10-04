//! Redpanda integration test (`KAFKA_BROKER`, `#[ignore]`d; `make up` first):
//! readings in, one `battery.health.v1.BatteryWindow` protobuf out, keyed by VIN,
//! offsets committed.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use battery_health::{encode, output_id, BatteryHealth, WINDOW_MS};
use common::config::Config;
use common::job::BATTERY;
use common::kafka::run_batches;
use common::proto::battery_input::BatteryReading;
use common::proto::battery_output::BatteryWindow;
use prost::Message as ProstMessage;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};

const VIN: &str = "TST00000000000001";
const WINDOW_START: i64 = 1_790_000_100_000;

#[tokio::test]
#[ignore]
async fn battery_windows_reach_kafka_as_protobuf_keyed_by_vin() -> Result<()> {
    let broker = std::env::var("KAFKA_BROKER").context("KAFKA_BROKER is required")?;
    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let suffix = format!("{}-{unique}", std::process::id());
    let input_topic = format!("stage5-battery-input-{suffix}");
    let output_topic = format!("stage5-battery-output-{suffix}");
    let group_id = format!("stage5-battery-group-{suffix}");
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

    let first = reading(
        "00000000-0000-4000-8000-000000000501",
        WINDOW_START + 1_000,
        80.0,
        30.0,
    );
    let second = reading(
        "00000000-0000-4000-8000-000000000502",
        WINDOW_START + 2_000,
        60.0,
        56.0,
    );
    let mut invalid = reading(
        "00000000-0000-4000-8000-000000000503",
        WINDOW_START + 3_000,
        50.0,
        30.0,
    );
    invalid.soc_pct = None;
    // Moves the watermark to exactly the window end: the window closes, this reading stays open.
    let anchor = reading(
        "00000000-0000-4000-8000-000000000504",
        WINDOW_START + WINDOW_MS + BATTERY.delay_ms,
        70.0,
        30.0,
    );
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .create()
        .context("creating test producer")?;
    let inputs = [&first, &first, &second, &invalid, &anchor];
    for event in inputs {
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
            .context("producing test reading")?;
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
    // Empty polls end a batch, so the batch budget doubles as the time budget
    // for the fresh consumer group to get its assignment (15 x 200 ms).
    let runner = run_batches(&config, BATTERY, BatteryHealth, encode, Some(15)).await?;
    assert_eq!(runner.counters().rejected, 1);
    assert_eq!(runner.counters().duplicate_events, 1);
    assert_eq!(runner.watermark(), Some(WINDOW_START + WINDOW_MS));
    assert_eq!(
        committed_offset(&broker, &input_topic, 0, &group_id)?,
        inputs.len() as i64
    );

    let output_consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .set("group.id", format!("stage5-battery-reader-{suffix}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?;
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(&output_topic, 0, Offset::Beginning)?;
    output_consumer.assign(&assignment)?;
    let outputs = read_available(&output_consumer)?;
    assert_eq!(outputs.len(), 1, "exactly one closed window: {outputs:?}");
    let (key, window) = &outputs[0];
    assert_eq!(key, VIN, "Kafka key is the VIN");
    assert_eq!(
        *window,
        BatteryWindow {
            output_id: output_id(VIN, WINDOW_START),
            vin: VIN.to_string(),
            window_start: WINDOW_START,
            avg_soc_pct: 70.0,
            min_soh_pct: 95.0,
            max_cell_temp_c: 56.0,
            alert: true,
            event_count: 2,
        }
    );

    for result in admin
        .delete_topics(&[&input_topic, &output_topic], &AdminOptions::new())
        .await?
    {
        result.map_err(|(name, code)| anyhow::anyhow!("deleting topic {name}: {code}"))?;
    }
    Ok(())
}

fn reading(id: &str, ts: i64, soc_pct: f64, cell_temp_max_c: f64) -> BatteryReading {
    BatteryReading {
        event_id: id.to_string(),
        vin: VIN.to_string(),
        ts,
        soc_pct: Some(soc_pct),
        soh_pct: Some(95.0),
        cell_temp_max_c: Some(cell_temp_max_c),
        pack_voltage_v: Some(400.0),
    }
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

fn read_available(consumer: &BaseConsumer) -> Result<Vec<(String, BatteryWindow)>> {
    let mut outputs = Vec::new();
    let mut idle_polls = 0;
    while idle_polls < 3 {
        match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(message)) => {
                let key = String::from_utf8(message.key().unwrap_or_default().to_vec())?;
                let window = BatteryWindow::decode(message.payload().unwrap_or_default())?;
                outputs.push((key, window));
                idle_polls = 0;
            }
            Some(Err(_)) => continue,
            None => idle_polls += 1,
        }
    }
    Ok(outputs)
}
