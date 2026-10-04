//! Redpanda integration test (`KAFKA_BROKER`, `#[ignore]`d; `make up` first):
//! charging events in, one `charging.sessions.v1.ChargingSession` protobuf out,
//! keyed by VIN, offsets committed, the job's counters tallied.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use charging_sessions::{encode, output_id, ChargingSessions};
use common::config::Config;
use common::job::CHARGING;
use common::kafka::run_batches;
use common::proto::charging::{ChargingEvent, ChargingEventType};
use common::proto::charging_output::{ChargingSession, CloseReason};
use prost::Message as ProstMessage;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};

const VIN: &str = "TST00000000000001";
const START_TS: i64 = 1_790_000_100_000;

#[tokio::test]
#[ignore]
async fn charging_sessions_reach_kafka_as_protobuf_keyed_by_vin() -> Result<()> {
    let broker = std::env::var("KAFKA_BROKER").context("KAFKA_BROKER is required")?;
    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let suffix = format!("{}-{unique}", std::process::id());
    let input_topic = format!("stage6-charging-input-{suffix}");
    let output_topic = format!("stage6-charging-output-{suffix}");
    let group_id = format!("stage6-charging-group-{suffix}");
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

    let plug_in = event(
        "00000000-0000-4000-8000-000000000601",
        START_TS,
        ChargingEventType::PlugIn,
        0,
    );
    let progress = event(
        "00000000-0000-4000-8000-000000000602",
        START_TS + 300_000,
        ChargingEventType::Progress,
        1_500,
    );
    // START may not carry energy: rejected, never reaches the session.
    let invalid = event(
        "00000000-0000-4000-8000-000000000603",
        START_TS + 400_000,
        ChargingEventType::Start,
        10,
    );
    let unplug = event(
        "00000000-0000-4000-8000-000000000604",
        START_TS + 600_000,
        ChargingEventType::Unplug,
        0,
    );
    // Moves the watermark to exactly the UNPLUG: it is released, this one stays buffered.
    let anchor = event(
        "00000000-0000-4000-8000-000000000605",
        unplug.ts + CHARGING.delay_ms,
        ChargingEventType::PlugIn,
        0,
    );
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .create()
        .context("creating test producer")?;
    let inputs = [&plug_in, &progress, &progress, &invalid, &unplug, &anchor];
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
    // Empty polls end a batch, so the batch budget doubles as the time budget
    // for the fresh consumer group to get its assignment (15 x 200 ms).
    let runner = run_batches(
        &config,
        CHARGING,
        ChargingSessions::default(),
        encode,
        Some(15),
    )
    .await?;
    assert_eq!(runner.counters().rejected, 1);
    assert_eq!(runner.counters().duplicate_events, 1);
    assert_eq!(runner.watermark(), Some(unplug.ts));
    assert_eq!(
        *runner.processor(),
        ChargingSessions {
            orphan: 0,
            unplug: 1,
            inactivity_timeout: 0,
            replaced_by_plug_in: 0,
        }
    );
    assert_eq!(
        committed_offset(&broker, &input_topic, 0, &group_id)?,
        inputs.len() as i64
    );

    let output_consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .set("group.id", format!("stage6-charging-reader-{suffix}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?;
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(&output_topic, 0, Offset::Beginning)?;
    output_consumer.assign(&assignment)?;
    let outputs = read_available(&output_consumer)?;
    assert_eq!(outputs.len(), 1, "exactly one closed session: {outputs:?}");
    let (key, session) = &outputs[0];
    assert_eq!(key, VIN, "Kafka key is the VIN");
    assert_eq!(
        *session,
        ChargingSession {
            output_id: output_id(&plug_in.event_id),
            vin: VIN.to_string(),
            start_ts: START_TS,
            end_ts: unplug.ts,
            duration_ms: 600_000,
            total_energy_wh: 1_500,
            start_lat: -33.868,
            start_lon: 151.21,
            charger_type: "dc_fast".to_string(),
            close_reason: CloseReason::Unplug as i32,
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

fn event(id: &str, ts: i64, kind: ChargingEventType, energy_wh: i64) -> ChargingEvent {
    let plug_in = kind == ChargingEventType::PlugIn;
    ChargingEvent {
        event_id: id.to_string(),
        vin: VIN.to_string(),
        ts,
        event: kind as i32,
        energy_wh,
        lat: plug_in.then_some(-33.8675),
        lon: plug_in.then_some(151.2095),
        charger_type: plug_in.then(|| "dc_fast".to_string()),
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

fn read_available(consumer: &BaseConsumer) -> Result<Vec<(String, ChargingSession)>> {
    let mut outputs = Vec::new();
    let mut idle_polls = 0;
    while idle_polls < 3 {
        match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(message)) => {
                let key = String::from_utf8(message.key().unwrap_or_default().to_vec())?;
                let session = ChargingSession::decode(message.payload().unwrap_or_default())?;
                outputs.push((key, session));
                idle_polls = 0;
            }
            Some(Err(_)) => continue,
            None => idle_polls += 1,
        }
    }
    Ok(outputs)
}
