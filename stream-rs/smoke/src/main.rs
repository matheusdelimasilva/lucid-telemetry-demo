use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message as KafkaMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};

pub mod charging {
    include!(concat!(env!("OUT_DIR"), "/vehicle.charging.v1.rs"));
}
pub mod battery_input {
    include!(concat!(env!("OUT_DIR"), "/vehicle.battery.v1.rs"));
}
pub mod charging_output {
    include!(concat!(env!("OUT_DIR"), "/charging.sessions.v1.rs"));
}
pub mod battery_output {
    include!(concat!(env!("OUT_DIR"), "/battery.health.v1.rs"));
}

use charging::{ChargingEvent, ChargingEventType};

const TOPIC: &str = "vehicle.charging.v1";

#[tokio::main]
async fn main() -> Result<()> {
    let broker = env::var("KAFKA_BROKER").unwrap_or_else(|_| "localhost:9092".to_string());
    let msg = ChargingEvent {
        event_id: "00000000-0000-4000-8000-000000000001".to_string(),
        vin: "TST00000000000002".to_string(),
        ts: 1790000100000,
        event: ChargingEventType::PlugIn as i32,
        energy_wh: 0,
        lat: Some(37.4),
        lon: Some(-122.1),
        charger_type: Some("dc_fast".to_string()),
    };
    let payload = msg.encode_to_vec();

    // Prove the other three generated modules compile: encode a default value
    // from each.
    for (name, bytes) in [
        (
            "vehicle.battery.v1.BatteryReading",
            battery_input::BatteryReading::default().encode_to_vec(),
        ),
        (
            "charging.sessions.v1.ChargingSession",
            charging_output::ChargingSession::default().encode_to_vec(),
        ),
        (
            "battery.health.v1.BatteryWindow",
            battery_output::BatteryWindow::default().encode_to_vec(),
        ),
    ] {
        println!("{name}: {} bytes", bytes.len());
    }

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .set("message.timeout.ms", "10000")
        .create()
        .context("creating producer")?;

    let (partition, offset) = producer
        .send(
            FutureRecord::to(TOPIC).key(&msg.vin).payload(&payload),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)
        .context("producing charging event")?;
    println!(
        "produced {} bytes to {} partition {} offset {}",
        payload.len(),
        TOPIC,
        partition,
        offset
    );

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let group = format!("smoke-check-{nanos}");
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .set("group.id", &group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .context("creating consumer")?;

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(TOPIC, partition, Offset::Offset(offset))?;
    consumer.assign(&tpl).context("assigning partition")?;

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last_err = None;
    loop {
        match consumer.poll(Duration::from_secs(1)) {
            Some(Ok(m)) => {
                let decoded = ChargingEvent::decode(m.payload().unwrap_or(&[]))
                    .context("decoding charging event")?;
                if decoded != msg {
                    bail!("decoded message {decoded:?} does not match produced message {msg:?}");
                }
                println!("decoded: {decoded:?}");
                println!("OK");
                return Ok(());
            }
            // librdkafka reports transient connection errors (e.g. a broker still
            // starting) as consumer events; keep polling until the deadline.
            Some(Err(e)) if Instant::now() < deadline => {
                eprintln!("transient consumer error, retrying: {e}");
                last_err = Some(e);
            }
            Some(Err(e)) => return Err(e).context("poll error"),
            None if Instant::now() >= deadline => match last_err {
                Some(e) => return Err(e).context("timed out after 30s; last consumer error"),
                None => bail!("timed out after 30s waiting for the smoke message"),
            },
            None => {}
        }
    }
}
