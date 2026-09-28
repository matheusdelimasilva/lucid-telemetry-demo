use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message as KafkaMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};

pub mod smoke {
    include!(concat!(env!("OUT_DIR"), "/smoke.rs"));
}

const TOPIC: &str = "smoke.v1";

#[tokio::main]
async fn main() -> Result<()> {
    let broker = env::var("KAFKA_BROKER").unwrap_or_else(|_| "localhost:9092".to_string());
    let msg = smoke::Smoke {
        vin: "TST00000000000002".to_string(),
        ts: 1790000100000,
        note: "stage1 smoke from rust".to_string(),
    };
    let payload = msg.encode_to_vec();

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
        .context("producing smoke message")?;
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
    loop {
        match consumer.poll(Duration::from_secs(1)) {
            Some(Ok(m)) => {
                let decoded = smoke::Smoke::decode(m.payload().unwrap_or(&[]))
                    .context("decoding smoke message")?;
                if decoded.vin != msg.vin || decoded.ts != msg.ts || decoded.note != msg.note {
                    bail!("decoded message {decoded:?} does not match produced message {msg:?}");
                }
                println!("decoded: {decoded:?}");
                println!("OK");
                return Ok(());
            }
            Some(Err(e)) => return Err(e).context("poll error"),
            None if Instant::now() >= deadline => {
                bail!("timed out after 30s waiting for the smoke message")
            }
            None => {}
        }
    }
}
