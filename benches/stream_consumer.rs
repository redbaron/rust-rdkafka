use criterion::*;
use futures::StreamExt;

use std::collections::HashMap;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::{Offset, TopicPartitionList};

fn bench(c: &mut Criterion) {
    let elems: u64 = 10_000_000;

    let mut group = c.benchmark_group("consume");

    let mut consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "criterion-bench")
        .set("bootstrap.servers", "127.0.0.1:9092")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    let tpl = TopicPartitionList::from_topic_map(
        &(0..16)
            .map(|p| ((String::from("bench"), p), Offset::Beginning))
            .collect(),
    );

    group
        .throughput(Throughput::Elements(elems))
        .bench_function("abc", move |b| {
            b.iter_batched(
                || {
                    consumer.assign(&tpl).unwrap();
                },
                |_| {
                    // Create the runtime
                    let mut rt = tokio::runtime::Runtime::new().unwrap();
                    rt.block_on(async {
                        let mut i: u64 = 0;
                        let mut stream = consumer.start();
                        while let Some(message) = stream.next().await {
                            i = i + 1;
                            if i > elems {
                                break;
                            }
                        }
                    });
                },
                BatchSize::PerIteration,
            )
        });
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
