#![feature(async_closure)]
pub mod browser;
pub mod data_processor;

use std::{collections::HashSet, fs, path::Path};

use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use data_processor::Metrics;
use futures::{stream::FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    input: String,

    #[arg(short, long)]
    output_dir: String,

    #[arg(short, long, default_value_t = 3)]
    retry: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InputRecord {
    url: String,
}

fn read_input(path: impl AsRef<Path>) -> Result<HashSet<String>> {
    let input_file = fs::OpenOptions::new().read(true).open(path)?;
    let mut csv_reader = csv::Reader::from_reader(input_file);

    csv_reader
        .deserialize::<InputRecord>()
        .map(|record| match record {
            Ok(record) => Ok(record.url),
            Err(error) => Err(anyhow::Error::from(error)),
        })
        .collect::<Result<HashSet<_>>>()
}

fn write_data<'a, I: Iterator<Item = &'a Metrics>>(
    metrics_iter: I,
    path: impl AsRef<Path>,
) -> Result<()> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;

    let mut csv_writer = csv::Writer::from_writer(file);

    metrics_iter
        .map(|metrics| csv_writer.serialize(metrics))
        .try_for_each(|result| result.map_err(anyhow::Error::from))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let mut channels_url = read_input(args.input)?;

    fs::create_dir_all(&args.output_dir)?;

    let data_path = format!("{}/metrics.csv", args.output_dir);

    let mut try_count = 0;

    while !channels_url.is_empty() {
        println!("Reamining channel count: {}", channels_url.len());
        println!("Remaining channels {:#?}", channels_url);

        if try_count >= args.retry {
            println!("Failed after retry");
            break;
        }

        let metrics_list = channels_url
            .iter()
            .map(|url| async move {
                let mut metrics = Metrics::new(url.clone());

                metrics.fetch_description_and_video_ids().await?;
                metrics.fetch_video_metrics().await?;
                metrics.fetch_sentence_metrics().await?;

                Ok(metrics)
            })
            .collect::<FuturesUnordered<_>>()
            .filter_map(|metrics: Result<Metrics>| async move { metrics.ok() })
            .collect::<Vec<_>>()
            .await;

        write_data(metrics_list.iter(), &data_path)?;

        let mut is_removed = false;

        for metrics in metrics_list.iter() {
            channels_url.remove(&metrics.url);
            is_removed = true;
        }

        try_count = if is_removed { 0 } else { try_count + 1 };
    }

    println!("Finished");

    unsafe {
        browser::close().await?;
    }

    Ok(())
}
