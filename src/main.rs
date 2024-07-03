#![feature(async_closure)]
pub mod data_processor;
pub mod browser;

use std::{
    collections::HashSet,
    fs,
    path::Path,
};

use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InputRecord {
    url: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Metrics {
    url: String,
    description: String,
    average_duration: f64,
    average_sentence_duration: f64,
    average_sentence_length: f64,
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
    let mut channels_url = read_input("input.csv")?;

    let data_file_name = format!("data_{}.csv", Utc::now());

    let mut try_count = 0;

    while !channels_url.is_empty() {
        println!("Reamining channel count: {}", channels_url.len());
        println!("Remaining channels {:#?}", channels_url);

        if try_count >= 3 {
            println!("Failed after retry");
            break;
        }

        let metrics_map = data_processor::get_channels_metrics(channels_url.clone()).await;

        write_data(metrics_map.values(), &data_file_name)?;

        let mut is_removed = false;

        for url in metrics_map.keys() {
            channels_url.remove(url.as_str());
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
