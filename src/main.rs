#![feature(async_closure)]
pub mod metrics;

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use clap::Parser;
use futures::{stream::FuturesUnordered, StreamExt};
use metrics::Metrics;
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    input: String,

    #[arg(short, long)]
    output_dir: String,

    #[arg(short, long, default_value_t = 3)]
    retry: usize,

    #[arg(long, default_value_t = false)]
    is_download: bool,

    #[arg(short, long, default_value_t = 10)]
    limit: u64,
}

#[derive(Debug, Deserialize)]
struct InputRecord {
    link: String,
}

fn read_input(path: impl AsRef<Path>) -> Result<HashSet<String>> {
    let input_file = fs::OpenOptions::new().read(true).open(path)?;
    let mut csv_reader = csv::Reader::from_reader(input_file);

    csv_reader
        .deserialize::<InputRecord>()
        .map(|record| match record {
            Ok(record) => Ok(record.link),
            Err(error) => Err(anyhow::Error::from(error)),
        })
        .collect::<Result<HashSet<_>>>()
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().pretty())
        .with(LevelFilter::INFO)
        .init();

    let args = Args::parse();

    let channels_url = Arc::new(Mutex::new(read_input(args.input)?));

    fs::create_dir_all(&args.output_dir)?;

    let metrics_path = format!("{}/metrics.csv", args.output_dir);

    let mut try_count = 0;
    let mut pre_length = channels_url.lock().await.len();

    while pre_length > 0 {
        tracing::info!("Reamining channel count: {}", pre_length);
        tracing::info!("Remaining channels {:#?}", channels_url.lock().await);

        if try_count >= args.retry {
            tracing::info!("Failed after retry");
            break;
        }

        let urls = channels_url.lock().await.clone();
        urls.into_iter()
            .take(args.limit as usize)
            .map(|url| {
                let output_dir = args.output_dir.clone();
                let metrics_path = metrics_path.clone();
                let channels_url = channels_url.clone();
                async move {
                    let metrics = Metrics::new(url.clone()).await?;

                    tracing::info!("{}: start writing data", &metrics.link);

                    let metrics_file = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(metrics_path)?;
                    let mut metrics_writer = csv::WriterBuilder::new()
                        .has_headers(false)
                        .from_writer(metrics_file);

                    metrics_writer.serialize(&metrics).unwrap();

                    metrics_writer.flush()?;

                    tracing::info!("{}: finished writing data", &metrics.link);

                    if args.is_download {
                        metrics.download(PathBuf::from(output_dir)).await.unwrap();
                    }

                    channels_url.lock().await.remove(&url);

                    Ok(())
                }
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<Result<_>>>()
            .await;

        let current_length = channels_url.lock().await.len();
        try_count = if current_length == pre_length {
            try_count + 1
        } else {
            0
        };
        pre_length = current_length;
    }

    tracing::info!("Finished");

    Ok(())
}
