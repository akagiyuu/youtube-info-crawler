#![feature(async_closure)]
use std::collections::{HashMap, HashSet};

use anyhow::Result;
use chrono::Utc;
use fantoccini::{Client, ClientBuilder, Locator};
use futures::{stream::FuturesUnordered, StreamExt};
use rustube::{Id, VideoFetcher};
use tokio::{fs, io::AsyncWriteExt};
use youtube_captions::{language_tags::LanguageTag, DigestScraper};

#[derive(Debug)]
struct Metrics {
    description: String,
    average_duration: f64,
    average_sentence_duration: f64,
    average_sentence_length: f64,
}

async fn init_browser() -> Result<Client> {
    let client = ClientBuilder::native()
        .capabilities(
            serde_json::from_str(
                r#"{"browserName":"chrome","goog:chromeOptions":{"args":["--headless"]}}"#,
            )
            .unwrap(),
        )
        .connect("http://localhost:9515")
        .await?;

    Ok(client)
}

async fn get_channel_information(channel_url: &str) -> Result<(String, (String, Vec<String>))> {
    let client = init_browser().await?;

    let videos_url = format!("{}/videos", channel_url);

    client.goto(&videos_url).await?;

    let channel_description_show_more_link_selector =
        Locator::Css("yt-description-preview-view-model");
    client
        .wait()
        .for_element(channel_description_show_more_link_selector)
        .await?
        .click()
        .await?;

    let channel_description_selector = Locator::Css("#description-container > span:nth-child(1)");
    let description = client
        .wait()
        .for_element(channel_description_selector)
        .await?
        .text()
        .await?;

    let video_link_selector = Locator::Css("ytd-rich-item-renderer > div:nth-child(1) > ytd-rich-grid-media:nth-child(1) > div:nth-child(1) > div:nth-child(3) > div:nth-child(2) > h3:nth-child(1) > a:nth-child(2)");
    client.wait().for_element(video_link_selector).await?;
    let video_elements = client.find_all(video_link_selector).await?;
    let recent_videos_ids = video_elements
        .into_iter()
        .map(|element| async move { element.attr("href").await.unwrap().unwrap() })
        .collect::<FuturesUnordered<_>>()
        .map(|relative_link| format!("https://www.youtube.com/{relative_link}"))
        .map(|link| link.split_once('=').unwrap().1.to_string())
        .collect::<Vec<_>>()
        .await;
    eprintln!("{}: finished getting video ids", channel_url);

    Ok((channel_url.to_string(), (description, recent_videos_ids)))
}

async fn get_video_metrics(recent_videos_ids: Vec<String>) -> f64 {
    let (total_duration, video_count) = recent_videos_ids
        .into_iter()
        .map(|id| Id::from_string(id).unwrap())
        .map(|id| async move { VideoFetcher::from_id(id.clone()).unwrap().fetch().await })
        .collect::<FuturesUnordered<_>>()
        .filter_map(|descrambler| async move { descrambler.ok() })
        .map(|descrambler| descrambler.video_details().clone())
        .map(|video_details| video_details.length_seconds)
        .fold((0, 0u64), |acc, duration| async move {
            (acc.0 + duration, acc.1 + 1)
        })
        .await;

    total_duration as f64 / video_count as f64
}

async fn get_sentence_metrics(recent_videos_ids: Vec<String>) -> (f64, f64) {
    let (total_sentence_duration, total_sentence_length, sentence_count) = recent_videos_ids
        .into_iter()
        .map(|id| async move {
            DigestScraper::new(reqwest::Client::new())
                .fetch(id.as_str(), None)
                .await
        })
        .collect::<FuturesUnordered<_>>()
        .filter_map(|digest| async move { digest.ok() })
        .filter_map(|digest| async move {
            digest
                .captions
                .into_iter()
                .find(|cap| LanguageTag::parse("vi").unwrap().matches(&cap.lang_tag))
        })
        .filter_map(|caption_scrapper| async move { caption_scrapper.fetch_srv1().await.ok() })
        .map(futures::stream::iter)
        .flatten()
        .map(|subtitle| (subtitle.duration_secs, subtitle.value.len()))
        .fold((0., 0, 0), |acc, subtitle_metric| async move {
            (
                acc.0 + subtitle_metric.0,
                acc.1 + subtitle_metric.1,
                acc.2 + 1,
            )
        })
        .await;

    let average_sentence_duration = total_sentence_duration as f64 / sentence_count as f64;
    let average_sentence_length = total_sentence_length as f64 / sentence_count as f64;

    (average_sentence_duration, average_sentence_length)
}

async fn get_channels_metrics(channels_url: HashSet<&str>) -> HashMap<String, Metrics> {
    let partial_channels_metrics = channels_url
        .iter()
        .map(|channel_url| async move { get_channel_information(channel_url).await })
        .collect::<FuturesUnordered<_>>()
        .filter_map(|info| async move { info.ok() });

    partial_channels_metrics
        .map(
            |(channel_url, (description, recent_videos_ids)): (
                String,
                (String, Vec<String>),
            )| async move {
                println!("{channel_url}: start getting metrics");
                let average_duration = get_video_metrics(recent_videos_ids.clone()).await;
                let (average_sentence_duration, average_sentence_length) =
                    get_sentence_metrics(recent_videos_ids).await;
                println!("{channel_url}: finish getting metrics");
                (
                    channel_url,
                    Metrics {
                        description,
                        average_duration,
                        average_sentence_duration,
                        average_sentence_length,
                    },
                )
            },
        )
        .collect::<FuturesUnordered<_>>()
        .await
        .collect::<HashMap<_, _>>()
        .await
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut channels_url = HashSet::from_iter([
    ]);

    let data_file_name = format!("data_{}.csv", Utc::now());

    let mut pre_length = channels_url.len();
    let mut try_count = 0;

    while !channels_url.is_empty() {
        println!("Reamining channel count: {}", channels_url.len());
        println!("Remaining channels {:#?}", channels_url);

        if try_count >= 3 {
            println!("Failed after retry");
            break;
        }

        let metrics_map = get_channels_metrics(channels_url.clone()).await;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&data_file_name)
            .await?;

        for (url, metrics) in metrics_map {
            let content = format!(
                "\"{}\",{},{},{},\"{}\"\n",
                url,
                metrics.average_sentence_duration,
                metrics.average_sentence_length,
                metrics.average_duration,
                metrics.description.replace('\n', "\\n"),
            );

            file.write_all(content.as_bytes()).await?;

            channels_url.remove(url.as_str());
        }

        if channels_url.len() == pre_length {
            try_count += 1;
        } else {
            try_count = 0;
        }

        pre_length = channels_url.len();
    }

    println!("Finished");

    Ok(())
}
