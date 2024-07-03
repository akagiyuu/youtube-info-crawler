#![feature(async_closure)]
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use anyhow::Result;
use chromiumoxide::{browser::Browser, BrowserConfig, Element, Page};
use chrono::Utc;
use futures::{stream::FuturesUnordered, StreamExt};
use rustube::{Id, VideoFetcher};
use serde::Serialize;
use tokio::{sync::OnceCell, time::sleep};
use youtube_captions::{language_tags::LanguageTag, DigestScraper};

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct Metrics {
    url: String,
    description: String,
    average_duration: f64,
    average_sentence_duration: f64,
    average_sentence_length: f64,
}

static mut BROWSER: OnceCell<Browser> = OnceCell::const_new();

async fn init_browser() -> Result<Browser> {
    let (browser, mut handler) = Browser::launch(BrowserConfig::builder().build().unwrap())
        .await
        .unwrap();

    tokio::task::spawn(async move {
        while let Some(h) = handler.next().await {
            if h.is_err() {
                break;
            }
        }
    });

    Ok(browser)
}

async fn wait_for_element(selector: &str, page: &Page) -> Element {
    loop {
        if let Ok(element) = page.find_element(selector).await {
            return element;
        }
        sleep(Duration::from_secs(1)).await
    }
}

async fn wait_for_elements(selector: &str, page: &Page) -> Vec<Element> {
    loop {
        if let Ok(elements) = page.find_elements(selector).await {
            return elements;
        }
        sleep(Duration::from_secs(1)).await
    }
}

async fn get_channel_information(channel_url: &str) -> Result<(String, (String, Vec<String>))> {
    let browser = unsafe { BROWSER.get_or_try_init(init_browser).await? };

    let videos_url = format!("{}/videos", channel_url);

    let page = browser.new_page(&videos_url).await?;

    wait_for_element("yt-description-preview-view-model", &page)
        .await
        .click()
        .await?;

    let description = wait_for_element("#description-container > span:nth-child(1)", &page)
        .await
        .inner_text()
        .await?
        .unwrap();

    let video_elements = wait_for_elements("ytd-rich-item-renderer > div:nth-child(1) > ytd-rich-grid-media:nth-child(1) > div:nth-child(1) > div:nth-child(3) > div:nth-child(2) > h3:nth-child(1) > a:nth-child(2)", &page).await;
    let recent_videos_ids = video_elements
        .into_iter()
        .map(|element| async move { element.attribute("href").await.unwrap().unwrap() })
        .collect::<FuturesUnordered<_>>()
        .map(|relative_link| format!("https://www.youtube.com/{relative_link}"))
        .map(|link| link.split_once('=').unwrap().1.to_string())
        .collect::<Vec<_>>()
        .await;
    println!("{}: finished getting video ids", channel_url);

    let _ = page.close().await;

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
    channels_url
        .iter()
        .map(|channel_url| async move {
            let (channel_url, (description, recent_videos_ids)) =
                match get_channel_information(channel_url).await {
                    Ok(value) => value,
                    Err(_) => return None,
                };

            println!("{channel_url}: start getting metrics");
            let average_duration = get_video_metrics(recent_videos_ids.clone()).await;
            let (average_sentence_duration, average_sentence_length) =
                get_sentence_metrics(recent_videos_ids).await;
            println!("{channel_url}: finish getting metrics");

            let data = (
                channel_url.clone(),
                Metrics {
                    url: channel_url,
                    description,
                    average_duration,
                    average_sentence_duration,
                    average_sentence_length,
                },
            );

            Some(data)
        })
        .collect::<FuturesUnordered<_>>()
        .filter_map(|data| async move { data })
        .collect()
        .await
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut channels_url = HashSet::from_iter([]);

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
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&data_file_name)?;

        let mut csv_writer = csv::Writer::from_writer(file);

        for (url, metrics) in metrics_map {
            csv_writer.serialize(metrics)?;

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

    unsafe {
        BROWSER.get_mut().unwrap().close().await.unwrap();
    }

    Ok(())
}
