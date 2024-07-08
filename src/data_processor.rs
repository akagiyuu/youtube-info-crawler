use std::{
    collections::{HashMap, HashSet},
    fs,
    path::PathBuf,
    sync::Arc,
};

use anyhow::Result;
use futures::{stream::FuturesUnordered, StreamExt};
use rustube::{Id, Video, VideoFetcher};
use serde::Serialize;
use youtube_captions::{language_tags::LanguageTag, DigestScraper};

use crate::browser::{init_browser, wait_for_element, wait_for_elements, BROWSER};

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Metrics {
    pub url: String,
    pub description: Option<String>,
    #[serde(skip)]
    pub recent_videos_ids: Option<Vec<String>>,
    pub average_duration: Option<f64>,
    pub average_sentence_duration: Option<f64>,
    pub average_sentence_length: Option<f64>,
}

impl Metrics {
    pub fn new(url: String) -> Metrics {
        Metrics {
            url,
            description: None,
            recent_videos_ids: None,
            average_duration: None,
            average_sentence_duration: None,
            average_sentence_length: None,
        }
    }

    pub async fn fetch_description_and_video_ids(&mut self) -> Result<()> {
        let browser = unsafe { BROWSER.get_or_try_init(init_browser).await? };

        let videos_url = format!("{}/videos", self.url);

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
        println!("{}: finished getting video ids", self.url);

        let _ = page.close().await;

        self.description = Some(description);
        self.recent_videos_ids = Some(recent_videos_ids);

        Ok(())
    }

    pub async fn fetch_video_metrics(&mut self) -> Result<()> {
        let recent_videos_ids = self.recent_videos_ids.clone().ok_or(anyhow::anyhow!(
            "Call fetch_description_and_video_ids first",
        ))?;

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

        self.average_duration = Some(total_duration as f64 / video_count as f64);

        Ok(())
    }

    pub async fn fetch_sentence_metrics(&mut self) -> Result<()> {
        let recent_videos_ids = self.recent_videos_ids.clone().ok_or(anyhow::anyhow!(
            "Call fetch_description_and_video_ids first",
        ))?;

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

        self.average_sentence_duration =
            Some(total_sentence_duration as f64 / sentence_count as f64);
        self.average_sentence_length = Some(total_sentence_length as f64 / sentence_count as f64);
        Ok(())
    }

    pub async fn download(
        &self,
        output_dir: PathBuf,
        number_of_videos_to_download: u8,
    ) -> Result<()> {
        let recent_videos_ids = self.recent_videos_ids.clone().ok_or(anyhow::anyhow!(
            "Call fetch_description_and_video_ids first",
        ))?;

        let mut current_channel_out_dir = output_dir;
        current_channel_out_dir.push(&self.url);
        fs::create_dir_all(&current_channel_out_dir)?;

        recent_videos_ids
            .into_iter()
            .take(number_of_videos_to_download as usize)
            .map(|id| Id::from_string(id).unwrap())
            .map(|id| {
                let current_channel_out_dir = current_channel_out_dir.clone();
                async move {
                    println!("{}: downloading video", current_channel_out_dir.file_name().unwrap().to_str().unwrap());
                    let video_path = Video::from_id(id.clone())
                        .await
                        .unwrap()
                        .best_quality()
                        .unwrap()
                        .download_to_dir(&current_channel_out_dir)
                        .await?;

                    println!("{}: run ASD", current_channel_out_dir.file_name().unwrap().to_str().unwrap());
                    tokio::process::Command::new("python")
                        .current_dir("Light-ASD")
                        .args([
                            "Columbia_test.py",
                            "--videoName",
                            video_path.file_name().unwrap().to_str().unwrap(),
                            "--videoFolder",
                            current_channel_out_dir.to_str().unwrap(),
                        ])
                        .spawn()?
                        .wait()
                        .await?;

                    println!("{}: finished processing", current_channel_out_dir.file_name().unwrap().to_str().unwrap());

                    Ok(())
                }
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<Result<_>>>()
            .await;

        eprintln!("DEBUGPRINT[2]: data_processor.rs:184 (after .await;)");
        Ok(())
    }
}
