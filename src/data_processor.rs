use std::{fs, path::PathBuf};

use anyhow::{anyhow, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use rayon::iter::{IntoParallelRefIterator, ParallelBridge, ParallelIterator};
use serde::Serialize;
use youtube_captions::{language_tags::LanguageTag, DigestScraper};
use youtube_dl::YoutubeDl;

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Metrics {
    pub url: String,
    pub name: Option<String>,
    #[serde(skip)]
    pub video_ids: Option<Vec<String>>,
    pub average_duration: Option<f64>,
    pub average_sentence_duration: Option<f64>,
    pub average_sentence_length: Option<f64>,
}

impl Metrics {
    pub fn new(url: String) -> Metrics {
        Metrics {
            url,
            name: None,
            video_ids: None,
            average_duration: None,
            average_sentence_duration: None,
            average_sentence_length: None,
        }
    }

    pub async fn fetch_channel_infos(&mut self) -> Result<()> {
        self.name = YoutubeDl::new("https://www.youtube.com/@MonsterUP1")
            .extra_arg("--playlist-items")
            .extra_arg("0")
            .run_async()
            .await?
            .into_playlist()
            .unwrap()
            .uploader;

        Ok(())
    }

    pub async fn fetch_video_metrics(&mut self) -> Result<()> {
        println!("{}: start fetching video metrics", self.url);

        let data = YoutubeDl::new(format!("{}/videos", self.url))
            .extra_arg("--flat-playlist")
            .run_async()
            .await?
            .into_playlist()
            .ok_or(anyhow!("Expect playlist"))?
            .entries
            .ok_or(anyhow!("Expect videos"))?;

        let video_ids = data
            .par_iter()
            .map(|video| video.id.clone())
            .collect::<Vec<_>>();
        let video_count = video_ids.len() as f64;
        let total_duration: f64 = data
            .par_iter()
            .map(|video| video.duration.as_ref().unwrap().as_f64().unwrap())
            .sum();

        self.video_ids = Some(video_ids);
        self.average_duration = Some(total_duration / video_count);

        println!("{}: finished fetching video metrics", self.url);

        Ok(())
    }

    pub async fn fetch_sentence_metrics(&mut self) -> Result<()> {
        println!("{}: start fetching setence metrics", self.url);

        let recent_videos_ids = self
            .video_ids
            .clone()
            .ok_or(anyhow::anyhow!("Call fetch_video_metrics first",))?;

        let transcript = recent_videos_ids
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
            .collect::<FuturesUnordered<_>>()
            .await;

        let sentence_count = transcript.len();
        let (total_sentence_duration, total_sentence_length) = transcript
            .into_iter()
            .flatten()
            .par_bridge()
            .map(|subtitle| (subtitle.duration_secs, subtitle.value.len()))
            .reduce(
                || (0., 0),
                |acc, subtitle_metric| (acc.0 + subtitle_metric.0, acc.1 + subtitle_metric.1),
            );

        self.average_sentence_duration =
            Some(total_sentence_duration as f64 / sentence_count as f64);
        self.average_sentence_length = Some(total_sentence_length as f64 / sentence_count as f64);

        println!("{}: finished fetching setence metrics", self.url);

        Ok(())
    }

    pub async fn download(&self, output_dir: PathBuf) -> Result<()> {
        println!("{}: start download and processing videos", self.url);

        let mut current_channel_out_dir = output_dir;
        current_channel_out_dir.push(self.name.clone().unwrap());
        fs::create_dir_all(&current_channel_out_dir)?;

        YoutubeDl::new(&self.url)
            .download_to_async(&current_channel_out_dir)
            .await?;

        // fs::read_dir(&current_channel_out_dir)?
        //     .filter_map(|entry| entry.ok())
        //     .map(|entry| entry.path())
        //     .filter(|path| path.is_file())
        //     .map(|path| path.file_name().unwrap().to_str().unwrap().to_string())
        //     .map(|video_name| {
        //         let current_channel_out_dir = current_channel_out_dir.clone();
        //         async move {
        //             tokio::process::Command::new("python")
        //                 .current_dir("Light-ASD")
        //                 .args([
        //                     "Columbia_test.py",
        //                     "--videoName",
        //                     &video_name,
        //                     "--videoFolder",
        //                     current_channel_out_dir.to_str().unwrap(),
        //                 ])
        //                 .spawn()?
        //                 .wait()
        //                 .await?;
        //
        //             Ok(())
        //         }
        //     })
        //     .collect::<FuturesUnordered<_>>()
        //     .collect::<Vec<Result<_>>>()
        //     .await;

        println!("{}: finished download and processing videos", self.url);

        Ok(())
    }
}
