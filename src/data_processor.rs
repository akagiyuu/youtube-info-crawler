use std::{fs, path::PathBuf, str::FromStr};

use anyhow::{anyhow, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use rayon::iter::{
    IntoParallelIterator, IntoParallelRefIterator, ParallelBridge, ParallelIterator,
};
use serde::Serialize;
use youtube_dl::{SingleVideo, Subtitle, YoutubeDl};

const LANGUAGE: &str = "vi";
const BATCH_SIZE: usize = 100;
const MAX_VIDEO_COUNT: usize = 5000;

#[derive(Debug, Serialize)]
pub struct Metrics {
    pub link: String,
    pub name: String,
    pub video_count: usize,
    pub average_duration: f64,
    pub average_sentence_duration: f64,
    pub average_sentence_length: f64,
}

impl Metrics {
    #[tracing::instrument(err)]
    async fn fetch_channel_infos(link: &str) -> Result<String> {
        let channel_name = YoutubeDl::new(link)
            .extra_arg("--playlist-items")
            .extra_arg("0")
            .run_async()
            .await?
            .into_playlist()
            .unwrap()
            .uploader
            .ok_or(anyhow!("No channel name found"))?;

        Ok(channel_name)
    }

    async fn fetch_videos_data(link: &str, start: usize, end: usize) -> Result<Vec<SingleVideo>> {
        YoutubeDl::new(format!("{}/videos", link))
            .extra_arg("--write-auto-subs")
            .ignore_errors(true)
            .extra_arg("--playlist-start")
            .extra_arg(start.to_string())
            .extra_arg("--playlist-end")
            .extra_arg(end.to_string())
            .run_async()
            .await?
            .into_playlist()
            .ok_or(anyhow!("Expect playlist"))?
            .entries
            .ok_or(anyhow!("Expect videos"))
    }

    fn get_subtitles(video: &SingleVideo) -> Option<Vec<Subtitle>> {
        match video
            .subtitles
            .clone()
            .and_then(|subtitles_list| subtitles_list.get(LANGUAGE).and_then(|x| x.clone()))
        {
            Some(subtitles) => Some(subtitles),
            None => video
                .automatic_captions
                .clone()
                .and_then(|subtitles| subtitles.get(LANGUAGE).cloned()),
        }
    }

    #[tracing::instrument(err)]
    async fn fetch_videos_metrics(
        link: String,
        start: usize,
        end: usize,
    ) -> Result<Option<(usize, f64, usize, usize, f64)>> {
        let data = Metrics::fetch_videos_data(&link, start, end).await?;

        if data.is_empty() {
            return Ok(None);
        }

        let subtitle_links = data
            .par_iter()
            .map(Metrics::get_subtitles)
            .filter_map(|subtitles| subtitles)
            .map(|subtitles| subtitles.into_par_iter())
            .flatten()
            .filter(|subtitle| subtitle.ext == Some("srv1".to_string()))
            .filter_map(|subtitle| subtitle.url)
            .collect::<Vec<_>>();

        let video_count = data.len();
        let total_duration: f64 = data
            .par_iter()
            .map(|video| video.duration.as_ref().unwrap().as_f64().unwrap())
            .sum();
        let (sentence_count, total_sentence_length, total_sentence_duration) = subtitle_links
            .iter()
            .map(|link| async move {
                let response = reqwest::get(link).await?;
                let subtitle_raw = response.text().await?;
                let subtitle = youtube_captions::format::srv1::Transcript::from_str(&subtitle_raw)?;

                Ok(subtitle
                    .into_iter()
                    .par_bridge()
                    .map(|entry| (entry.value.len(), entry.duration_secs as f64))
                    .fold(
                        || (0usize, 0, 0.),
                        |acc, (length, duration)| (acc.0 + 1, acc.1 + length, acc.2 + duration),
                    )
                    .reduce(
                        || (0, 0, 0.),
                        |acc, x| (acc.0 + x.0, acc.1 + x.1, acc.2 + x.2),
                    ))
            })
            .collect::<FuturesUnordered<_>>()
            .filter_map(|x: Result<_>| async move { x.ok() })
            .fold((0, 0, 0.), |acc, x| async move {
                (acc.0 + x.0, acc.1 + x.1, acc.2 + x.2)
            })
            .await;

        Ok(Some((
            video_count,
            total_duration,
            sentence_count,
            total_sentence_length,
            total_sentence_duration,
        )))
    }

    pub async fn new(link: String) -> Result<Metrics> {
        let name = Metrics::fetch_channel_infos(&link).await?;

        tracing::info!("{}: start fetching video metrics", link);

        let (
            video_count,
            total_duration,
            sentence_count,
            total_sentence_length,
            total_sentence_duration,
        ) = (0..MAX_VIDEO_COUNT / BATCH_SIZE)
            .map(|i| i * BATCH_SIZE)
            .map(|start| (start + 1, start + BATCH_SIZE))
            .map(|(start, end)| {
                let link = link.clone();
                async move { Metrics::fetch_videos_metrics(link, start, end).await }
            })
            .collect::<FuturesUnordered<_>>()
            .filter_map(|x| async move { x.ok() })
            .filter_map(|x| async move { x })
            .fold((0, 0., 0, 0, 0.), |acc, x| async move {
                (
                    acc.0 + x.0,
                    acc.1 + x.1,
                    acc.2 + x.2,
                    acc.3 + x.3,
                    acc.4 + x.4,
                )
            })
            .await;

        tracing::info!("{}: finished fetching video metrics", link);

        Ok(Metrics {
            link,
            name,
            video_count,
            average_duration: total_duration / video_count as f64,
            average_sentence_length: total_sentence_length as f64 / sentence_count as f64,
            average_sentence_duration: total_sentence_duration / sentence_count as f64,
        })
    }

    pub async fn download(&self, output_dir: PathBuf) -> Result<()> {
        tracing::info!("{}: start download and processing videos", self.link);

        let mut current_channel_out_dir = output_dir;
        current_channel_out_dir.push(self.name.clone());
        fs::create_dir_all(&current_channel_out_dir)?;

        YoutubeDl::new(&self.link)
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

        tracing::info!("{}: finished download and processing videos", self.link);

        Ok(())
    }
}
