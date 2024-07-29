use std::str::FromStr as _;

use anyhow::{Context as _, Result};
use derive_more::Add;
use futures::{stream::FuturesUnordered, StreamExt as _};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelBridge, ParallelIterator};
use serde::Serialize;
use youtube_dl::{SingleVideo, Subtitle, YoutubeDl};

const LANGUAGE: &str = "vi";
const BATCH_SIZE: usize = 100;
const MAX_VIDEO_COUNT: usize = 5000;

async fn fetch_videos_data(link: &str, start: usize, end: usize) -> Result<Vec<SingleVideo>> {
    let ytdlp_output = YoutubeDl::new(format!("{}/videos", link))
        .extra_arg("--write-auto-subs")
        .ignore_errors(true)
        .extra_arg("--playlist-start")
        .extra_arg(start.to_string())
        .extra_arg("--playlist-end")
        .extra_arg(end.to_string())
        .run_async()
        .await?;

    let all_videos = ytdlp_output
        .into_playlist()
        .and_then(|playlist| playlist.entries)
        .context("Expect channel to contain at least 1 videos")?;

    Ok(all_videos)
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

#[derive(Default, Add)]
struct PartialVideoListMetrics {
    video_count: usize,
    total_duration: f64,
    sentence_count: usize,
    total_sentence_length: usize,
    total_sentence_duration: f64,
}

impl PartialVideoListMetrics {
    #[tracing::instrument(err)]
    async fn new(link: String, start: usize, end: usize) -> Result<Self> {
        let data = fetch_videos_data(&link, start, end).await?;

        let subtitle_links = data
            .par_iter()
            .map(get_subtitles)
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

        Ok(Self {
            video_count,
            total_duration,
            sentence_count,
            total_sentence_length,
            total_sentence_duration,
        })
    }
}

#[derive(Debug, Serialize)]
pub struct VideoListMetrics {
    pub video_count: usize,
    pub average_duration: f64,
    pub average_sentence_duration: f64,
    pub average_sentence_length: f64,
}

impl From<PartialVideoListMetrics> for VideoListMetrics {
    fn from(partial: PartialVideoListMetrics) -> Self {
        let PartialVideoListMetrics {
            video_count,
            total_duration,
            sentence_count,
            total_sentence_length,
            total_sentence_duration,
        } = partial;

        Self {
            video_count,
            average_duration: total_duration / video_count as f64,
            average_sentence_length: total_sentence_length as f64 / sentence_count as f64,
            average_sentence_duration: total_sentence_duration / sentence_count as f64,
        }
    }
}

impl VideoListMetrics {
    pub async fn new(link: String) -> Self {
        let video_list_metrics_raw = (0..MAX_VIDEO_COUNT / BATCH_SIZE)
            .map(|i| i * BATCH_SIZE)
            .map(|start| (start + 1, start + BATCH_SIZE))
            .map(|(start, end)| {
                let link = link.clone();
                PartialVideoListMetrics::new(link, start, end)
            })
            .collect::<FuturesUnordered<_>>()
            .filter_map(|x| async move { x.ok() })
            .fold(PartialVideoListMetrics::default(), |acc, x| async move {
                acc + x
            })
            .await;

        video_list_metrics_raw.into()
    }
}
