mod channel_metrics;
mod video_list_metrics;

use std::path::PathBuf;

use anyhow::Result;
use serde::Serialize;

use channel_metrics::ChannelMetrics;
use video_list_metrics::VideoListMetrics;

#[derive(Debug, Serialize)]
pub struct Metrics {
    pub link: String,
    pub channel_name: String,

    pub video_count: usize,
    pub average_duration: f64,
    pub average_sentence_duration: f64,
    pub average_sentence_length: f64,
}

impl Metrics {
    #[tracing::instrument(err)]
    pub async fn new(link: String) -> Result<Self> {
        let ChannelMetrics { link, name } = ChannelMetrics::new(link).await?;
        let VideoListMetrics {
            video_count,
            average_duration,
            average_sentence_duration,
            average_sentence_length,
        } = VideoListMetrics::new(link.clone()).await;

        Ok(Metrics {
            link,
            channel_name: name,
            video_count,
            average_duration,
            average_sentence_duration,
            average_sentence_length,
        })
    }

    pub async fn download(&self, _output_dir: PathBuf) -> Result<()> {
        todo!()
        // tracing::info!("{}: start download and processing videos", self.link);
        //
        // let mut current_channel_out_dir = output_dir;
        // current_channel_out_dir.push(self.name.clone());
        // fs::create_dir_all(&current_channel_out_dir)?;
        //
        // YoutubeDl::new(&self.link)
        //     .download_to_async(&current_channel_out_dir)
        //     .await?;
        //
        // // fs::read_dir(&current_channel_out_dir)?
        // //     .filter_map(|entry| entry.ok())
        // //     .map(|entry| entry.path())
        // //     .filter(|path| path.is_file())
        // //     .map(|path| path.file_name().unwrap().to_str().unwrap().to_string())
        // //     .map(|video_name| {
        // //         let current_channel_out_dir = current_channel_out_dir.clone();
        // //         async move {
        // //             tokio::process::Command::new("python")
        // //                 .current_dir("Light-ASD")
        // //                 .args([
        // //                     "Columbia_test.py",
        // //                     "--videoName",
        // //                     &video_name,
        // //                     "--videoFolder",
        // //                     current_channel_out_dir.to_str().unwrap(),
        // //                 ])
        // //                 .spawn()?
        // //                 .wait()
        // //                 .await?;
        // //
        // //             Ok(())
        // //         }
        // //     })
        // //     .collect::<FuturesUnordered<_>>()
        // //     .collect::<Vec<Result<_>>>()
        // //     .await;
        //
        // tracing::info!("{}: finished download and processing videos", self.link);
        //
        // Ok(())
    }
}
