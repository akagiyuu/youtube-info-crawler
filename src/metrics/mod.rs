mod channel_metrics;
mod video_list_metrics;

use std::path::PathBuf;

use anyhow::Result;
use serde::Serialize;

use channel_metrics::ChannelMetrics;
use video_list_metrics::VideoListMetrics;

#[derive(Debug, Serialize)]
pub struct Metrics {
    #[serde(flatten)]
    pub channel_metrics: ChannelMetrics,
    #[serde(flatten)]
    pub video_list_metrics: VideoListMetrics,
}

impl Metrics {
    pub async fn new(link: String) -> Result<Self> {
        let channel_metrics = ChannelMetrics::new(link.clone()).await?;
        let video_list_metrics = VideoListMetrics::new(link).await;

        Ok(Metrics {
            channel_metrics,
            video_list_metrics,
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
