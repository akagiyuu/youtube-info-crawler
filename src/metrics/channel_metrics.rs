use anyhow::{Context as _, Result};
use serde::Serialize;
use youtube_dl::YoutubeDl;

#[derive(Debug, Serialize)]
pub struct ChannelMetrics {
    pub link: String,
    pub name: String,
}

impl ChannelMetrics {
    pub async fn new(link: String) -> Result<Self> {
        let ytdlp_output = YoutubeDl::new(&link)
            .extra_arg("--playlist-items")
            .extra_arg("0")
            .run_async()
            .await?;
        let name = ytdlp_output
            .into_playlist()
            .context("Expect channel to contain at least 1 videos")?
            .uploader
            .context("No channel name found")?;

        Ok(Self { link, name })
    }
}
