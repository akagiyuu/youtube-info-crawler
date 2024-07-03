use std::time::Duration;

use anyhow::Result;
use chromiumoxide::{browser::Browser, BrowserConfig, Element, Page};
use futures::StreamExt;
use tokio::{sync::OnceCell, time::sleep};

pub static mut BROWSER: OnceCell<Browser> = OnceCell::const_new();

pub async fn init_browser() -> Result<Browser> {
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

pub async fn wait_for_element(selector: &str, page: &Page) -> Element {
    loop {
        if let Ok(element) = page.find_element(selector).await {
            return element;
        }
        sleep(Duration::from_secs(1)).await
    }
}

pub async fn wait_for_elements(selector: &str, page: &Page) -> Vec<Element> {
    loop {
        if let Ok(elements) = page.find_elements(selector).await {
            return elements;
        }
        sleep(Duration::from_secs(1)).await
    }
}

pub async unsafe fn close() -> Result<()> {
    BROWSER.get_mut().unwrap().close().await?;

    Ok(())
}
