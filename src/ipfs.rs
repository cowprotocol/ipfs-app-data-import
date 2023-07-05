use crate::AppDataHash;
use {
    anyhow::{anyhow, Context, Result},
    reqwest::{Client, StatusCode, Url},
};

pub struct Ipfs {
    client: Client,
    base: Url,
    query: Option<String>,
}

impl Ipfs {
    pub fn new(client: Client, base: Url, query: Option<String>) -> Self {
        assert!(!base.cannot_be_a_base());
        Self {
            client,
            base,
            query,
        }
    }

    /// IPFS gateway behavior when a CID cannot be found is inconsistent and can
    /// be confusing:
    ///
    /// - The public ipfs.io gateway responds "504 Gateway Timeout" after 2
    ///   minutes.
    /// - The public cloudflare gateway responds "524" after 20 seconds.
    /// - A private Pinata gateway responds "404 Not Found" after 2 minutes.
    ///
    /// This function treats all status codes except "200 OK" as errors and you
    /// likely want to use it with a timeout.
    pub async fn fetch(&self, cid: &str) -> Result<Vec<u8>> {
        let url = self.prepare_url(cid);
        let response = self.client.get(url).send().await.context("send")?;
        let status = response.status();
        let body = response.bytes().await.context("body")?;
        match status {
            StatusCode::OK => Ok(body.into()),
            _ => {
                let body_text = String::from_utf8_lossy(&body);
                let body_text: &str = &body_text;
                Err(anyhow!("status {status}, body {body_text:?}"))
            }
        }
    }

    fn prepare_url(&self, cid: &str) -> Url {
        let mut url = self.base.clone();
        let mut segments = url.path_segments_mut().unwrap();
        segments.push("ipfs");
        segments.push(cid);
        std::mem::drop(segments);
        if let Some(query) = &self.query {
            url.set_query(Some(query.as_str()));
        }
        url
    }
}

pub fn old_app_data_cid(contract_app_data: &AppDataHash) -> String {
    let mut raw_cid = [0u8; 4 + 32];
    raw_cid[0] = 1; // cid version
    raw_cid[1] = 0x70; // dag-pb
    raw_cid[2] = 0x12; // sha2-256
    raw_cid[3] = 32; // hash length
    raw_cid[4..].copy_from_slice(contract_app_data);
    multibase::encode(multibase::Base::Base32Lower, raw_cid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn public_gateway() {
        let ipfs = Ipfs::new(Default::default(), "https://ipfs.io".parse().unwrap(), None);
        let cid = "Qma4Dwke5h8mgJyZMDRvKqM3RF7c6Mxcj3fR4um9UGaNF6";
        let content = ipfs.fetch(cid).await.unwrap();
        let content = std::str::from_utf8(&content).unwrap();
        println!("{content}");
    }

    #[tokio::test]
    #[ignore]
    async fn private_gateway() {
        let url = std::env::var("url").unwrap();
        let query = std::env::var("query").unwrap();
        let ipfs = Ipfs::new(Default::default(), url.parse().unwrap(), Some(query));
        let cid = "Qma4Dwke5h8mgJyZMDRvKqM3RF7c6Mxcj3fR4um9UGaNF6";
        let content = ipfs.fetch(cid).await.unwrap();
        let content = std::str::from_utf8(&content).unwrap();
        println!("{content}");
    }
}
