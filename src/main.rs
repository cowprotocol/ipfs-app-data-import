mod ipfs;
mod postgres;

use crate::ipfs::old_app_data_cid;
use anyhow::{Context, Result};
use futures::StreamExt;
use ipfs::Ipfs;
use postgres::Postgres;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

type AppDataHash = [u8; 32];

#[tokio::main]
async fn main() {
    main_().await;
}

async fn main_() {
    let postgres_url = std::env::var("postgres_url").unwrap();
    let ipfs_url = std::env::var("ipfs_url").unwrap();
    let ipfs_auth = std::env::var("ipfs_auth").unwrap();

    let postgres = Postgres::new(&postgres_url).await.unwrap();

    let reqwest_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .unwrap();
    let ipfs = Ipfs::new(reqwest_client, ipfs_url.parse().unwrap(), Some(ipfs_auth));

    println!("Fetching all missing app data hashes.");
    let app_data_hashes = postgres.app_data_without_full().await.unwrap();
    println!("Done. Have {} total.", app_data_hashes.len());

    let ipfs = &ipfs;
    let postgres = &postgres;
    let inserts: AtomicUsize = AtomicUsize::new(0);
    let inserts = &inserts;
    let handle_one = |(i, app_data_hash)| async move {
        let hex = hex::encode(app_data_hash);
        match handle_one(ipfs, postgres, &app_data_hash).await {
            Ok(()) => {
                inserts.fetch_add(1, Ordering::SeqCst);
                println!("ok {i} {hex}")
            }
            Err(err) => {
                println!("err {i} {hex} {err:?}");
            }
        }
    };
    futures::stream::iter(app_data_hashes.into_iter().enumerate())
        .for_each_concurrent(32, handle_one)
        .await;
    println!(
        "Completed with {} successful insertions.",
        inserts.load(Ordering::SeqCst)
    );
}

async fn handle_one(ipfs: &Ipfs, postgres: &Postgres, app_data_hash: &AppDataHash) -> Result<()> {
    let cid = old_app_data_cid(app_data_hash);
    let full = ipfs.fetch(&cid).await.context("ipfs fetch")?;
    postgres
        .insert(app_data_hash, &full)
        .await
        .context("insert")
}
