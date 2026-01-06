use std::path::PathBuf;
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::AsyncReadExt;

pub async fn sha256_sum(path: &PathBuf) -> anyhow::Result<[u8;32]> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();

    let mut buffer = [0u8; 8192];
    loop {
        let n = file.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }

    let result = hasher.finalize();
    Ok(result.into())
}