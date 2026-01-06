use log::{debug, info};
use tokio::fs;
use std::collections::HashMap;
use std::path::Path;
use std::sync::OnceLock;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio::time::Instant;
use crate::asyncutil;
use crate::util::Sha256Sum;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Resource {
    pub path: String,
    pub checksum: Sha256Sum,

    fingerprint: Fingerprint,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Fingerprint {
    size: u64,
    modified: u64,
}

impl Fingerprint {
    pub async fn new(path: &Path) -> anyhow::Result<Self> {
        let metadata = fs::metadata(path).await?;
        let modified = metadata.modified()?
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();

        Ok(Fingerprint {
            size: metadata.len(),
            modified,
        })
    }
}

impl PartialEq for Fingerprint {
    fn eq(&self, other: &Self) -> bool {
        self.size == other.size && self.modified == other.modified
    }
}
#[derive(Debug)]
struct CommonState {
    resources: RwLock<HashMap<String, Resource>>,
}

impl CommonState {
    fn new() -> Self {
        CommonState {
            resources: RwLock::new(HashMap::new()),
        }
    }
}

static COMMON_STATE: OnceLock<CommonState> = OnceLock::new();

pub async fn init() -> anyhow::Result<()> {
    fs::create_dir_all("resources").await?;

    info!("Creating resource map...");
    let sw = Instant::now();
    let base_path = Path::new("resources");
    let fingerprints = &common_read_fingerprint_cache().await?;
    let resource_map = common_map_resources(base_path, fingerprints).await?;
    common_write_fingerprint_cache(&resource_map).await?;

    info!("Resources mapped in {} seconds with {} resources.", &sw.elapsed().as_secs_f32(), &resource_map.len());

    COMMON_STATE.set(CommonState {
        resources: RwLock::new(resource_map)
    }).unwrap();

    Ok(())
}

#[async_recursion::async_recursion]
async fn common_map_resources(base_path: &Path, fingerprints: &HashMap<String, Resource>) -> anyhow::Result<HashMap<String, Resource>> {
    let mut read_dir = fs::read_dir(base_path).await?;
    let mut ret = HashMap::new();

    while let Some(entry) = read_dir.next_entry().await? {
        let file_type = entry.file_type().await?;
        if file_type.is_dir() {
            let path = entry.path();
            let child_map = common_map_resources(&path, &fingerprints).await?;
            ret.extend(child_map);
            continue;
        }

        let full_path = entry.path();
        let mut relative_path = full_path.strip_prefix("resources")?.to_string_lossy().to_string();
        // use forward slashes for resource paths
        relative_path = relative_path.replace("\\", "/");

        if let Some(res) = fingerprints.get(&relative_path) {
            if res.fingerprint == Fingerprint::new(&full_path).await? {
                ret.insert(res.path.clone(), res.clone());
                continue;
            } else {
                info!("Resource '{}' has changed, recalculating checksum.", &relative_path);
            }
        } else {
            info!("Resource '{}' is new, calculating checksum.", &relative_path);
        }

        let checksum = asyncutil::sha256_sum(&full_path).await?;

        let resource = Resource {
            path: relative_path,
            checksum,
            fingerprint: Fingerprint::new(&full_path).await?,
        };

        ret.insert(resource.path.clone(), resource);
    }

    Ok(ret)
}

async fn common_read_fingerprint_cache() -> anyhow::Result<HashMap<String, Resource>> {
    let path = "cache/resource_fingerprints.json";

    let exists = fs::metadata(path).await.is_ok();
    if !exists {
        return Ok(HashMap::new());
    }

    let data = fs::read_to_string(path).await?;
    let resources: HashMap<String, Resource> = serde_json::from_str(&data)?;
    Ok(resources)
}

async fn common_write_fingerprint_cache(resources: &HashMap<String, Resource>) -> anyhow::Result<()> {
    let serialized = serde_json::to_string(resources)?;
    fs::write("cache/resource_fingerprints.json", serialized).await?;
    Ok(())
}