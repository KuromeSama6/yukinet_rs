use log::{debug, info};
use tokio::fs;
use std::collections::HashMap;
use std::path::Path;
use std::sync::OnceLock;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::{BufReader, BufWriter};
use tokio::sync::RwLock;
use tokio::time::Instant;
use crate::asyncutil;
use crate::util::Sha256Sum;

pub type ChecksumMap = HashMap<String, Sha256Sum>;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Resource {
    pub path: String,
    pub checksum: Sha256Sum,

    pub fingerprint: Fingerprint,
}

impl Resource {
    pub async fn read_buf(&self) -> anyhow::Result<BufReader<File>> {
        let path = Path::new("resources").join(&self.path);

        let file = File::open(path).await?;
        Ok(BufReader::new(file))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Fingerprint {
    pub size: u64,
    pub modified: u64,
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

    let state = CommonState {
        resources: RwLock::new(HashMap::new())
    };
    COMMON_STATE.set(state).unwrap();

    rebuild_resources().await?;

    Ok(())
}

pub async fn rebuild_resources() -> anyhow::Result<()> {
    info!("Rebuilding resources...");
    let sw = Instant::now();
    let base_path = Path::new("resources");
    let fingerprints = &read_fingerprint_cache().await?;
    let resource_map = map_resources(base_path, fingerprints).await?;
    write_fingerprint_cache(&resource_map).await?;

    let len = resource_map.len();

    let mut write = COMMON_STATE.get().unwrap().resources.write().await;
    *write = resource_map;

    info!("Resources mapped in {} seconds with {} resources.", &sw.elapsed().as_secs_f32(), len);

    Ok(())
}

pub async fn checksum_map() -> ChecksumMap {
    let resources = COMMON_STATE.get().unwrap().resources.read().await;
    let mut ret = HashMap::new();

    for (path, resource) in resources.iter() {
        ret.insert(path.clone(), resource.checksum.clone());
    }

    ret
}

pub async fn diff_checksums(checksums: &ChecksumMap) -> Vec<String> {
    let mut ret = Vec::new();
    let resources = COMMON_STATE.get().unwrap().resources.read().await;

    for (path, checksum) in checksums.iter() {
        if let Some(resource) = resources.get(path) {
            if &resource.checksum != checksum {
                ret.push(path.clone());
            }
        } else {
            ret.push(path.clone());
        }
    }

    ret
}

pub async fn get_resource(path: &str) -> Option<Resource> {
    let resources = COMMON_STATE.get().unwrap().resources.read().await;

    if let Some(resource) = resources.get(path) {
        Some(resource.clone())
    } else {
        None
    }
}

pub async fn open_write(path: String) -> anyhow::Result<BufWriter<File>> {
    let full_path = Path::new("resources").join(&path);
    if let Some(parent) = full_path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let file = File::create(full_path).await?;

    Ok(BufWriter::new(file))
}

#[async_recursion::async_recursion]
async fn map_resources(base_path: &Path, fingerprints: &HashMap<String, Resource>) -> anyhow::Result<HashMap<String, Resource>> {
    let mut read_dir = fs::read_dir(base_path).await?;
    let mut ret = HashMap::new();

    while let Some(entry) = read_dir.next_entry().await? {
        let file_type = entry.file_type().await?;
        if file_type.is_dir() {
            let path = entry.path();
            let child_map = map_resources(&path, &fingerprints).await?;
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

async fn read_fingerprint_cache() -> anyhow::Result<HashMap<String, Resource>> {
    let path = "cache/resource_fingerprints.json";

    let exists = fs::metadata(path).await.is_ok();
    if !exists {
        return Ok(HashMap::new());
    }

    let data = fs::read_to_string(path).await?;
    let resources: HashMap<String, Resource> = serde_json::from_str(&data)?;
    Ok(resources)
}

async fn write_fingerprint_cache(resources: &HashMap<String, Resource>) -> anyhow::Result<()> {
    let serialized = serde_json::to_string(resources)?;
    fs::write("cache/resource_fingerprints.json", serialized).await?;
    Ok(())
}