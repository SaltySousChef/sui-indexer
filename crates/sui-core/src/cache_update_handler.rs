use dashmap::DashSet;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, OnceLock,
};
use sui_types::base_types::ObjectID;
use sui_types::object::Object;
use tokio::io::AsyncWriteExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;

use tracing::{error, info, warn};

const SOCKET_PATH: &str = "/tmp/sui_cache_updates.sock";

pub fn pool_related_ids_path() -> &'static str {
    static PATH: OnceLock<String> = OnceLock::new();
    PATH.get_or_init(|| {
        std::env::var("SUI_POOL_RELATED_IDS_PATH")
            .unwrap_or_else(|_| "/var/lib/sui/pool_related_ids.txt".to_string())
    })
}

pub fn pool_related_object_ids() -> DashSet<ObjectID> {
    let path = pool_related_ids_path();
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) => {
            warn!("Pool related IDs file not found at {}: {}. Starting with empty set.", path, e);
            return DashSet::new();
        }
    };

    let set = DashSet::new();
    for line in content.trim().lines() {
        match line.parse() {
            Ok(id) => {
                set.insert(id);
            }
            Err(e) => {
                warn!("Failed to parse pool ID '{}': {}", line, e);
            }
        }
    }
    set
}

#[derive(Debug)]
pub struct CacheUpdateHandler {
    socket_path: Arc<PathBuf>,
    connections: Arc<Mutex<Vec<UnixStream>>>,
    running: Arc<AtomicBool>,
}

impl Clone for CacheUpdateHandler {
    fn clone(&self) -> Self {
        Self {
            socket_path: Arc::clone(&self.socket_path),
            connections: Arc::clone(&self.connections),
            running: Arc::clone(&self.running),
        }
    }
}

impl CacheUpdateHandler {
    pub fn new() -> Self {
        // info!("CacheUpdateHandler::new() called, creating socket at {}", SOCKET_PATH);
        let socket_path = Arc::new(PathBuf::from(SOCKET_PATH));
        // Remove existing socket file if it exists
        let _ = std::fs::remove_file(socket_path.as_ref());

        let listener = UnixListener::bind(socket_path.as_ref()).expect("Failed to bind Unix socket");
        // info!("CacheUpdateHandler: Unix socket bound successfully at {}", SOCKET_PATH);

        let connections = Arc::new(Mutex::new(Vec::new()));
        let running = Arc::new(AtomicBool::new(true));

        let connections_clone = Arc::clone(&connections);
        let running_clone = Arc::clone(&running);

        // Spawn connection acceptor task
        tokio::spawn(async move {
            while running_clone.load(Ordering::SeqCst) {
                match listener.accept().await {
                    Ok((stream, _addr)) => {
                        info!("New client connected to cache update socket");
                        let mut connections = connections_clone.lock().await;
                        connections.push(stream);
                    }
                    Err(e) => {
                        error!("Error accepting connection: {}", e);
                        // Optionally, decide whether to break the loop or continue
                    }
                }
            }
        });

        Self {
            socket_path,
            connections,
            running,
        }
    }

    pub async fn notify_written(&self, objects: Vec<(ObjectID, Object)>) {
        let serialized = bcs::to_bytes(&objects).expect("serialization error");
        let len = serialized.len() as u32;
        let len_bytes = len.to_le_bytes();

        let mut connections = self.connections.lock().await;

        // Iterate over connections and remove any that fail
        let mut i = 0;
        while i < connections.len() {
            let stream = &mut connections[i];

            // Attempt to write to the stream
            let result = async {
                if let Err(e) = stream.write_all(&len_bytes).await {
                    error!("Error writing length prefix to client: {}", e);
                    Err(e)
                } else if let Err(e) = stream.write_all(&serialized).await {
                    error!("Error writing to client: {}", e);
                    Err(e)
                } else {
                    Ok(())
                }
            }
            .await;

            // Remove connection if there was an error
            if result.is_err() {
                connections.remove(i);
            } else {
                i += 1;
            }
        }
    }
}

impl Default for CacheUpdateHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for CacheUpdateHandler {
    fn drop(&mut self) {
        // Only clean up when this is the last reference
        if Arc::strong_count(&self.socket_path) == 1 {
            info!("CacheUpdateHandler::drop() called (last reference), removing socket at {:?}", self.socket_path);
            self.running.store(false, Ordering::SeqCst);
            let _ = std::fs::remove_file(self.socket_path.as_ref());
        }
    }
}