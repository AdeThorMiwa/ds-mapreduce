use super::{
    cluster::Cluster,
    node::{Node, NodeEvent},
};
use futures::Future;
use kube::Client;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct NodeManager {
    cluster: Arc<Mutex<Cluster>>,
}

impl Clone for NodeManager {
    fn clone(&self) -> Self {
        Self {
            cluster: self.cluster.clone(),
        }
    }
}

#[derive(Debug)]
pub enum NodeManagerError {
    CreateClientError,
}

impl NodeManager {
    pub async fn try_default() -> anyhow::Result<Self, NodeManagerError> {
        let client = Client::try_default()
            .await
            .map_err(|_| NodeManagerError::CreateClientError)?;

        let cluster = Arc::new(Mutex::new(Cluster::new(client)));

        Ok(Self { cluster })
    }

    pub async fn new_node(&mut self, image: &str, replicas: i32) -> usize {
        let mut cluster = self.cluster.lock().await;
        let client = cluster.get_client();
        let node_id = cluster.get_next_node_id();
        let node = Arc::new(Mutex::new(Node::new(
            node_id,
            image,
            cluster.name(),
            replicas,
            client,
        )));
        cluster.add_node(node_id, node);
        node_id
    }

    pub async fn watch_node<F, R>(&self, node_id: usize, f: F)
    where
        F: Send + Clone + 'static,
        F: FnOnce(NodeEvent) -> R,
        R: Future<Output = ()> + Send,
    {
        let cluster = self.cluster.lock().await;
        if let Some(node) = cluster.get_node(node_id) {
            let mut node = node.lock().await;
            node.watch(f).await;
        }
    }

    pub async fn send(&self, _node_id: usize) {
        todo!("figure out a way to send message to node")
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        let cluster = self.cluster.lock().await;
        if cluster.has_existing_version().await? {
            cluster.clean_old_versions().await?;
        }

        cluster.create_node_definitions().await?;
        cluster.boot_nodes().await?;

        'keep_alive: loop {
            if !cluster.has_running_node().await {
                break 'keep_alive;
            }
        }

        Ok(())
    }
}
