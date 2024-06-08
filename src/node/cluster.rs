use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::{
    api::{ListParams, PostParams, ResourceExt},
    Api, Client, CustomResourceExt,
};
use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
};
use tokio::sync::Mutex;

use crate::node::node::ComputeServer;

use super::{
    node::{Node, NodeStatus},
    utils::cluster::cluster_utils,
};

pub struct Cluster {
    client: Client,
    nodes: HashMap<usize, Arc<Mutex<Node>>>,
    pub ready: Arc<Mutex<AtomicBool>>,
}

impl Cluster {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            nodes: HashMap::new(),
            ready: Arc::new(Mutex::new(AtomicBool::new(false))),
        }
    }

    pub fn name(&self) -> String {
        "computeservers.node.manager".to_owned()
    }

    pub fn get_client(&self) -> Client {
        self.client.clone()
    }

    pub fn get_next_node_id(&self) -> usize {
        self.nodes.len()
    }

    pub fn add_node(&mut self, node_id: usize, node: Arc<Mutex<Node>>) {
        self.nodes.insert(node_id, node);
    }

    pub fn get_node(&self, node_id: usize) -> Option<Arc<Mutex<Node>>> {
        self.nodes.get(&node_id).map(|n| n.clone())
    }

    pub async fn boot_nodes(&self) -> anyhow::Result<()> {
        for (_, node) in self.nodes.iter() {
            let mut node = node.lock().await;
            node.boot().await?;
        }

        Ok(())
    }

    pub async fn has_running_node(&self) -> bool {
        let mut has_running_node = false;

        for (_, node) in self.nodes.iter() {
            let node = node.lock().await;
            if node.status == NodeStatus::Running {
                has_running_node = true;
            }
        }

        has_running_node
    }

    pub async fn clean_old_versions(&self) -> anyhow::Result<()> {
        cluster_utils::clean(&self.name(), self.client.clone()).await?;
        'wait: loop {
            match self.has_existing_version().await {
                Ok(true) => continue,
                _ => break 'wait,
            }
        }
        Ok(())
    }

    pub async fn has_existing_version(&self) -> anyhow::Result<bool> {
        let crds: Api<CustomResourceDefinition> = Api::all(self.client.clone());
        let res = crds.list(&ListParams::default()).await?;
        Ok(res.items.len() > 0)
    }

    pub async fn create_node_definitions(&self) -> anyhow::Result<()> {
        let crds: Api<CustomResourceDefinition> = Api::all(self.client.clone());

        let nodecrd = ComputeServer::crd();
        println!("Creatingg CRD {}", nodecrd.name_any());
        let pp = PostParams::default();
        match crds.create(&pp, &nodecrd).await {
            Ok(o) => {
                println!("Created {}", o.name_any(),);
            }
            Err(e) => {
                println!("error creating crd definition: {:?}", e);
                return Err(e.into());
            }
        }

        'wait: loop {
            match self.has_existing_version().await {
                Ok(true) => break 'wait,
                _ => continue,
            }
        }

        Ok(())
    }
}
