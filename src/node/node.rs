use anyhow::Context;
use futures::{Future, StreamExt};
use garde::Validate;
use k8s_openapi::api::apps::v1::Deployment;
use kube::{
    api::{Api, DeleteParams, Patch, PatchParams, PostParams, ResourceExt},
    runtime::{controller::Action, Controller},
    Client, CustomResource, Resource,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::BTreeMap, fmt::Display, sync::Arc, time::Duration};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::*;

use super::utils::node::node_utils::get_deployment_definition;

#[derive(thiserror::Error, Debug)]
pub enum NodeError {
    AddFinalizerError,
    RemoveFinalizerError,
    DeploymentError,
    DestroyError,
}

impl Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub type ReconcileResult<T, E = NodeError> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum NodeEvent {
    Created,
    Destroyed,
}

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, Validate, JsonSchema)]
#[kube(
    group = "node.manager",
    version = "v1",
    kind = "ComputeServer",
    namespaced
)]
#[kube(status = "ComputeServerStatus")]
#[kube(scale = r#"{"specReplicasPath":".spec.replicas", "statusReplicasPath":".status.replicas"}"#)]
#[kube(printcolumn = r#"{"name":"Team", "jsonPath": ".spec.metadata.team", "type": "string"}"#)]
pub struct ComputeServerSpec {
    #[schemars(length(min = 3))]
    #[garde(length(min = 3))]
    name: String,
    #[garde(skip)]
    replicas: i32,
    #[garde(skip)]
    image: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
pub struct ComputeServerStatus {
    replicas: u32,
    is_deployed: bool,
}

enum ComputerServerAction {
    Create,
    Delete,
    NoOp,
}

struct ComputeServerCtx {
    #[allow(dead_code)]
    id: usize,
    client: Client,
    s: mpsc::Sender<NodeEvent>,
    cluster_name: String,
}

impl ComputeServerCtx {
    fn new(id: usize, cluster_name: &str, client: Client, s: mpsc::Sender<NodeEvent>) -> Self {
        Self {
            id,
            client,
            s,
            cluster_name: cluster_name.to_owned(),
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum NodeStatus {
    Idle,
    Running,
    Terminated,
}

pub struct Node {
    pub id: usize,
    image: String,
    cluster_name: String,
    replicas: i32,
    client: Client,
    controller: Option<JoinHandle<()>>,
    executor: Option<JoinHandle<()>>,
    pub status: NodeStatus,
    compute: Option<ComputeServer>,
}

impl Node {
    pub fn new(
        id: usize,
        image: &str,
        cluster_name: String,
        replicas: i32,
        client: Client,
    ) -> Self {
        Self {
            id,
            image: image.to_owned(),
            cluster_name,
            replicas,
            client,
            controller: None,
            executor: None,
            status: NodeStatus::Idle,
            compute: None,
        }
    }

    fn name(&self) -> String {
        format!("compute-server-{}", self.id)
    }

    pub async fn boot(&mut self) -> anyhow::Result<()> {
        let servers = Api::<ComputeServer>::default_namespaced(self.client.clone());
        let data = ComputeServer::new(
            &self.name(),
            ComputeServerSpec {
                name: self.name(),
                replicas: self.replicas,
                image: self.image.to_owned(),
            },
        );
        let compute = servers
            .create(&PostParams::default(), &data)
            .await
            .with_context(|| format!("creating compute server for node {}", self.name()))?;
        self.compute = Some(compute);
        self.status = NodeStatus::Running;
        Ok(())
    }

    pub async fn watch<F, R>(&mut self, on_event: F)
    where
        F: Send + Clone + 'static,
        F: FnOnce(NodeEvent) -> R,
        R: Future<Output = ()> + Send,
    {
        let (s, mut r) = mpsc::channel(1);
        let ctx = ComputeServerCtx::new(self.id, &self.cluster_name, self.client.clone(), s);

        let client = self.client.clone();
        let controller = tokio::spawn(async move {
            let pods = Api::<ComputeServer>::all(client);
            Controller::new(pods.clone(), Default::default())
                .run(Self::reconcile, Self::error_policy, Arc::new(ctx))
                .for_each(|_| futures::future::ready(()))
                .await;
        });

        let executor = tokio::spawn(async move {
            while let Some(e) = r.recv().await {
                let on_event = on_event.clone();
                on_event(e).await;
            }
        });

        self.controller = Some(controller);
        self.executor = Some(executor);
    }

    async fn reconcile(
        server: Arc<ComputeServer>,
        ctx: Arc<ComputeServerCtx>,
    ) -> ReconcileResult<Action> {
        let client = ctx.client.clone();
        match Self::get_action(&server) {
            ComputerServerAction::Create => {
                if let Some(status) = &server.status {
                    if status.is_deployed {
                        return Ok(Action::await_change());
                    }
                }

                Self::add_finalizer(&ctx.cluster_name, &server, client.clone())
                    .await
                    .map_err(|_| NodeError::AddFinalizerError)?;
                match Self::deploy_server(&server, client).await {
                    Ok(_) => {}
                    Err(kube::Error::Api(e)) if e.code == 409 => {}
                    Err(e) => {
                        eprintln!(
                            "error during deployment for {} ------ {:?}",
                            server.name_any(),
                            e
                        );
                        return Err(NodeError::DeploymentError);
                    }
                }
                ctx.s.send(NodeEvent::Created).await.unwrap();
                Ok(Action::requeue(Duration::from_secs(5)))
            }
            ComputerServerAction::Delete => {
                match Self::destroy_server(&server, client.clone()).await {
                    Ok(_) => {}
                    Err(kube::Error::Api(e)) if e.code == 404 => {}
                    _ => return Err(NodeError::DestroyError),
                }
                match Self::remove_finalizer(&server, client.clone()).await {
                    Ok(_) => {}
                    Err(kube::Error::Api(e)) if e.code == 404 => {}
                    _ => return Err(NodeError::RemoveFinalizerError),
                }
                ctx.s.send(NodeEvent::Destroyed).await.unwrap();
                Ok(Action::await_change())
            }
            ComputerServerAction::NoOp => Ok(Action::requeue(Duration::from_secs(5))),
        }
    }

    fn error_policy(
        server: Arc<ComputeServer>,
        err: &NodeError,
        _ctx: Arc<ComputeServerCtx>,
    ) -> Action {
        eprintln!(
            "Node Reconciliation error:{:?}. {:?}",
            err,
            server.name_any()
        );
        Action::requeue(Duration::from_secs(5))
    }

    fn get_action(server: &ComputeServer) -> ComputerServerAction {
        if server.meta().deletion_timestamp.is_some() {
            ComputerServerAction::Delete
        } else if server
            .meta()
            .finalizers
            .as_ref()
            .map_or(true, |finalizers| finalizers.is_empty())
        {
            ComputerServerAction::Create
        } else {
            ComputerServerAction::NoOp
        }
    }

    async fn add_finalizer(
        cluster_name: &str,
        server: &ComputeServer,
        client: Client,
    ) -> Result<ComputeServer, kube::Error> {
        let (name, namespace) = Self::get_name_and_namespace(&server)?;
        let api = Api::<ComputeServer>::namespaced(client, &namespace);
        let finalizer: Value = json!({
            "metadata": {
                "finalizers": [format!("{cluster_name}/finalizer")]
            }
        });

        let patch: Patch<&Value> = Patch::Merge(&finalizer);
        api.patch(&name, &PatchParams::default(), &patch).await
    }

    async fn remove_finalizer(
        server: &ComputeServer,
        client: Client,
    ) -> Result<ComputeServer, kube::Error> {
        let (name, namespace) = Self::get_name_and_namespace(&server)?;
        let api = Api::<ComputeServer>::namespaced(client, &namespace);
        let finalizer: Value = json!({
            "metadata": {
                "finalizers": null
            }
        });

        let patch: Patch<&Value> = Patch::Merge(&finalizer);
        api.patch(&name, &PatchParams::default(), &patch).await
    }

    async fn deploy_server(
        server: &ComputeServer,
        client: Client,
    ) -> Result<Deployment, kube::Error> {
        let (name, namespace) = Self::get_name_and_namespace(&server)?;
        let image = &server.spec.image;
        let replicas = server.spec.replicas;
        let mut labels: BTreeMap<String, String> = BTreeMap::new();
        labels.insert("app".to_owned(), name.to_owned());
        let deployment = get_deployment_definition(&name, &namespace, image, replicas, labels);
        let deployment_api = Api::<Deployment>::namespaced(client.clone(), &namespace);
        let deployment = deployment_api
            .create(&PostParams::default(), &deployment)
            .await;

        // update status
        let status_data = json!({
            "status": ComputeServerStatus { is_deployed: true, replicas: 1 }
        });
        let servers = Api::<ComputeServer>::namespaced(client, &namespace);
        servers
            .patch_status(&name, &PatchParams::default(), &Patch::Merge(&status_data))
            .await?;
        deployment
    }

    async fn destroy_server(server: &ComputeServer, client: Client) -> Result<(), kube::Error> {
        let (name, namespace) = Self::get_name_and_namespace(&server)?;
        let api: Api<Deployment> = Api::namespaced(client, &namespace);
        api.delete(&name, &DeleteParams::default()).await?;
        Ok(())
    }

    fn get_name_and_namespace(server: &ComputeServer) -> Result<(String, String), kube::Error> {
        let name = server.name_any();
        let namespace = match server.namespace() {
            None => {
                let e = "Expected Echo resource to be namespaced. Can't deploy to an unknown namespace.";
                return Err(kube::Error::Discovery(
                    kube::error::DiscoveryError::MissingResource(e.to_owned()),
                ));
            }
            Some(namespace) => namespace,
        };

        Ok((name, namespace))
    }

    fn _destroy(self) {
        warn!("Deleting node id: {}", self.id);
        drop(self);
    }
}
