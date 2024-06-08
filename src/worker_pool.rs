use crate::{
    node::manager::NodeManager,
    worker::{Worker, WorkerMessage, WorkerStatus},
};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::sync::{broadcast, Mutex, OwnedSemaphorePermit, Semaphore};

pub struct WorkerPool {
    workers: Arc<Mutex<HashMap<usize, Arc<Mutex<Worker>>>>>,
    size: usize,
    resource_manager: Arc<Semaphore>,
    node_manager: Option<NodeManager>,
}

impl WorkerPool {
    pub fn new(size: usize) -> Self {
        Self {
            workers: Arc::new(Mutex::new(HashMap::new())),
            size,
            resource_manager: Arc::new(Semaphore::new(size * 2)),
            node_manager: None,
        }
    }

    pub async fn init_node_manager(&mut self) -> std::io::Result<()> {
        let node_manager = NodeManager::try_default()
            .await
            .expect("unable to initialize node manager");
        self.node_manager = Some(node_manager);
        Ok(())
    }

    pub async fn spawn(&self, actor: PathBuf, sender: broadcast::Sender<WorkerMessage>) {
        for _ in 0..self.size {
            self.spawn_single(actor.clone(), sender.clone()).await;
        }
    }

    pub async fn spawn_single(&self, actor: PathBuf, sender: broadcast::Sender<WorkerMessage>) {
        let workers = self.workers.clone();
        let mut workers = workers.lock().await;
        let worker_id = workers.len();
        let mut worker = Worker::new(worker_id, sender.clone(), self.node_manager.clone());
        worker.start(actor).await.expect("unable to start worker");
        workers.insert(worker_id, Arc::new(Mutex::new(worker)));
    }

    pub async fn get_worker_by_id(&self, worker_id: usize) -> Option<Arc<Mutex<Worker>>> {
        let workers = self.workers.lock().await;
        workers.get(&worker_id).map(|w| w.clone())
    }

    pub async fn next(&self, n: usize) -> Option<(usize, Arc<Mutex<Worker>>)> {
        let workers = self.workers.lock().await;
        if let Some((_, w)) = workers.iter().nth(n) {
            Some((n + 1, w.clone()))
        } else {
            None
        }
    }

    pub async fn get_idle_worker(&self) -> (Arc<Mutex<Worker>>, OwnedSemaphorePermit) {
        let permit_manager = self.resource_manager.clone();
        let permit = permit_manager.acquire_owned().await.unwrap();

        // at this point there should be atleast one free (idle) worker
        let worker = async {
            let workers = async {
                let workers = self.workers.lock().await;
                let mut wks = Vec::new();
                for w in workers.values() {
                    wks.push(w.clone())
                }
                wks
            }
            .await;
            let mut workers = workers.iter();

            while let Some(r_worker) = workers.next() {
                let worker = r_worker.lock().await;
                if worker.status == WorkerStatus::Idle {
                    return Some(r_worker.clone());
                }
            }
            None
        }
        .await;

        (worker.unwrap(), permit)
    }

    pub async fn collect_idle_worker(&self, worker_id: usize) {
        let workers = self.workers.lock().await;
        if let Some(worker) = workers.get(&worker_id) {
            worker.lock().await.reset().await;
        }
    }
}

impl Clone for WorkerPool {
    fn clone(&self) -> Self {
        Self {
            workers: self.workers.clone(),
            size: self.size,
            resource_manager: self.resource_manager.clone(),
            node_manager: self.node_manager.clone(),
        }
    }
}
