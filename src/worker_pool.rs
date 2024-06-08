use crate::worker::{Worker, WorkerMessage, WorkerStatus};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{broadcast, Mutex, OwnedSemaphorePermit, Semaphore};

pub struct WorkerPool {
    workers: Arc<Mutex<HashMap<usize, Arc<Mutex<Worker>>>>>,
    size: usize,
    manager: Arc<Semaphore>,
}

impl WorkerPool {
    pub fn new(size: usize) -> Self {
        Self {
            workers: Arc::new(Mutex::new(HashMap::new())),
            size,
            manager: Arc::new(Semaphore::new(size * 2)),
        }
    }

    pub async fn spawn(&self, sender: broadcast::Sender<WorkerMessage>) {
        for _ in 0..self.size {
            self.spawn_single(sender.clone()).await;
        }
    }

    pub async fn spawn_single(&self, sender: broadcast::Sender<WorkerMessage>) {
        let workers = self.workers.clone();
        let mut workers = workers.lock().await;
        let worker_id = workers.len();
        let worker = Worker::new(worker_id, sender.clone());
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
        let permit_manager = self.manager.clone();
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
            manager: self.manager.clone(),
        }
    }
}
