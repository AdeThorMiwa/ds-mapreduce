use crate::{
    constants::PING_CYCLE_DELAY,
    task::{Task, TaskKind, TaskStatusKind},
    task_manager::TaskManager,
    utils::split_file_to_chunks,
    worker::{WorkerMessage, WorkerStatus},
    worker_pool::WorkerPool,
};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{sync::broadcast, time::sleep};

pub struct Master {
    sender: broadcast::Sender<WorkerMessage>,
    task_manager: TaskManager,
    worker_pool: WorkerPool,
    map_chunk_size: usize,
    total_input_size: usize,
    enable_ping: bool,
}

impl Master {
    pub fn new(worker_size: Option<usize>, map_chunk_size: Option<usize>) -> Self {
        let (sender, ..) = broadcast::channel(100);
        Self {
            sender,
            task_manager: TaskManager::new(),
            worker_pool: WorkerPool::new(worker_size.unwrap_or(10)),
            map_chunk_size: map_chunk_size.unwrap_or(512), // export to config
            total_input_size: 0,
            enable_ping: true,
        }
    }

    async fn split_input_to_tasks(&mut self, input: PathBuf) -> Vec<Task> {
        let mut tasks = Vec::new();

        split_file_to_chunks(input, self.map_chunk_size)
            .await
            .into_iter()
            .enumerate()
            .for_each(|(task_id, task_input)| {
                tasks.push(Task::new(task_id, TaskKind::Map, task_input));
            });

        tasks
    }

    pub async fn spawn_workers(&mut self) {
        self.worker_pool.spawn(self.sender.clone()).await;
    }

    pub async fn create_tasks_from_input(&mut self, input: PathBuf) {
        let tasks = self.split_input_to_tasks(input).await;
        self.total_input_size = tasks.len();
        self.task_manager.add_tasks(tasks).await;
    }

    pub async fn run_scheduler(&mut self) -> std::io::Result<()> {
        'sheduler_cycle: loop {
            {
                let completed_tasks = self.task_manager.clone().get_completed_tasks().await;
                if completed_tasks.len() >= self.total_input_size {
                    break 'sheduler_cycle; // all task completed
                }
            }

            {
                // destroy and replace failed workers
                let workers = self.worker_pool.clone();
                let mut n = 0;
                while let Some((next, r_worker)) = workers.next(n).await {
                    let worker = r_worker.lock().await;
                    if worker.status == WorkerStatus::Failed {
                        Arc::try_unwrap(r_worker.clone())
                            .unwrap()
                            .into_inner()
                            .destroy();
                    }
                    n = next;
                }
            }

            let pending_tasks = self.task_manager.get_pending_tasks().await;
            let mut pending_tasks = pending_tasks.iter();
            while let Some(r_task) = pending_tasks.next() {
                let task = r_task.lock().await;
                if task.status == TaskStatusKind::Idle {
                    let (worker, permit) = self.worker_pool.get_idle_worker().await;
                    worker
                        .lock()
                        .await
                        .assign_task(r_task.clone(), permit)
                        .await;
                }
            }
        }

        Ok(())
    }

    async fn setup_worker_health_monitor(&self) {
        if !self.enable_ping {
            return;
        }
        // ping
        let workers = self.worker_pool.clone();
        tokio::spawn(async move {
            loop {
                {
                    let mut n = 0;
                    while let Some((next, worker)) = workers.next(n).await {
                        worker.lock().await.ping().await;
                        n = next;
                    }
                }

                sleep(Duration::from_millis(PING_CYCLE_DELAY)).await;
            }
        });
    }

    async fn setup_worker_message_listener(&self) {
        let mut receiver = self.sender.subscribe();
        let task_manager = self.task_manager.clone();
        let worker_pool = self.worker_pool.clone();
        tokio::spawn(async move {
            loop {
                if let Ok(msg) = receiver.recv().await {
                    match msg {
                        WorkerMessage::TaskCompleted {
                            worker_id,
                            task_id,
                            output,
                        } => {
                            if let Some(r_task) = task_manager.get_task_by_id(task_id).await {
                                println!(
                                    "task id: {} output >> {} worker {}",
                                    task_id, output, worker_id
                                );

                                if r_task.lock().await.kind == TaskKind::Map {
                                    task_manager.move_task_to_idle(task_id, output).await;
                                } else {
                                    task_manager.move_task_to_completed(task_id).await;
                                }

                                // collect worker
                                worker_pool.collect_idle_worker(worker_id).await;
                            }
                        }
                        _ => {}
                    }
                }
            }
        });
    }

    pub async fn run(&mut self, input: PathBuf) -> std::io::Result<()> {
        self.spawn_workers().await;
        self.create_tasks_from_input(input).await;
        self.setup_worker_message_listener().await;
        self.setup_worker_health_monitor().await;
        self.run_scheduler().await?;

        Ok(())
    }
}
