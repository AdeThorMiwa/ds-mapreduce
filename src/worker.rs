use crate::{
    constants::{INTERMEDIATE_DIR, OUTPUT_DIR},
    implm::word_count::WordCount,
    map_reduce::MapReducer,
    task::{Task, TaskKind},
    utils::{get_file_name, retrieve_parsed_intermediate_file, write_result},
};
use std::{collections::HashMap, fs::read_to_string, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    sync::{broadcast, Mutex, Notify, OwnedSemaphorePermit},
    task::JoinHandle,
    time::timeout,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerKind {
    Map,
    Reduce,
}

#[derive(PartialEq, Debug)]
pub enum WorkerStatus {
    Idle,
    Busy,
    Failed,
}

#[derive(Clone, Debug)]
pub enum WorkerMessageKind {
    Ping {
        notifier: Arc<Notify>,
    },
    TaskCompleted {
        task_id: usize,
        output: String,
        worker_id: usize,
    },
    NewTask {
        task_id: usize,
        input: PathBuf,
        kind: TaskKind,
    },
}

#[derive(Clone, Debug)]
pub struct WorkerMessage {
    pub worker_id: Option<usize>,
    pub kind: WorkerMessageKind,
}

#[derive(Debug)]
pub struct Worker {
    pub id: usize,
    pub status: WorkerStatus,
    pub task: Option<Arc<Mutex<Task>>>,
    sender: broadcast::Sender<WorkerMessage>,
    _task_runner: JoinHandle<()>,
    permit: Option<OwnedSemaphorePermit>,
}

impl Worker {
    pub fn new(id: usize, sender: broadcast::Sender<WorkerMessage>) -> Self {
        let task_runner = Self::create_task_runner(id, sender.clone());

        Self {
            id,
            status: WorkerStatus::Idle,
            task: None,
            _task_runner: task_runner,
            permit: None,
            sender,
        }
    }

    fn create_task_runner(worker_id: usize, s: broadcast::Sender<WorkerMessage>) -> JoinHandle<()> {
        let mut r = s.subscribe();
        tokio::spawn(async move {
            loop {
                if let Ok(msg) = r.recv().await {
                    // TODO: figure out a better way to structure this, its weird
                    if let Some(w_id) = msg.worker_id {
                        if w_id == worker_id {
                            match msg.kind {
                                WorkerMessageKind::NewTask {
                                    task_id,
                                    input,
                                    kind,
                                } => {
                                    if kind == TaskKind::Map {
                                        Self::map_runner(input, task_id, worker_id, s.clone())
                                            .await;
                                    } else {
                                        Self::reduce_runner(input, task_id, worker_id, s.clone())
                                            .await;
                                    }
                                }
                                _ => {}
                            }
                        }
                    } else {
                        match msg.kind {
                            WorkerMessageKind::Ping { notifier } => {
                                notifier.notify_one();
                            }
                            _ => {}
                        }
                    }
                }
            }
        })
    }

    async fn map_runner(
        input: PathBuf,
        task_id: usize,
        worker_id: usize,
        s: broadcast::Sender<WorkerMessage>,
    ) {
        let filename = get_file_name(&input).unwrap();
        let input = read_to_string(input).unwrap();
        let w = WordCount;
        let result = w.map(filename, input);
        let output = write_result(result, INTERMEDIATE_DIR).await;
        let msg = WorkerMessage {
            worker_id: Some(worker_id),
            kind: WorkerMessageKind::TaskCompleted {
                task_id,
                output,
                worker_id,
            },
        };
        s.send(msg).unwrap();
    }

    async fn reduce_runner(
        input: PathBuf,
        task_id: usize,
        worker_id: usize,
        s: broadcast::Sender<WorkerMessage>,
    ) {
        let parsed = retrieve_parsed_intermediate_file(&input).await;
        let grouped = Self::group_input(parsed);

        let mut reduce_result = Vec::new();
        let w = WordCount;
        for (key, values) in grouped.clone() {
            let value = w.reduce(key.clone(), values);
            reduce_result.push((key, value));
        }

        let output = write_result(reduce_result, OUTPUT_DIR).await;
        let msg = WorkerMessage {
            worker_id: Some(worker_id),
            kind: WorkerMessageKind::TaskCompleted {
                task_id,
                output,
                worker_id,
            },
        };
        s.send(msg).unwrap();
    }

    fn group_input(input: Vec<(String, String)>) -> HashMap<String, Vec<String>> {
        let mut grouped: HashMap<String, Vec<String>> = HashMap::new();

        for (key, value) in input {
            let mut values: Vec<String> = vec![value];

            if let Some(value) = grouped.get(&key) {
                values.extend(value.into_iter().map(|t| t.to_owned()))
            }

            grouped.insert(key, values);
        }

        grouped
    }

    pub async fn assign_task(&mut self, task: Arc<Mutex<Task>>, permit: OwnedSemaphorePermit) {
        if self.status != WorkerStatus::Idle {
            return;
        }

        self.status = WorkerStatus::Busy;
        self.task = Some(task.clone());
        self.permit = Some(permit);

        let mut task = task.lock().await;
        task.started(self.id);

        let msg = WorkerMessage {
            worker_id: Some(self.id),
            kind: WorkerMessageKind::NewTask {
                task_id: task.id,
                input: PathBuf::from(&task.input),
                kind: task.kind,
            },
        };
        self.sender.send(msg).unwrap();
    }

    pub async fn ping(&mut self) {
        let notifier = Arc::new(Notify::new());
        let msg = WorkerMessage {
            worker_id: None,
            kind: WorkerMessageKind::Ping {
                notifier: notifier.clone(),
            },
        };
        self.sender.send(msg).unwrap();

        if let Err(_) = timeout(Duration::from_millis(500), notifier.notified()).await {
            println!("worker {} failed...", self.id);
            self.status = WorkerStatus::Failed;
        }
    }

    pub async fn reset(&mut self) {
        self.status = WorkerStatus::Idle;
        self.task = None;
        self.permit = None;
    }

    pub fn destroy(self) {
        drop(self)
    }
}
