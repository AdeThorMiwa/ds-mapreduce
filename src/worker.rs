use crate::{
    constants::{INTERMEDIATE_DIR, OUTPUT_DIR},
    implm::word_count::WordCount,
    map_reduce::MapReducer,
    task::{Task, TaskKind},
    utils::{get_file_name, retrieve_parsed_intermediate_file, write_result},
};
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    fs::read_to_string,
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
pub enum WorkerMessage {
    Ping {
        notifier: Arc<Notify>,
    },
    TaskCompleted {
        worker_id: usize,
        task_id: usize,
        output: String,
    },
    NewTask {
        task_id: usize,
        input: PathBuf,
        kind: TaskKind,
    },
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
        println!("spawing worker with id: {}", worker_id);
        let mut r = s.subscribe();
        tokio::spawn(async move {
            loop {
                if let Ok(msg) = r.recv().await {
                    match msg {
                        WorkerMessage::NewTask {
                            task_id,
                            input,
                            kind,
                        } => {
                            if kind == TaskKind::Map {
                                Self::map_runner(input, task_id, worker_id, s.clone()).await;
                            } else {
                                Self::reduce_runner(input, task_id, worker_id, s.clone()).await;
                            }
                        }
                        WorkerMessage::Ping { notifier } => {
                            notifier.notify_one();
                        }
                        _ => {}
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
        let input = read_to_string(input).await.unwrap();
        let w = WordCount;
        let result = w.map(filename, input);
        let output = write_result(result, INTERMEDIATE_DIR).await;
        let msg = WorkerMessage::TaskCompleted {
            task_id,
            output,
            worker_id,
        };
        s.send(msg).unwrap();
    }

    async fn reduce_runner(
        input: PathBuf,
        task_id: usize,
        worker_id: usize,
        s: broadcast::Sender<WorkerMessage>,
    ) {
        let filename = get_file_name(&input).unwrap();
        let parsed = retrieve_parsed_intermediate_file(&filename).await;
        let grouped = Self::group_input(parsed);

        let mut reduce_result = Vec::new();
        let w = WordCount;
        for (key, values) in grouped.clone() {
            let value = w.reduce(key.clone(), values);
            reduce_result.push((key, value));
        }

        let output = write_result(reduce_result, OUTPUT_DIR).await;
        let msg = WorkerMessage::TaskCompleted {
            task_id,
            output,
            worker_id,
        };
        s.send(msg).unwrap();
    }

    fn group_input(input: Vec<(String, u32)>) -> HashMap<String, Vec<u32>> {
        let mut grouped: HashMap<String, Vec<u32>> = HashMap::new();

        for (key, value) in input {
            let mut values: Vec<u32> = vec![value];

            if let Some(value) = grouped.get(&key) {
                values.extend(value.iter())
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

        let msg = WorkerMessage::NewTask {
            task_id: task.id,
            input: PathBuf::from(&task.input),
            kind: task.kind,
        };
        self.sender.send(msg).unwrap();
    }

    pub async fn ping(&mut self) {
        let notifier = Arc::new(Notify::new());
        self.sender
            .send(WorkerMessage::Ping {
                notifier: notifier.clone(),
            })
            .unwrap();

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
