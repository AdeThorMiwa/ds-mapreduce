use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    fs::{create_dir_all, File},
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, Mutex},
    task::JoinHandle,
    time::sleep,
};

use rand::Rng;

use crate::implm::word_count::WordCount;

const TEMP_DIR: &str = "tmp";
const INTERMEDIATE_DIR: &str = "inter";
const OUTPUT_DIR: &str = "out";
const PING_CYCLE_DELAY: u64 = 2000;

pub trait MapReducer {
    type MapKey;
    type MapValue;
    type ReduceKey;
    type ReduceValue;

    fn map(
        &self,
        key: Self::MapKey,
        value: Self::MapValue,
    ) -> Vec<(Self::ReduceKey, Self::ReduceValue)>;
    fn reduce(&self, key: Self::ReduceKey, value: Vec<Self::ReduceValue>) -> Self::ReduceValue;
}

async fn split_file_to_chunks(file_path: PathBuf, chunk_size: usize) -> Vec<PathBuf> {
    let mut file = File::open(&file_path).await.expect("invalid file");
    let mut buf = vec![0; chunk_size];
    let filename = file_path
        .file_name()
        .expect("unable to get filename")
        .to_str()
        .expect("filename to_str failed");
    let file_ext = file_path
        .extension()
        .expect("unable to get file extension")
        .to_str()
        .expect("file extension to_str failed");

    let mut chunk_index = 0;
    let mut chunk_file_paths = vec![];

    if !Path::new(TEMP_DIR).exists() {
        create_dir_all(TEMP_DIR)
            .await
            .expect("failed to create tmp directory")
    }

    while let Ok(_) = file.read_exact(&mut buf).await {
        let chunk_filename = format!(
            "{}/{}-chunk-{}.{}",
            TEMP_DIR, filename, chunk_index, file_ext
        );
        let chunk_file_path = PathBuf::from(chunk_filename);
        let mut chunk_file = File::create(&chunk_file_path)
            .await
            .expect(format!("failed to create chunk file {}", chunk_index).as_str());
        let _ = chunk_file.write_all(&mut buf);

        chunk_file_paths.push(chunk_file_path);
        chunk_index += 1;
    }

    chunk_file_paths
}

fn result_to_buf(result: Vec<(String, u32)>) -> String {
    let mut buf = String::new();
    for (key, value) in result {
        buf.push_str(format!("{} {}\n", key, value).as_str())
    }
    buf
}

async fn write_result(result: Vec<(String, u32)>, dir: &str) -> String {
    let random_number = rand::thread_rng().gen_range((u32::MAX as u64)..u64::MAX);
    let chunk_filename = format!("{}/chunk-{}.txt", dir, random_number); // What format should intermediate file be saved in
    let chunk_file_path = PathBuf::from(&chunk_filename);

    if !Path::new(dir).exists() {
        create_dir_all(dir)
            .await
            .expect("failed to create intermediate directory")
    }

    let mut chunk_file = File::create(&chunk_file_path)
        .await
        .expect(format!("failed to create chunk file {}", chunk_filename).as_str());
    let _ = chunk_file.write_all(&mut result_to_buf(result).as_bytes());
    chunk_filename
}

async fn retrieve_parsed_intermediate_file(filename: &str) -> Vec<(String, u32)> {
    let file_path = PathBuf::from(filename);
    let mut buf = String::new();
    let mut file = File::open(file_path)
        .await
        .expect("failed to oopen intermediate file");
    let _ = file.read_to_string(&mut buf);

    let mut parsed = vec![];
    for line in buf.lines() {
        if let Some((key, value)) = line.split_once(" ") {
            parsed.push((key.to_owned(), value.parse::<u32>().expect("invalid u32")))
        }
    }

    parsed
}

pub async fn map_reduce<
    F: MapReducer<MapKey = String, MapValue = String, ReduceKey = String, ReduceValue = u32>,
>(
    file: PathBuf,
    func: F,
) where
    F: Sync + Send + 'static,
{
    // make workers into struct
    let func = Arc::new(Mutex::new(func));
    let files = split_file_to_chunks(file, 512).await;
    let (m_sender, mut m_receiver) = mpsc::channel(files.len() * 2);

    let mut map_workers_join_handle = Vec::new();
    for file_path in files {
        let func = func.clone();
        let sender = m_sender.clone();
        let handle = tokio::spawn(async move {
            let mut buf = String::new();
            let mut file = File::open(&file_path)
                .await
                .expect(format!("failed to open file chunk {:?}", &file_path.file_name()).as_str());
            let _ = file.read_to_string(&mut buf);

            let filename = file_path
                .file_name()
                .expect("could not get filename")
                .to_str()
                .expect("file name to_str failed");

            let result = func.lock().await.map(filename.to_owned(), buf);

            let file_name = write_result(result, INTERMEDIATE_DIR).await;

            sender.send(file_name).await.unwrap();
        });

        map_workers_join_handle.push(handle)
    }

    let mut reducer_workers_join_handle = Vec::new();
    while let Some(intermediate_file_name) = m_receiver.recv().await {
        // println!("[master] intermediate output");
        // println!("[master] {:?}", completed_task);
        let func = func.clone();
        let handle = tokio::spawn(async move {
            let parsed = retrieve_parsed_intermediate_file(&intermediate_file_name).await;
            let mut grouped: HashMap<String, Vec<u32>> = HashMap::new();

            for (key, value) in parsed {
                let mut values: Vec<u32> = vec![value];

                if let Some(value) = grouped.get(&key) {
                    values.extend(value.iter())
                }

                grouped.insert(key, values);
            }

            let mut reduce_result = Vec::new();
            for (key, values) in grouped.clone() {
                let value = func.lock().await.reduce(key.clone(), values);
                reduce_result.push((key, value));
            }

            write_result(reduce_result, OUTPUT_DIR).await
        });

        reducer_workers_join_handle.push(handle)
    }

    for handle in map_workers_join_handle {
        handle.await.unwrap();
    }

    for handle in reducer_workers_join_handle {
        handle.await.unwrap();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkerKind {
    Map,
    Reduce,
}

#[derive(PartialEq, Debug)]
enum WorkerStatus {
    Idle,
    Busy,
    Failed,
}

#[derive(Debug, PartialEq)]
enum TaskStatusKind {
    Idle,
    InProgress { worker_id: usize },
    Completed,
}

#[derive(Eq, PartialEq, Debug)]
enum TaskKind {
    Map,
    Reduce,
}

#[derive(Debug)]
struct Task {
    id: usize,
    status: TaskStatusKind,
    kind: TaskKind,
    input: PathBuf,
}

impl Task {
    pub fn new(id: usize, kind: TaskKind, input: PathBuf) -> Self {
        Self {
            id,
            status: TaskStatusKind::Idle,
            kind,
            input,
        }
    }

    pub fn started(&mut self, worker_id: usize) {
        if self.status == TaskStatusKind::Idle {
            self.status = TaskStatusKind::InProgress { worker_id }
        }
    }

    pub fn completed(&mut self) {
        if let TaskStatusKind::InProgress { .. } = self.status {
            self.status = TaskStatusKind::Completed
        }
    }
}

#[derive(Debug)]
enum WorkerTaskRunnerMessage {
    NewMap { task: Arc<Mutex<Task>> },
    NewReduce { task: Arc<Mutex<Task>> },
    Ping,
}

#[derive(Debug)]
struct Worker {
    kind: WorkerKind,
    status: WorkerStatus,
    task: Option<Arc<Mutex<Task>>>,
    sender: mpsc::Sender<WorkerTaskRunnerMessage>,
    _task_runner: JoinHandle<()>,
}

impl Worker {
    pub fn new(id: usize, kind: WorkerKind, m_comm: mpsc::Sender<WorkerMessage>) -> Self {
        let (sender, receiver) = mpsc::channel(10);
        let task_runner = if kind == WorkerKind::Map {
            Self::create_map_task_runner(id, m_comm, receiver)
        } else {
            Self::create_reduce_task_runner(id, m_comm, receiver)
        };

        Self {
            kind,
            status: WorkerStatus::Idle,
            task: None,
            sender,
            _task_runner: task_runner,
        }
    }

    fn create_map_task_runner(
        worker_id: usize,
        s: mpsc::Sender<WorkerMessage>,
        mut r: mpsc::Receiver<WorkerTaskRunnerMessage>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(msg) = r.recv().await {
                match msg {
                    WorkerTaskRunnerMessage::NewMap { task } => {
                        {
                            task.lock().await.started(worker_id)
                        }

                        let file_path = {
                            let task = task.lock().await;
                            PathBuf::from(&task.input)
                        };
                        let mut buf = String::new();
                        let mut file = File::open(&file_path).await.expect(
                            format!("failed to open file chunk {:?}", &file_path.file_name())
                                .as_str(),
                        );
                        let _ = file.read_to_string(&mut buf);

                        let filename = file_path
                            .file_name()
                            .expect("could not get filename")
                            .to_str()
                            .expect("file name to_str failed");

                        let w = WordCount;
                        let result = w.map(filename.to_owned(), buf);
                        let output = write_result(result, INTERMEDIATE_DIR).await;

                        let mut task = task.lock().await;
                        task.completed();
                        s.send(WorkerMessage::TaskCompleted {
                            task_id: task.id,
                            output,
                        })
                        .await
                        .unwrap();
                    }
                    WorkerTaskRunnerMessage::Ping => {
                        s.send(WorkerMessage::Pong { worker_id }).await.unwrap()
                    }
                    _ => {}
                }
            }
        })
    }

    fn create_reduce_task_runner(
        worker_id: usize,
        s: mpsc::Sender<WorkerMessage>,
        mut r: mpsc::Receiver<WorkerTaskRunnerMessage>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(msg) = r.recv().await {
                match msg {
                    WorkerTaskRunnerMessage::NewReduce { task } => {
                        {
                            task.lock().await.started(worker_id);
                        }

                        let filename = {
                            let task = task.lock().await;
                            task.input.to_str().unwrap().to_string()
                        };
                        let parsed = retrieve_parsed_intermediate_file(&filename).await;
                        let mut grouped: HashMap<String, Vec<u32>> = HashMap::new();

                        for (key, value) in parsed {
                            let mut values: Vec<u32> = vec![value];

                            if let Some(value) = grouped.get(&key) {
                                values.extend(value.iter())
                            }

                            grouped.insert(key, values);
                        }

                        let mut reduce_result = Vec::new();
                        let w = WordCount;
                        for (key, values) in grouped.clone() {
                            let value = w.reduce(key.clone(), values);
                            reduce_result.push((key, value));
                        }

                        let output = write_result(reduce_result, OUTPUT_DIR).await;
                        let mut task = task.lock().await;
                        task.completed();
                        s.send(WorkerMessage::TaskCompleted {
                            task_id: task.id,
                            output,
                        })
                        .await
                        .unwrap();
                    }
                    WorkerTaskRunnerMessage::Ping => {
                        s.send(WorkerMessage::Pong { worker_id }).await.unwrap()
                    }
                    _ => {}
                }
            }
        })
    }

    pub async fn assign_task(&mut self, task: Arc<Mutex<Task>>) {
        if self.status != WorkerStatus::Idle {
            return;
        }

        self.status = WorkerStatus::Busy;
        self.task = Some(task.clone());

        let msg = if self.kind == WorkerKind::Map {
            WorkerTaskRunnerMessage::NewMap { task: task.clone() }
        } else {
            WorkerTaskRunnerMessage::NewReduce { task: task.clone() }
        };

        self.sender.send(msg).await.unwrap();
    }

    pub async fn ping(&self) -> Result<(), ()> {
        self.sender
            .send(WorkerTaskRunnerMessage::Ping)
            .await
            .map_err(|_| ())
    }

    pub fn destroy(self) {}
}

enum WorkerMessage {
    Pong { worker_id: usize },
    TaskCompleted { task_id: usize, output: String },
}

struct Master {
    comm_channel: (
        mpsc::Sender<WorkerMessage>,
        Arc<mpsc::Receiver<WorkerMessage>>,
    ),
    pending_tasks: Arc<HashMap<usize, Arc<Mutex<Task>>>>,
    completed_tasks: Arc<HashMap<usize, Arc<Mutex<Task>>>>,
    workers: Arc<HashMap<usize, Arc<Mutex<Worker>>>>,
    workers_pool_size: usize,
    map_chunk_size: usize,
    total_input_size: usize,
}

impl Master {
    pub fn new(worker_size: Option<usize>, map_chunk_size: Option<usize>) -> Self {
        let (sender, receiver) = mpsc::channel(100);
        Self {
            comm_channel: (sender, Arc::new(receiver)),
            pending_tasks: Arc::new(HashMap::new()),
            completed_tasks: Arc::new(HashMap::new()),
            workers: Arc::new(HashMap::new()),
            workers_pool_size: worker_size.unwrap_or(10), // export to config
            map_chunk_size: map_chunk_size.unwrap_or(512), // export to config
            total_input_size: 0,
        }
    }

    async fn split_input_to_tasks(
        &mut self,
        input: PathBuf,
    ) -> Arc<HashMap<usize, Arc<Mutex<Task>>>> {
        let mut tasks = Arc::new(HashMap::new());

        split_file_to_chunks(input, self.map_chunk_size)
            .await
            .into_iter()
            .enumerate()
            .for_each(|(task_id, task_input)| {
                let task = Arc::new(Mutex::new(Task::new(task_id, TaskKind::Map, task_input)));
                let tasks = Arc::get_mut(&mut tasks).unwrap();
                tasks.insert(task_id, task);
            });

        tasks
    }

    pub fn spawn(&mut self, kind: WorkerKind) {
        let workers = Arc::get_mut(&mut self.workers).unwrap();
        let worker_id = workers.len();
        let worker = Worker::new(worker_id, kind, self.comm_channel.0.clone());
        workers.insert(worker_id, Arc::new(Mutex::new(worker)));
    }

    pub fn spawn_multiple(&mut self, kind: WorkerKind, n: usize) {
        (0..n).into_iter().for_each(|_| {
            self.spawn(kind);
        })
    }

    pub async fn schedule_tasks(&mut self) -> std::io::Result<()> {
        loop {
            if self.completed_tasks.len() >= self.total_input_size {
                // all tasks completed
                break;
            }

            let pending_tasks = self.pending_tasks.clone();
            let mut pending_tasks = pending_tasks.values();
            while let Some(r_pending_task) = pending_tasks.next() {
                let pending_task = r_pending_task.clone();
                let pending_task = pending_task.lock().await;
                match pending_task.status {
                    TaskStatusKind::Idle => {
                        let worker_kind = if pending_task.kind == TaskKind::Map {
                            WorkerKind::Map
                        } else {
                            WorkerKind::Reduce
                        };

                        let worker = self.get_idle_worker(worker_kind).await;
                        let mut worker = worker.lock().await;
                        worker.assign_task(r_pending_task.clone()).await
                    }
                    _ => continue,
                }
            }
        }

        Ok(())
    }

    pub async fn get_idle_worker(&self, worker_kind: WorkerKind) -> Arc<Mutex<Worker>> {
        // current assumption is that there is more worker than there will be task
        // so we would always be able to get an idle worker
        // to relax assumption, we need to use some sort of resource manager to
        // guard getting an idle worker. e.g use a semaphore here that yields
        // control back to executor and continues execution back as soon as there's an
        // idle worker available to process task
        let workers = self.workers.clone();

        let worker = async {
            while let Some(r_worker) = workers.values().next() {
                let worker = r_worker.lock().await;
                if worker.status == WorkerStatus::Idle && worker.kind == worker_kind {
                    return Some(r_worker);
                }
            }

            None
        }
        .await;

        worker.unwrap().clone()
    }

    pub async fn run(&mut self, input: PathBuf) -> std::io::Result<()> {
        // spin up workers
        self.spawn_multiple(WorkerKind::Map, self.workers_pool_size);
        self.spawn_multiple(WorkerKind::Reduce, self.workers_pool_size);

        // populate pending tasks
        self.pending_tasks = self.split_input_to_tasks(input).await;
        self.total_input_size = self.pending_tasks.len();

        let mut receiver = self.comm_channel.1.clone();
        let workers = self.workers.clone();
        let mut pending_tasks = self.pending_tasks.clone();
        let mut completed_tasks = self.completed_tasks.clone();
        let listener_handle = tokio::spawn(async move {
            let workers = workers.clone();

            let receiver = Arc::get_mut(&mut receiver).unwrap();
            while let Some(msg) = receiver.recv().await {
                match msg {
                    WorkerMessage::Pong { worker_id } => {
                        if let Some(worker) = workers.get(&worker_id) {
                            let mut worker = worker.lock().await;
                            worker.status = WorkerStatus::Busy;
                        }
                    }
                    WorkerMessage::TaskCompleted { task_id, output } => {
                        println!("task id: {} output >> {}", task_id, output);
                        let pending_tasks_ref = pending_tasks.clone();
                        let pending_tasks_mut = Arc::get_mut(&mut pending_tasks).unwrap();

                        if let Some(r_task) = pending_tasks_ref.get(&task_id) {
                            let mut task = r_task.lock().await;
                            match task.kind {
                                TaskKind::Map => {
                                    // prepare for reduce and change status to idle so the scheduler can pick it up
                                    task.kind = TaskKind::Reduce;
                                    task.status = TaskStatusKind::Idle;
                                }
                                TaskKind::Reduce => {
                                    // remove from pending task and insert into the completed task list
                                    pending_tasks_mut.remove(&task_id);
                                    let completed_tasks =
                                        Arc::get_mut(&mut completed_tasks).unwrap();
                                    completed_tasks.insert(task_id, r_task.clone());
                                }
                            }
                        } else {
                            println!(
                                "[WorkerMessage::TaskCompleted] task with id: `{}` not found",
                                task_id
                            )
                        }
                    }
                }
            }
        });

        let mut workers = self.workers.clone();
        let comm_sender = self.comm_channel.0.clone();
        let ping_handle = tokio::spawn(async move {
            loop {
                let workers_clone = workers.clone();
                for (worker_id, r_worker) in workers_clone.iter() {
                    let mut worker = r_worker.lock().await;
                    match worker.status {
                        WorkerStatus::Failed => {
                            if let Some(task) = worker.task.clone() {
                                // if worker has task, reset task worker id, and status back to Idle so other workers can pick it up
                                let mut task = task.lock().await;
                                task.status = TaskStatusKind::Idle;
                            }

                            let replacement =
                                Worker::new(*worker_id, worker.kind, comm_sender.clone());
                            let workers = Arc::get_mut(&mut workers).unwrap();
                            workers.insert(*worker_id, Arc::new(Mutex::new(replacement)));

                            let worker = r_worker.clone();
                            let worker = Arc::try_unwrap(worker).unwrap().into_inner();
                            worker.destroy();
                        }
                        WorkerStatus::Busy => {
                            // NOTE: busy worker as expected to send back a `Busy` message that reverts their state back to busy
                            // NOTE: otherwise, we would assume no-response means worker has failed and clean it up in the next cycle
                            worker.status = WorkerStatus::Failed;
                            worker.ping().await.unwrap();
                        }
                        _ => continue,
                    }
                }

                sleep(Duration::from_millis(PING_CYCLE_DELAY)).await;
            }
        });

        self.schedule_tasks().await?;

        let _ = listener_handle.await;
        let _ = ping_handle.await;

        Ok(())
    }
}

pub struct MapReduce;

impl MapReduce {
    pub async fn run(file: PathBuf) -> std::io::Result<()> {
        let mut master = Master::new(None, None);
        master.run(file).await?;

        Ok(())
    }
}
