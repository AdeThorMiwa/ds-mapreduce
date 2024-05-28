use std::{
    collections::HashMap,
    fs::{create_dir_all, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{mpsc, Arc, Mutex},
    thread::{self, sleep},
    time::Duration,
};

use rand::Rng;

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

fn split_file_to_chunks(file_path: PathBuf, chunk_size: usize) -> Vec<PathBuf> {
    let mut file = File::open(&file_path).expect("invalid file");
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
        create_dir_all(TEMP_DIR).expect("failed to create tmp directory")
    }

    while let Ok(_) = file.read_exact(&mut buf) {
        let chunk_filename = format!(
            "{}/{}-chunk-{}.{}",
            TEMP_DIR, filename, chunk_index, file_ext
        );
        let chunk_file_path = PathBuf::from(chunk_filename);
        let mut chunk_file = File::create(&chunk_file_path)
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

fn write_result(result: Vec<(String, u32)>, dir: &str) -> String {
    let random_number = rand::thread_rng().gen_range((u32::MAX as u64)..u64::MAX);
    let chunk_filename = format!("{}/chunk-{}.txt", dir, random_number); // What format should intermediate file be saved in
    let chunk_file_path = PathBuf::from(&chunk_filename);

    if !Path::new(dir).exists() {
        create_dir_all(dir).expect("failed to create intermediate directory")
    }

    let mut chunk_file = File::create(&chunk_file_path)
        .expect(format!("failed to create chunk file {}", chunk_filename).as_str());
    let _ = chunk_file.write_all(&mut result_to_buf(result).as_bytes());
    chunk_filename
}

fn retrieve_parsed_intermediate_file(filename: &str) -> Vec<(String, u32)> {
    let file_path = PathBuf::from(filename);
    let mut buf = String::new();
    let mut file = File::open(file_path).expect("failed to oopen intermediate file");
    let _ = file.read_to_string(&mut buf);

    let mut parsed = vec![];
    for line in buf.lines() {
        if let Some((key, value)) = line.split_once(" ") {
            parsed.push((key.to_owned(), value.parse::<u32>().expect("invalid u32")))
        }
    }

    parsed
}

pub fn map_reduce<
    F: MapReducer<MapKey = String, MapValue = String, ReduceKey = String, ReduceValue = u32>,
>(
    file: PathBuf,
    func: F,
) where
    F: Sync + Send + 'static,
{
    // make workers into struct
    let func = Arc::new(Mutex::new(func));
    let files = split_file_to_chunks(file, 512);
    let (m_sender, m_receiver) = mpsc::channel();

    let mut map_workers_join_handle = Vec::new();
    for file_path in files {
        let func = func.clone();
        let sender = m_sender.clone();
        let handle = thread::spawn(move || {
            let mut buf = String::new();
            let mut file = File::open(&file_path)
                .expect(format!("failed to open file chunk {:?}", &file_path.file_name()).as_str());
            let _ = file.read_to_string(&mut buf);

            let filename = file_path
                .file_name()
                .expect("could not get filename")
                .to_str()
                .expect("file name to_str failed");

            let result = func
                .lock()
                .expect("unable to retrieve lock on func")
                .map(filename.to_owned(), buf);

            let file_name = write_result(result, INTERMEDIATE_DIR);

            sender.send(file_name).expect("failed to emit map result")
        });

        map_workers_join_handle.push(handle)
    }

    let mut reducer_workers_join_handle = Vec::new();
    while let Ok(intermediate_file_name) = m_receiver.recv() {
        // println!("[master] intermediate output");
        // println!("[master] {:?}", completed_task);
        let func = func.clone();
        let handle = thread::spawn(move || {
            let parsed = retrieve_parsed_intermediate_file(&intermediate_file_name);
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
                let value = func
                    .lock()
                    .expect("failed to retrieve lock on func")
                    .reduce(key.clone(), values);
                reduce_result.push((key, value));
            }

            write_result(reduce_result, OUTPUT_DIR)
        });

        reducer_workers_join_handle.push(handle)
    }

    for handle in map_workers_join_handle {
        handle.join().unwrap();
    }

    for handle in reducer_workers_join_handle {
        handle.join().unwrap();
    }
}

#[derive(Clone, Copy, Debug)]
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

enum TaskStatusKind {
    Idle,
    InProgress,
    Completed,
}

enum TaskKind {
    Map,
    Reduce,
}

struct Task {
    id: usize,
    worker_id: Option<usize>,
    status: TaskStatusKind,
    kind: TaskKind,
    input: PathBuf,
}

impl Task {
    pub fn new(id: usize, kind: TaskKind, input: PathBuf) -> Self {
        Self {
            id,
            worker_id: None,
            status: TaskStatusKind::Idle,
            kind,
            input,
        }
    }
}

#[derive(Debug)]
struct Worker {
    id: usize,
    kind: WorkerKind,
    status: WorkerStatus,
    task_id: Option<usize>,
}

impl Worker {
    pub fn new(id: usize, kind: WorkerKind) -> Self {
        Self {
            id,
            kind,
            status: WorkerStatus::Idle,
            task_id: None,
        }
    }

    pub fn ping(&self) -> Result<(), ()> {
        Err(())
    }

    pub fn destroy(self) {}
}

enum CommMessage {
    Ping { worker_id: usize },
    Pong { worker_id: usize },
}

trait CommunicationChannel: Sync + Send {
    fn send(&self) -> std::io::Result<()>;
    fn listen(&self) -> Result<CommMessage, ()>;
}

struct SimpleMPSCChannel {}

impl SimpleMPSCChannel {
    pub fn new() -> Self {
        Self {}
    }
}

impl CommunicationChannel for SimpleMPSCChannel {
    fn listen(&self) -> Result<CommMessage, ()> {
        Err(())
    }

    fn send(&self) -> std::io::Result<()> {
        Ok(())
    }
}

struct Master {
    comm_channel: Arc<dyn CommunicationChannel>,
    tasks: Arc<HashMap<usize, Arc<Mutex<Task>>>>,
    workers: Arc<HashMap<usize, Arc<Mutex<Worker>>>>,
}

impl Master {
    pub fn new(input: PathBuf, worker_size: Option<usize>, map_chunk_size: Option<usize>) -> Self {
        let comm_channel = Arc::new(SimpleMPSCChannel::new());

        let mut tasks = Arc::new(HashMap::new());

        split_file_to_chunks(input, map_chunk_size.unwrap_or(512))
            .into_iter()
            .enumerate()
            .for_each(|(task_id, task_input)| {
                let task = Arc::new(Mutex::new(Task::new(task_id, TaskKind::Map, task_input)));
                let tasks = Arc::get_mut(&mut tasks).unwrap();
                tasks.insert(task_id, task);
            });

        let mut master = Self {
            comm_channel,
            tasks,
            workers: Arc::new(HashMap::new()),
        };

        let worker_size = worker_size.unwrap_or(10);
        master.spawn_multiple(WorkerKind::Map, worker_size);
        master.spawn_multiple(WorkerKind::Reduce, worker_size);

        master
    }

    pub fn spawn(&mut self, kind: WorkerKind) {
        let workers = Arc::get_mut(&mut self.workers).unwrap();
        let worker_id = workers.len();
        let worker = Worker::new(worker_id, kind);
        workers.insert(worker_id, Arc::new(Mutex::new(worker)));
    }

    pub fn spawn_multiple(&mut self, kind: WorkerKind, n: usize) {
        (0..n).into_iter().for_each(|_| {
            self.spawn(kind);
        })
    }

    pub fn listen(&mut self) -> std::io::Result<()> {
        let comm_channel = Arc::clone(&self.comm_channel);
        let workers = Arc::clone(&self.workers);
        let listener_handle = thread::spawn(move || {
            let workers = Arc::clone(&workers);

            while let Ok(msg) = comm_channel.listen() {
                match msg {
                    CommMessage::Pong { worker_id } => {
                        if let Some(worker) = workers.get(&worker_id) {
                            let mut worker = worker.lock().unwrap();
                            worker.status = WorkerStatus::Busy;
                        }
                    }
                    _ => {}
                }
            }
        });

        let tasks = Arc::clone(&self.tasks);
        let mut workers = Arc::clone(&self.workers);
        let ping_handle = thread::spawn(move || loop {
            let workers_clone = Arc::clone(&workers);
            for (worker_id, r_worker) in workers_clone.iter() {
                let mut worker = r_worker.lock().unwrap();
                match worker.status {
                    WorkerStatus::Failed => {
                        if let Some(task_id) = worker.task_id {
                            // if worker has task, reset task worker id, and status back to Idle so other workers can pick it up
                            if let Some(task) = tasks.get(&task_id) {
                                let mut task = task.lock().unwrap();
                                task.worker_id = None;
                                task.status = TaskStatusKind::Idle;
                            }
                        }

                        let replacement = Worker::new(*worker_id, worker.kind);
                        let workers = Arc::get_mut(&mut workers).unwrap();
                        workers.insert(*worker_id, Arc::new(Mutex::new(replacement)));

                        let worker = Arc::clone(r_worker);
                        let worker = Arc::try_unwrap(worker).unwrap().into_inner().unwrap();
                        worker.destroy();
                    }
                    WorkerStatus::Busy => {
                        // NOTE: busy worker as expected to send back a `Busy` message that reverts their state back to busy
                        // NOTE: otherwise, we would assume no-response means worker has failed and clean it up in the next cycle
                        worker.status = WorkerStatus::Failed;
                        worker.ping().unwrap();
                    }
                    _ => continue,
                }
            }

            sleep(Duration::from_millis(PING_CYCLE_DELAY))
        });

        let _ = listener_handle.join();
        let _ = ping_handle.join();

        Ok(())
    }
}

pub struct MapReduce;

impl MapReduce {
    pub fn run(
        file: PathBuf,
        func: Box<
            dyn MapReducer<
                MapKey = String,
                MapValue = String,
                ReduceKey = String,
                ReduceValue = u32,
            >,
        >,
    ) -> std::io::Result<()> {
        let mut master = Master::new(file, None, None);

        master.listen()?;

        Ok(())
    }
}
