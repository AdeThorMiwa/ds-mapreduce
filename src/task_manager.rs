use crate::task::{Task, TaskStatusKind};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

pub struct TaskManager {
    tasks: Arc<Mutex<HashMap<usize, Arc<Mutex<Task>>>>>,
}

impl TaskManager {
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get_pending_tasks(&self) -> Vec<Arc<Mutex<Task>>> {
        let mut tasks = Vec::new();
        let all_tasks = self.tasks.lock().await;
        for task in all_tasks.values() {
            if let TaskStatusKind::InProgress { .. } = task.lock().await.status {
                tasks.push(task.clone())
            }
        }
        tasks
    }

    pub async fn get_completed_tasks(&self) -> Vec<Arc<Mutex<Task>>> {
        let mut tasks = Vec::new();
        let all_tasks = self.tasks.lock().await;
        for task in all_tasks.values() {
            if let TaskStatusKind::Completed = task.lock().await.status {
                tasks.push(task.clone())
            }
        }
        tasks
    }

    pub async fn get_task_by_id(&self, task_id: usize) -> Option<Arc<Mutex<Task>>> {
        let tasks = self.tasks.lock().await;
        tasks.get(&task_id).map(|t| t.clone())
    }

    pub async fn add_task(&self, task: Task) {
        self.tasks
            .lock()
            .await
            .insert(task.id, Arc::new(Mutex::new(task)));
    }

    pub async fn add_tasks(&self, tasks: Vec<Task>) {
        for task in tasks {
            self.add_task(task).await;
        }
    }

    pub async fn move_task_to_completed(&self, task_id: usize) {
        if let Some(task) = self.tasks.lock().await.get(&task_id) {
            let mut task = task.lock().await;
            task.completed();
        }
    }

    pub async fn move_task_to_idle(&self, task_id: usize, output: String) {
        if let Some(task) = self.tasks.lock().await.get(&task_id) {
            let mut task = task.lock().await;
            task.status = TaskStatusKind::Idle;
            task.input = PathBuf::from(output)
        }
    }
}

impl Clone for TaskManager {
    fn clone(&self) -> Self {
        Self {
            tasks: self.tasks.clone(),
        }
    }
}
