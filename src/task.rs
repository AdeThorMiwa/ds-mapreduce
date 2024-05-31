use std::path::PathBuf;

#[derive(Debug, PartialEq)]
pub enum TaskStatusKind {
    Idle,
    InProgress { worker_id: usize },
    Completed,
}

#[derive(Eq, PartialEq, Debug, Clone, Copy)]
pub enum TaskKind {
    Map,
    Reduce,
}

#[derive(Debug)]
pub enum TaskMessage {}

#[derive(Debug)]
pub struct Task {
    pub id: usize,
    pub status: TaskStatusKind,
    pub kind: TaskKind,
    pub input: PathBuf,
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
