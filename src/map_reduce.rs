use crate::master::Master;
use std::path::PathBuf;

pub trait MapReducer {
    fn map(&self, key: String, value: String) -> Vec<(String, String)>;
    fn reduce(&self, key: String, value: Vec<String>) -> String;
}

pub struct MapReduce;

impl MapReduce {
    pub async fn run(file: PathBuf) -> std::io::Result<()> {
        let mut master = Master::new(None, None);
        master.run(file).await?;

        Ok(())
    }
}
