use crate::master::Master;
use std::path::PathBuf;

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

pub struct MapReduce;

impl MapReduce {
    pub async fn run(file: PathBuf) -> std::io::Result<()> {
        let mut master = Master::new(None, None);
        master.run(file).await?;

        Ok(())
    }
}
