use map_reduce::map_reduce::MapReduce;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    MapReduce::run(PathBuf::from("word.txt")).await
}
