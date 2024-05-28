use map_reduce::{implm::word_count::WordCount, map_reduce::map_reduce};
use std::path::PathBuf;

fn main() {
    map_reduce(PathBuf::from("word.txt"), WordCount)
}
