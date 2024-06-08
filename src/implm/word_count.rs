use crate::map_reduce::MapReducer;

pub struct WordCount;

impl MapReducer for WordCount {
    fn map(&self, _key: String, value: String) -> Vec<(String, String)> {
        let mut v = Vec::new();

        for line in value.lines() {
            for word in line.split_ascii_whitespace() {
                v.push((word.to_owned(), String::from("1")))
            }
        }

        v
    }

    fn reduce(&self, _key: String, value: Vec<String>) -> String {
        let mut i = 0;
        for v in value {
            i += v.parse::<i32>().unwrap();
        }
        format!("{i}")
    }
}
