use crate::map_reduce::MapReducer;

pub struct WordCount;

impl MapReducer for WordCount {
    type MapKey = String;
    type MapValue = String;
    type ReduceKey = String;
    type ReduceValue = u32;

    fn map(
        &self,
        _key: Self::MapKey,
        value: Self::MapValue,
    ) -> Vec<(Self::ReduceKey, Self::ReduceValue)> {
        let mut v = Vec::new();

        for line in value.lines() {
            for word in line.split_ascii_whitespace() {
                v.push((word.to_owned(), 1 as u32))
            }
        }

        v
    }

    fn reduce(&self, _key: Self::ReduceKey, value: Vec<Self::ReduceValue>) -> Self::ReduceValue {
        let mut i = 0;
        for v in value {
            i += v;
        }
        i
    }
}
