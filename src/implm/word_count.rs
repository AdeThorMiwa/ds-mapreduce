pub fn map(_key: String, value: String, emit: fn(String, String)) {
    for line in value.lines() {
        for word in line.split_ascii_whitespace() {
            emit(word.to_owned(), String::from("1"))
        }
    }
}

pub fn reduce(_key: String, _value: Vec<String>, _emit: fn(String, String)) {}
