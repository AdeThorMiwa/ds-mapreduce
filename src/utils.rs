use crate::constants::TEMP_DIR;
use rand::Rng;
use std::{
    fs::{create_dir_all, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};

pub async fn split_file_to_chunks(file_path: PathBuf, chunk_size: usize) -> Vec<PathBuf> {
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

fn result_to_buf(result: Vec<(String, String)>) -> String {
    let mut buf = String::new();
    for (key, value) in result {
        buf.push_str(format!("{} {}\n", key, value).as_str())
    }
    buf
}

pub fn get_file_name(path: &PathBuf) -> std::io::Result<String> {
    let filename = path
        .file_name()
        .ok_or(std::io::ErrorKind::InvalidInput)?
        .to_str()
        .ok_or(std::io::ErrorKind::InvalidInput)?;
    Ok(filename.to_owned())
}

pub async fn write_result(result: Vec<(String, String)>, dir: &str) -> String {
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

pub async fn retrieve_parsed_intermediate_file(file_path: &PathBuf) -> Vec<(String, String)> {
    let mut buf = String::new();
    let mut file = File::open(file_path).expect("failed to oopen intermediate file");
    let _ = file.read_to_string(&mut buf);

    let mut parsed = vec![];
    for line in buf.lines() {
        if let Some((key, value)) = line.split_once(" ") {
            parsed.push((key.to_owned(), value.to_owned()))
        }
    }

    parsed
}
