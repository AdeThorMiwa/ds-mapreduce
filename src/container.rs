use std::{
    fs::{create_dir, read_to_string, remove_dir_all, OpenOptions},
    io::{stdout, Error, ErrorKind, Write},
    path::PathBuf,
    process::Command,
};

pub struct ContainerManager {
    dir: PathBuf,
    image_name: String,
}

impl ContainerManager {
    pub fn try_prepare(actor_file: PathBuf, name: &str) -> std::io::Result<Self> {
        Self::cleanup_existing_tmp_dir()?;
        println!("creating temp dir...");
        let temp_dir = Self::create_temp_dir()?;
        println!("copying template...");
        Self::copy_template_files_to_dir(&temp_dir)?;
        let template_src_dir = {
            let mut a = temp_dir.clone();
            a.push("src");
            a
        };
        println!("copying actor file to dir...");
        Self::copy_actor_file_to_dir(actor_file, template_src_dir)?;

        Ok(Self {
            dir: temp_dir,
            image_name: name.to_owned(),
        })
    }

    fn cleanup_existing_tmp_dir() -> std::io::Result<()> {
        let tmp_path = PathBuf::from("./tmp");
        if tmp_path.exists() {
            remove_dir_all(tmp_path)?;
        }
        Ok(())
    }

    fn create_temp_dir() -> std::io::Result<PathBuf> {
        let path = PathBuf::from("./tmp");
        create_dir(&path).map(|_| path)
    }

    fn copy_template_files_to_dir(dir: &PathBuf) -> std::io::Result<()> {
        Command::new("cp")
            .args(["-R", "./src/template/", dir.to_str().unwrap()])
            .spawn()?;
        Ok(())
    }

    fn copy_actor_file_to_dir(actor_file: PathBuf, dir: PathBuf) -> std::io::Result<()> {
        let file_content = read_to_string(actor_file)?;
        let mut actor_file_path = dir.clone();
        if !actor_file_path.exists() {
            create_dir(&actor_file_path)?;
        }
        actor_file_path.push("actor.rs");
        let mut actor_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(actor_file_path)?;
        actor_file.write_all(file_content.as_bytes())
    }

    pub fn containerize(&self) -> std::io::Result<String> {
        // check for docker file
        let docker_dir = {
            let mut dir = self.dir.clone();
            dir.push("Dockerfile");
            dir
        };

        if !docker_dir.exists() {
            return Err(Error::new(
                ErrorKind::Other,
                "Dockerfile is required in the given directory",
            ));
        }

        let child = Command::new("docker")
            .current_dir(&self.dir)
            .args(["build", "-t", &self.image_name, "."])
            .output()?;

        stdout().write_all(&child.stdout)?;
        Ok(self.image_name.to_owned())
    }
}
