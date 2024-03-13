use futures::future::join_all;
use std::time::Instant;
use std::{
    hash::{DefaultHasher, Hash, Hasher},
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct DB {
    storage: PathBuf,
}

fn calculate_hash<T: Hash + ?Sized>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

trait Storage {
    async fn store(&self, key: &String, val: &String);
    async fn retrive(&self, key: &String) -> Option<String>;
}

impl Storage for DB {
    async fn store(&self, key: &String, val: &String) {
        let file_name = format!(
            "{}/{}",
            &self.storage.to_str().unwrap(),
            &calculate_hash(&key).to_string()
        );

        let path = Path::new(&file_name);

        // Check if the file exists
        let file_exists = tokio::fs::metadata(path).await.is_ok();

        let file_result = if file_exists {
            OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(path)
                .await
                .ok()
        } else {
            File::create(path).await.ok()
        };

        if let Some(mut file) = file_result {
            // Attempt to write the new content to the file, ignoring errors
            let _ = file.write_all(val.as_bytes()).await.ok();
        }
    }
    async fn retrive(&self, key: &String) -> Option<String> {
        let file_name = format!(
            "{}/{}",
            &self.storage.to_str().unwrap(),
            &calculate_hash(&key).to_string()
        );

        let path = Path::new(&file_name);

        let mut file = match File::open(path).await {
            Ok(file) => file,
            Err(_) => return None,
        };

        let mut content = String::new();

        match file.read_to_string((&mut content)).await {
            Ok(_) => Some(content),
            Err(e) => None,
        }
    }
}

impl DB {
    async fn new(path: String) -> Self {
        let _path = PathBuf::from(&path);

        if let Err(e) = fs::create_dir(&path).await {
            if e.kind() != ErrorKind::AlreadyExists {
                panic!("{:?}", e)
            }
        }

        DB {
            storage: PathBuf::from(path),
        }
    }

    async fn set(&self, key: &String, val: &String) {
        self.store(key, val).await
    }

    async fn get(&self, key: &String) -> Option<String> {
        self.retrive(key).await
    }
}

async fn write_5m_keys_parallel(db: Arc<DB>) {
    let mut handles = Vec::new();

    let start = Instant::now();
    let key_count = 5_000_000;

    for i in 0..key_count {
        let db = db.clone();
        let key = i.to_string();
        let val = i.to_string();

        // Spawn a new task for each write operation
        let handle = tokio::task::spawn(async move { db.set(&key, &val).await });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    let results = join_all(handles).await;
    let duration = start.elapsed();

    // Optionally, process the results of each task
    for result in results {
        match result {
            Ok(_) => println!("Writen {} keys in {} ms", key_count, duration.as_millis()),
            Err(e) => eprintln!("Error writing to the database: {:?}", e),
        }
    }
}

async fn avarage_time_taken(db: Arc<DB>) {
    let mut total = 0;
    let mut count = 0;
    for i in 1000..100000 {
        let start = Instant::now();
        let _ = db.get(&"10000".to_string()).await;
        let duration = start.elapsed();

        count += 1;
        total += duration.as_micros();
    }

    if count > 0 {
        let average = total / count;
        println!("Average time taken: {} microseconds", average);
    } else {
        println!("No operations were performed.");
    }
}

#[tokio::main]
async fn main() {
    // Benchmarking
    // Write 5m random key value pairs
    // Read one random key value pairs

    let db = DB::new("test".to_string()).await;

    let db_arc = Arc::new(db);

    //write_5m_keys_parallel(db_arc.clone()).await;

    //println!("Writen 5 m key in db");

    avarage_time_taken(db_arc.clone()).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_set() {
        let db = DB::new("test".to_string()).await;
        db.set(&"Samet".to_string(), &"Samet".to_string()).await;
    }

    #[tokio::test]
    async fn test_get() {
        let db = DB::new("test".to_string()).await;
        db.set(&"Samet".to_string(), &"Samet".to_string()).await;
        let result = db.get(&"Samet".to_string()).await;
        assert_eq!(result, Some("Samet".to_string()));
    }
}
