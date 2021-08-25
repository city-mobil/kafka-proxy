pub mod wal {
    use chrono::Local;
    use std::path::{Path, PathBuf};
    use std::io::Error;
    use std::sync::Arc;
    use std::fs::{File, rename, OpenOptions, create_dir_all};
    use std::borrow::BorrowMut;
    use std::cell::Cell;
    use serde::{Serialize, Deserialize};
    use std::io::Write;

    pub struct WAL {
        // Size of segment in Kb
        segment_size: u64, // todo use &'static u64

        // How many bytes written
        bytes_written: u64,

        // Directory for collecting write ahead log
        wal_dir: &'static str,

        // Current active segment file
        current_segment: Cell<Segment>,

        // Metadata for segments usage after shutting down
        segment_meta_file: Option<File>
    }

    impl WAL {
        const DEFAULT_SEGMENT_SIZE: u64 = 2 << 19; // todo use smaller value
        const WAL_DIR: &'static str = "wal-segments";

        pub fn new() -> WAL {
            Default::default()
        }

        pub fn write_record(&mut self, record: Record) -> Result<(), Error> {
            let buf_vec = bincode::serialize(&record).unwrap();
            let buf = buf_vec.as_slice();
            let buf_size = buf.len() as u64;
            if buf_size + self.bytes_written > self.segment_size {
                let mut segment = match self.create_segment() {
                    Ok(s) => s,
                    Err(err) => {
                        return Err(err)
                    }
                };
                self.current_segment.get_mut().close();
                self.current_segment = Cell::new(segment);
                self.bytes_written = 0;
            }
            self.bytes_written += buf_size;
            self.current_segment.get_mut().write(buf);

            Ok(())
        }

        fn create_segment(&self) -> Result<Segment, Error> {
            Segment::create(self.wal_dir.to_string(), self.segment_size)
        }
    }

    impl Default for WAL {
        fn default() -> Self {
            let current_segment = Cell::new(
                Segment::create(
                    WAL::WAL_DIR.to_string(),
                    WAL::DEFAULT_SEGMENT_SIZE
                ).unwrap()
            );
            WAL {
                segment_size: WAL::DEFAULT_SEGMENT_SIZE,
                bytes_written: 0,
                wal_dir: WAL::WAL_DIR,
                current_segment,
                segment_meta_file: None,
            }
        }
    }

    // Сегментом является отдельный файл в котором есть записи
    pub struct Segment {
        active_path: PathBuf,
        closed_path: PathBuf,
        file: File,
    }

    impl Segment {
        fn generate_file_path(wal_dir: &String, file_name: &String) -> PathBuf {
            Path::new(wal_dir).join(file_name)
        }

        pub fn generate_active_file_name() -> String {
            Local::now().format("%Y%m%d%H%M%S%.3f.a").to_string()
        }

        pub fn generate_closed_file_name() -> String {
            Local::now().format("%Y%m%d%H%M%S%.3f").to_string()
        }

        // Создаём сегмент по указанному пути
        // Файл будет создан с указанным размером capacity
        pub fn create(wal_dir: String, capacity: u64) -> Result<Segment, Error> {
            create_dir_all(&wal_dir);
            let active_file_name = Self::generate_active_file_name();
            let active_path = Self::generate_file_path(&wal_dir, &active_file_name);

            let closed_file_name = Self::generate_closed_file_name();
            let closed_path = Self::generate_file_path(&wal_dir, &closed_file_name);

            let capacity = capacity & !7;

            let file = match OpenOptions::new()
                .append(true)
                .write(true)
                .create(true)
                .open(active_path.clone()) {
                Ok(file) => file,
                Err(err) => {
                    return Err(err)
                },
            };

            // Попытка задать размер нашему сенгменту
            match file.set_len(capacity) {
                Ok(_) => {}
                Err(err) => {
                    return Err(err)
                }
            };


            Ok(Segment {
                active_path,
                closed_path,
                file
            })
        }

        pub fn write(&mut self, buf: &[u8]) {
            self.file.write(buf);
        }

        pub fn close(&self) {
            rename(self.active_path.clone(), self.closed_path.clone());
        }
    }

    #[derive(Serialize, Deserialize)]
    pub struct Record {
        pub topic: String,
        pub data: String,
        pub key: Option<String>
    }

    impl Record {
        pub fn new(topic: String, data: String, key: Option<String>) -> Record {
            Record { topic, data, key }
        }
    }
}
