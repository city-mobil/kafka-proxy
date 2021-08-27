pub mod wal {
    use chrono::Local;
    use std::path::{Path, PathBuf};
    use std::io::{Error, Read, BufReader, BufRead, BufWriter};
    use std::sync::Arc;
    use std::fs::{File, rename, OpenOptions, create_dir_all, remove_file, DirEntry};
    use std::borrow::BorrowMut;
    use std::cell::Cell;
    use serde::{Serialize, Deserialize};
    use std::io::Write;
    use uuid::Uuid;

    // WAL Writer
    pub struct WALWriter {
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

    impl WALWriter {
        const DEFAULT_SEGMENT_SIZE: u64 = 2 << 19; // todo use smaller value
        const WAL_DIR: &'static str = "wal-segments";

        pub fn new() -> WALWriter {
            Default::default()
        }

        pub fn write_record(&mut self, record: Record) -> Result<(), Error> {
            let buf_vec = serde_json::to_vec(&record).unwrap();
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
            Segment::create(Path::new(self.wal_dir), self.segment_size)
        }
    }

    impl Default for WALWriter {
        fn default() -> Self {
            let current_segment = Cell::new(
                Segment::prepare_env_and_create(
                    WALWriter::WAL_DIR.to_string(),
                    WALWriter::DEFAULT_SEGMENT_SIZE
                ).unwrap()
            );
            WALWriter {
                segment_size: WALWriter::DEFAULT_SEGMENT_SIZE,
                bytes_written: 0,
                wal_dir: WALWriter::WAL_DIR,
                current_segment,
                segment_meta_file: None,
            }
        }
    }

    // WAL Reader
    pub struct WALReader {
        // Size of segment in Kb
        segment_size: u64, // todo use &'static u64

        // Directory for collecting write ahead log
        wal_dir: &'static str,

        // Current active segment file
        current_segment: Cell<Segment>,
    }

    impl WALReader {
        pub fn new() -> WALReader {
            WALReader {
                ..Default::default()
            }
        }

        pub fn read_records(&mut self) -> Vec<Record> {
            // todo находим нужный нам сегмент если таковой есть
            // затем вычитываем из него всё в память
            // проходим по каждой записи, отправляем их в кафку пачками и записываем результат в файл / ок не ок
            // после переходим к следующей пачке
            // когда все записи кончились, файл удаляется или переносится под другой экстеншн
            // ищем новый закрытый файл, создаём сегмент, переходим вначало
            self.current_segment.get_mut().read_records()
        }
    }

    impl Default for WALReader {
        fn default() -> Self {
            let current_segment = Cell::new(
                Segment::prepare_env_and_create(
                    WALWriter::WAL_DIR.to_string(),
                    WALWriter::DEFAULT_SEGMENT_SIZE
                ).unwrap()
            );
            WALReader {
                segment_size: WALWriter::DEFAULT_SEGMENT_SIZE,
                wal_dir: WALWriter::WAL_DIR,
                current_segment,
            }
        }
    }

    // Segment
    pub struct Segment {
        file_path: PathBuf,
        file_writer: BufWriter<File>,
        file_reader: BufReader<File>
    }

    impl Segment {
        const ACTIVE_SEGMENT_EXTENSION: &'static str = "A";
        const CLOSED_SEGMENT_EXTENSION: &'static str = "C";

        pub fn generate_file_name() -> String {
            let mut file_name = Local::now().format("%Y%m%d%H%M%S%3f.").to_string();
            file_name.push_str(Self::ACTIVE_SEGMENT_EXTENSION);
            file_name
        }

        // Create new segment in `wal_dir` with specific `capacity`
        // All active segments will close
        // If wal_dir does not exist, it will be created
        pub fn prepare_env_and_create(wal_dir: String, capacity: u64) -> Result<Segment, Error> {
            let wal_dir = Path::new(&wal_dir);
            if wal_dir.exists() {
                Self::close_active_segments(wal_dir);
            } else {
                create_dir_all(&wal_dir);
            }
            Self::create(wal_dir, capacity)
        }

        pub fn load_latest_segment(wal_dir: &Path) -> Result<Segment, Error> {

        }

        fn close_active_segments(wal_dir: &Path) {
            let files = wal_dir.read_dir().unwrap();
            for file in files {
                match file {
                    Ok(file) => {
                        let file_path = file.path();
                        let ext = file_path.extension();
                        if ext.is_none() {
                            continue;
                        }
                        if ext.unwrap() == Self::ACTIVE_SEGMENT_EXTENSION {
                            match Segment::load(file.path()) {
                                Ok(segment) => {
                                    segment.close();
                                }
                                Err(err) => {
                                    // todo logging error
                                }
                            }
                        }
                    }
                    Err(err) => {
                        // todo logging error
                    }
                };
            }
        }

        fn create(wal_dir: &Path, capacity: u64) -> Result<Segment, Error> {
            let file_name = Self::generate_file_name();
            let file_path = wal_dir.join(file_name);

            let capacity = capacity & !7;

            let file = match OpenOptions::new()
                .append(true)
                .write(true)
                .create(true)
                .open(file_path.clone()) {
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

            let file_reader = BufReader::new(file.try_clone().unwrap());
            let file_writer = BufWriter::new(file);

            Ok(Segment {
                file_path,
                file_reader,
                file_writer,
            })
        }

        pub fn write(&mut self, buf: &[u8]) {
            self.file_writer.write_all(buf);
            self.file_writer.write("\n".as_bytes());
        }

        pub fn close(&self) {
            let mut closed_segment = self.file_path.clone();
            closed_segment.set_extension(Self::CLOSED_SEGMENT_EXTENSION);
            rename(self.file_path.clone(), closed_segment);
        }

        // Load segment by path
        pub fn load(file_path: PathBuf) -> Result<Segment, Error> {
            let file = match OpenOptions::new()
                .read(true)
                .open(file_path.clone()) {
                Ok(file) => file,
                Err(err) => {
                    return Err(err)
                },
            };

            let file_reader = BufReader::new(file.try_clone().unwrap());
            let file_writer = BufWriter::new(file);

            return Ok(Segment {
                file_path,
                file_reader,
                file_writer
            })
        }

        pub fn read_records(&mut self) -> Vec<Record> {
            let mut records = vec![];

            for line in self.file_reader.by_ref().lines() {
                match line {
                    Ok(l) => {
                        records.push(serde_json::from_str(l.as_str()).unwrap());
                    }
                    Err(_) => {
                        // todo logging error
                    }
                }
            };
            return records
        }

        // Remove segment if all records executed
        pub fn remove(&self) {
            remove_file(self.file_path.clone());
        }
    }

    #[derive(Serialize, Deserialize)]
    pub enum RecordStatus {
        Insert,
        Completed,
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct Record {
        pub uuid: Uuid,
        pub topic: String,
        pub data: String,
        pub key: Option<String>,
        pub status: RecordStatus,
    }

    impl Record {
        pub fn new(topic: String, data: String, key: Option<String>) -> Record {
            Record { topic, data, key, ..Default::default() }
        }

        fn complete(&mut self) {
            self.status = RecordStatus::Completed;
        }
    }

    impl Default for Record {
        fn default() -> Self {
            Record {
                uuid: Uuid::new_v4(),
                topic: "".to_string(),
                data: "".to_string(),
                key: None,
                status: RecordStatus::Insert,
            }
        }
    }
}
