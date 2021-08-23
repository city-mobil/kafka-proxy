use crate::segment::Segment;
use crate::record::Record;
use chrono::Local;
use std::path::Path;
use std::io::Error;
use std::sync::Arc;

mod segment;
mod record;

pub struct WAL {
    // Size of segment in Kb
    segment_size: u64, // todo use &'static u64

    // How many bytes written
    bytes_written: u64,

    // Directory for collecting write ahead log
    wal_dir: &'static str,

    // Current active segment file
    current_segment: Option<Arc<Segment>>,
}

impl WAL {
    pub fn new() -> WAL {
        Default::default()
    }

    pub fn write_record(&mut self, record: Record) -> Result<(), Error> {
        if self.current_segment.is_none() {
            let segment = match self.create_segment() {
                Ok(s) => s,
                Err(err) => {
                    return Err(err)
                }
            };
            self.current_segment = Some(Arc::new(segment));
        }

        let buf_vec = bincode::serialize(&record).unwrap();
        let buf = buf_vec.as_slice();
        let buf_size = buf.len() as u64;
        if buf_size + self.bytes_written > self.segment_size {
            let segment = match self.create_segment() {
                Ok(s) => s,
                Err(err) => {
                    return Err(err)
                }
            };
            self.current_segment = Some(Arc::new(segment));
            self.bytes_written = buf_size;
        } else {
            self.bytes_written += buf_size;
        }

        let mut current_segment = self.current_segment.as_ref().unwrap();
        current_segment.write(buf);

        Ok(())
    }

    fn create_segment(&self) -> Result<Segment, Error> {
        let file_name = Local::now().format("%Y%m%d%H%M").to_string();
        let file_path = Path::new(self.wal_dir).join(file_name);

        Segment::create(file_path, self.segment_size)
    }
}

impl Default for WAL {
    fn default() -> Self {
        WAL {
            segment_size: 4096,
            bytes_written: 0,
            wal_dir: "wal-segments",
            current_segment: None,
        }
    }
}