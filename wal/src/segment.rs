use std::path::{PathBuf, Path};
use std::io::{BufWriter, Error, Write};
use std::fs::{File, OpenOptions};
use std::io;
use chrono::Local;

// Сегментом является отдельный файл в котором есть записи
pub struct Segment {
    path: PathBuf,
    file: File,
}

impl Segment {
    // Создаём сегмент по указанному пути
    pub fn create(path: PathBuf, capacity: u64) -> Result<Segment, Error> {
        let capacity = capacity & !7;

        let file = match OpenOptions::new()
            .append(true)
            .write(true)
            .create(true)
            .open(path.clone()) {
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
            path,
            file
        })
    }

    pub fn write(&mut self, buf: &[u8]) {
        self.file.write(buf);
    }
}
