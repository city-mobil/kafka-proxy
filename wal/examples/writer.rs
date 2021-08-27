use wal::wal::{WALWriter, Record, RecordStatus};

fn main() -> ! {
    let mut wal = WALWriter::new();

    loop {
        let record = Record {
            uuid: Default::default(),
            topic: "hello".to_string(),
            data: "world".to_string(),
            key: None,
            status: RecordStatus::Insert
        };
        match wal.write_record(record) {
            Ok(_) => {

            }
            Err(_) => {}
        };
    }
}
