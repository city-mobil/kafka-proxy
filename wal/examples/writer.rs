use wal::wal::{WAL, Record};

fn main() -> ! {
    let mut wal = WAL::default();

    loop {
        let record = Record {
            topic: "hello".to_string(),
            data: "world".to_string(),
            key: None
        };
        match wal.write_record(record) {
            Ok(_) => {

            }
            Err(_) => {}
        };
    }
}
