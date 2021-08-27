use wal::wal::{Record, RecordStatus, WALReader};

fn main() -> ! {
    let mut wal = WALReader::new();

    loop {
        let records = wal.read_records();
        for record in records {
            // todo logging record info
        }
    }
}
