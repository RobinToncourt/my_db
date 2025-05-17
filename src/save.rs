use std::fs::File;
use std::io;
use std::io::Write;

use crate::TABLE;

#[cfg_attr(debug_assertions, derive(Debug))]
pub enum WriteTableToDiskError {
    IoError(io::Error),
    PoisonedTable,
    NotAllBytesWritten,
}

pub fn write_table_to_disk(path: &str) -> Result<(), WriteTableToDiskError> {
    let mut file = File::create(path).map_err(WriteTableToDiskError::IoError)?;
    let Ok(table) = TABLE.lock() else {
        return Err(WriteTableToDiskError::PoisonedTable);
    };
    let table_nb_row_bytes = table.get_nb_rows().to_be_bytes();
    let table_nb_row_bytes_written = file
        .write(&table_nb_row_bytes)
        .map_err(WriteTableToDiskError::IoError)?;

    if table_nb_row_bytes.len() != table_nb_row_bytes_written {
        return Err(WriteTableToDiskError::NotAllBytesWritten);
    }

    for page_bytes in table.iter().flatten() {
        let table_page_bytes_written = file
            .write(&page_bytes[..])
            .map_err(WriteTableToDiskError::IoError)?;
        if page_bytes.len() != table_page_bytes_written {
            return Err(WriteTableToDiskError::NotAllBytesWritten);
        }
    }

    Ok(())
}
