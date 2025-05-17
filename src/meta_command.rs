use crate::save::{WriteTableToDiskError, write_table_to_disk};
use crate::{EXIT_SUCCESS, FILE_PATH};

#[cfg_attr(debug_assertions, derive(Debug))]
pub enum MetaCommandError {
    MetaCommandSave(MetaCommadSaveError),
    UnknownMetaCommandError,
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub enum MetaCommadSaveError {
    WriteTableToDisk(WriteTableToDiskError),
    PoisonedFilePath,
    NoFileToWriteProvided,
}

pub fn is_meta_command(buffer: &str) -> bool {
    buffer.starts_with('.')
}

pub fn do_meta_command(buffer: &str) -> Result<(), MetaCommandError> {
    if buffer == ".exit" {
        std::process::exit(EXIT_SUCCESS)
    }
    if buffer.starts_with(".save") {
        return meta_command_save(buffer).map_err(MetaCommandError::MetaCommandSave);
    }

    Err(MetaCommandError::UnknownMetaCommandError)
}

pub fn meta_command_save(buffer: &str) -> Result<(), MetaCommadSaveError> {
    if let Some(provided_file_path) = buffer.split_ascii_whitespace().nth(1) {
        let _ =
            write_table_to_disk(provided_file_path).map_err(MetaCommadSaveError::WriteTableToDisk);
    } else {
        let Ok(guard) = FILE_PATH.lock() else {
            return Err(MetaCommadSaveError::PoisonedFilePath);
        };

        if let Some(file_path) = guard.as_ref() {
            let _ = write_table_to_disk(file_path).map_err(MetaCommadSaveError::WriteTableToDisk);
        } else {
            return Err(MetaCommadSaveError::NoFileToWriteProvided);
        }
    }

    Ok(())
}
