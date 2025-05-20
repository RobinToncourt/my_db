use crate::EXIT_SUCCESS;
use crate::pager::{PAGER, SaveToDiskError};

#[cfg_attr(debug_assertions, derive(Debug))]
pub enum MetaCommandError {
    MetaCommandSave(MetaCommandSaveError),
    UnknownMetaCommandError,
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub enum MetaCommandSaveError {
    PoisonedPager,
    SaveToDisk(SaveToDiskError),
}

pub fn is_meta_command(buffer: &str) -> bool {
    buffer.starts_with('.')
}

pub fn do_meta_command(buffer: &str) -> Result<(), MetaCommandError> {
    if buffer.to_lowercase() == ".exit" {
        std::process::exit(EXIT_SUCCESS)
    }
    if buffer.to_lowercase().starts_with(".save") {
        return meta_command_save(buffer).map_err(MetaCommandError::MetaCommandSave);
    }

    Err(MetaCommandError::UnknownMetaCommandError)
}

pub fn meta_command_save(buffer: &str) -> Result<(), MetaCommandSaveError> {
    let Ok(mut pager) = PAGER.lock() else {
        return Err(MetaCommandSaveError::PoisonedPager);
    };

    let provided_file_path: Option<&str> = buffer.split_ascii_whitespace().nth(1);
    pager
        .save_to_disk(provided_file_path)
        .map_err(MetaCommandSaveError::SaveToDisk)
}
