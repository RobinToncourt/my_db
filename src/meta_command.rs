use crate::EXIT_SUCCESS;
use crate::pager::{PAGER, SaveToDiskError};

#[cfg_attr(debug_assertions, derive(Debug))]
pub enum MetaCommandError {
    MetaCommandSave(MetaCommadSaveError),
    UnknownMetaCommandError,
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub enum MetaCommadSaveError {
    PoisonedPager,
    SaveToDisk(SaveToDiskError),
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
    let Ok(mut pager) = PAGER.lock() else {
        return Err(MetaCommadSaveError::PoisonedPager);
    };

    if let Some(provided_file_path) = buffer.split_ascii_whitespace().nth(1) {
        let _ =
            pager.save_to_disk(Some(provided_file_path)).map_err(MetaCommadSaveError::SaveToDisk);
    } else {
        pager.save_to_disk(None).map_err(MetaCommadSaveError::SaveToDisk);
    }

    Ok(())
}
