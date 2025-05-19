#![deny(clippy::unwrap_used, clippy::expect_used)]

mod meta_command;
mod pager;
mod row;
mod statement;
mod table;

use std::env;
use std::io;
use std::io::Write;

use crate::pager::{PAGER, SaveToDiskError, SetOpenSaveFileError};
use crate::table::WriteRowError;
use crate::meta_command::{
    MetaCommandError, MetaCommandSaveError, do_meta_command, is_meta_command,
};
use crate::statement::{
    StatementError, StatementOutput, StatementOutputError, execute_statement, prepare_statement,
};

const PROMPT: &str = "my_db> ";
const EXIT_SUCCESS: i32 = 0;

const POISONED_TABLE_ERROR_STR: &str = "An error occured while loading the save file.";
const POISONED_PAGER_ERROR_STR: &str = "An error occured while loading the pager.";

#[cfg_attr(debug_assertions, derive(Debug))]
pub enum CreateTableError {
    PoisonedFilePath,
    IoError(io::Error),
    NotEnoughData,
    FileIsCorrupted,
    PoisonedTable,
}

fn main() -> ! {
    let args: Vec<String> = env::args().collect();
    if let Some(save_file_path) = args.get(1) {
        if let Ok(mut pager) = PAGER.lock() {
            if let Err(set_open_save_file_error) = pager.set_open_save_file(save_file_path) {
                match set_open_save_file_error {
                    SetOpenSaveFileError::IoError(e) => println!("{e}"),
                    SetOpenSaveFileError::PoisonedTable => println!("{POISONED_TABLE_ERROR_STR}"),
                }
            }
        } else {
            println!("{POISONED_PAGER_ERROR_STR}");
        }
    }

    main_loop()
}

fn main_loop() -> ! {
    let stdin = std::io::stdin();
    let mut buffer = String::new();

    println!("{}", crate::table::Table::ROWS_PER_PAGE);

    loop {
        print!("{PROMPT}");
        let _ = std::io::stdout().flush();
        buffer.clear();
        let Ok(_) = stdin.read_line(&mut buffer) else {
            println!("Invalid input.");
            continue;
        };

        remove_trailing_newline(&mut buffer);

        if buffer.is_empty() {
            continue;
        }

        if is_meta_command(&buffer) {
            let meta_command_result: Result<(), MetaCommandError> = do_meta_command(&buffer);
            handle_do_meta_command_result(meta_command_result, &buffer);
            continue;
        }

        let statement = prepare_statement(&buffer);
        match statement {
            Ok(statement) => match execute_statement(statement) {
                Ok(StatementOutput::Select(rows)) => {
                    for row in rows {
                        println!("{row}");
                    }
                    println!("Executed.");
                }
                Ok(StatementOutput::InsertSuccessfull) => {
                    println!("Executed.");
                }
                Err(StatementOutputError::Insert(WriteRowError::TableFull)) => {
                    println!("Error: Table full.");
                }
                Err(e) => println!("An error occured. {e:?}"),
            },
            Err(StatementError::UnrecognizedStatement) => {
                println!("Unrecognized keyword at start of '{buffer}'.");
            }
            Err(StatementError::InvalidInsert) => {
                println!("Insert statement malformed.");
            }
            Err(StatementError::StringTooLong(name, max)) => {
                println!("'{name}' is too long, max: '{max}'.");
            }
        }
    }
}

fn remove_trailing_newline(buffer: &mut String) {
    let _ = buffer.pop();
}

fn handle_do_meta_command_result(result: Result<(), MetaCommandError>, buffer: &str) {
    match result {
        Ok(()) => {},
        Err(MetaCommandError::MetaCommandSave(MetaCommandSaveError::PoisonedPager)) => println!("{POISONED_PAGER_ERROR_STR}"),
        Err(MetaCommandError::MetaCommandSave(MetaCommandSaveError::SaveToDisk(SaveToDiskError::NoFileToWriteProvided))) => println!("No file to save provided."),
        Err(MetaCommandError::MetaCommandSave(MetaCommandSaveError::SaveToDisk(SaveToDiskError::PoisonedTable))) => println!("{POISONED_TABLE_ERROR_STR}"),
        Err(MetaCommandError::MetaCommandSave(MetaCommandSaveError::SaveToDisk(SaveToDiskError::IoError(e)))) => println!("{e}"),
        Err(MetaCommandError::MetaCommandSave(MetaCommandSaveError::SaveToDisk(SaveToDiskError::NotAllBytesWritten))) => println!("Not all data written to file."),
        Err(MetaCommandError::UnknownMetaCommandError) => println!("Unrecognized command: '{buffer}'."),
    }
}

#[cfg(test)]
mod my_db_test {}
