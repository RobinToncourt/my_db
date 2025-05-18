#![deny(clippy::unwrap_used, clippy::expect_used)]

mod meta_command;
mod row;
mod statement;
mod table;
mod pager;

use std::env;
use std::io;
use std::io::Write;

use crate::pager::PAGER;

use crate::meta_command::{
    MetaCommadSaveError, MetaCommandError, do_meta_command, is_meta_command,
};
use crate::statement::{
    StatementError, StatementOutput, StatementOutputError, execute_statement, prepare_statement,
};

const PROMPT: &str = "my_db> ";
const EXIT_SUCCESS: i32 = 0;
const EXIT_ERROR: i32 = -1;

const POISONED_TABLE_ERROR_STR: &str = "An error occured while loading the save file.";
const POISONED_FILE_PATH_ERROR_STR: &str = "Couldn't load save file.";

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
            pager.set_open_save_file(save_file_path);
        } else {
            println!("The pager is poisoned.");
        }
    }

    main_loop()
}

fn main_loop() -> ! {
    let stdin = std::io::stdin();
    let mut buffer = String::new();

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
            handle_do_meta_command_result(do_meta_command(&buffer), &buffer);
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
                Err(StatementOutputError::TableFullError) => {
                    println!("Error: Table full.");
                }
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

fn handle_do_meta_command_result(result: Result<(), MetaCommandError>, buffer: &str) {
    todo!()
    /*
    match result {
        Ok(()) => {}
        Err(MetaCommandError::MetaCommandSave(MetaCommadSaveError::WriteTableToDisk(
            WriteTableToDiskError::IoError(io_error),
        ))) => println!("{io_error}"),
        Err(MetaCommandError::MetaCommandSave(MetaCommadSaveError::WriteTableToDisk(
            WriteTableToDiskError::PoisonedTable,
        ))) => println!("{POISONED_TABLE_ERROR_STR}"),
        Err(MetaCommandError::MetaCommandSave(MetaCommadSaveError::WriteTableToDisk(
            WriteTableToDiskError::NotAllBytesWritten,
        ))) => println!("Not all bytes where written."),
        Err(MetaCommandError::MetaCommandSave(MetaCommadSaveError::PoisonedFilePath)) => {
            println!("{POISONED_FILE_PATH_ERROR_STR}");
        }
        Err(MetaCommandError::MetaCommandSave(MetaCommadSaveError::NoFileToWriteProvided)) => {
            println!("You need to provide a file to save to.");
        }
        Err(MetaCommandError::UnknownMetaCommandError) => {
            println!("Unrecognized command: '{buffer}'.");
        }
    }
    */
}

fn remove_trailing_newline(buffer: &mut String) {
    let _ = buffer.pop();
}

#[cfg(test)]
mod my_db_test {}
