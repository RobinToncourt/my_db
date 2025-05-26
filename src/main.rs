//#![deny(clippy::unwrap_used, clippy::expect_used)]
#![allow(dead_code)]

mod btree;
mod cursor;
mod meta_command;
mod pager;
mod row;
mod slice_pointer;
mod statement;
mod table;

use std::env;
use std::io;
use std::io::Write;
use std::{cell::RefCell, rc::Rc};

use crate::meta_command::{
    MetaCommandError, MetaCommandSaveError, do_meta_command, is_meta_command,
};
use crate::pager::{GetPageError, Pager, SaveToDiskError};
use crate::row::DeserializeError;
use crate::statement::{
    PrepareStatementError, StatementOutput, StatementOutputError, execute_statement,
    prepare_statement,
};
use crate::table::{GetRowError, Table, WriteRowError};

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

    let file: Option<&str> = args.get(1).map(|s| s.as_str());

    let pager = Rc::new(RefCell::new(Pager::new(file)));
    let table = Rc::new(RefCell::new(Table::new(pager.clone())));

    main_loop(table)
}

fn main_loop(table: Rc<RefCell<Table>>) -> ! {
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
            if let Err(meta_command_error) = do_meta_command(table.clone(), &buffer) {
                handle_meta_command_error(meta_command_error, &buffer);
            }
            continue;
        }

        let statement = prepare_statement(&buffer);
        match statement {
            Ok(statement) => match execute_statement(table.clone(), statement) {
                Ok(StatementOutput::Select(rows)) => {
                    for row in rows {
                        println!("{row}");
                    }
                    println!("Executed.");
                }
                Ok(StatementOutput::InsertSuccessfull) => {
                    println!("Executed.");
                }
                Err(StatementOutputError::PoisonedTable) => println!("{POISONED_TABLE_ERROR_STR}"),
                Err(StatementOutputError::Select(rows, get_row_error)) => {
                    for row in rows {
                        println!("{row}");
                    }
                    handle_get_row_error(&get_row_error);
                }
                Err(StatementOutputError::Insert(e)) => handle_write_row_error(&e),
            },
            Err(PrepareStatementError::UnrecognizedStatement) => {
                println!("Unrecognized keyword at start of '{buffer}'.");
            }
            Err(PrepareStatementError::InvalidInsert) => {
                println!("Insert statement malformed.");
            }
            Err(PrepareStatementError::StringTooLong(name, max)) => {
                println!("'{name}' is too long, max: '{max}'.");
            }
        }
    }
}

fn remove_trailing_newline(buffer: &mut String) {
    let _ = buffer.pop();
}

fn handle_meta_command_error(error: MetaCommandError, buffer: &str) {
    match error {
        MetaCommandError::MetaCommandSave(e) => handle_meta_command_save_error(&e),
        MetaCommandError::UnknownMetaCommandError => println!("Unrecognized command: '{buffer}'."),
    }
}

fn handle_meta_command_save_error(error: &MetaCommandSaveError) {
    match error {
        MetaCommandSaveError::PoisonedPager => println!("{POISONED_PAGER_ERROR_STR}"),
        MetaCommandSaveError::SaveToDisk(e) => handle_save_to_disk_error(e),
    }
}

fn handle_save_to_disk_error(error: &SaveToDiskError) {
    match error {
        SaveToDiskError::NoFileToWriteProvided => println!("No file to save provided."),
        SaveToDiskError::PoisonedTable => println!("{POISONED_TABLE_ERROR_STR}"),
        SaveToDiskError::IoError(e) => println!("{e}"),
        SaveToDiskError::NotAllBytesWritten => println!("Not all data written to file."),
    }
}

fn handle_get_row_error(error: &GetRowError) {
    match error {
        GetRowError::PoisonedPager => println!("{POISONED_PAGER_ERROR_STR}"),
        GetRowError::GetPage(e) => handle_get_page_error(e),
        GetRowError::Deserialize(e) => handle_deserialize_error(e),
    }
}

fn handle_write_row_error(error: &WriteRowError) {
    match error {
        WriteRowError::TableFull => println!("Error: Table full."),
        WriteRowError::PoisonedPager => println!("{POISONED_PAGER_ERROR_STR}"),
        WriteRowError::GetPage(e) => handle_get_page_error(e),
    }
}

fn handle_get_page_error(error: &GetPageError) {
    match error {
        GetPageError::MaxPageReached => println!("Max page reached."),
        GetPageError::IoError(e) => println!("{e}"),
    }
}

fn handle_deserialize_error(error: &DeserializeError) {
    match error {
        DeserializeError::InvalidBytesSlice(_slice_len) => {
            println!("Error while deserializing row.");
        }
        DeserializeError::FromUtf8Error(e) => println!("{e}"),
        DeserializeError::TryFromSliceError { .. } => println!("Error while deserializing row."),
    }
}

#[cfg(test)]
mod my_db_test {}
