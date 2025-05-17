#![allow(dead_code, unused_variables)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

mod meta_command;
mod open;
mod row;
mod save;
mod statement;
mod table;

use std::env;
use std::io::Write;
use std::sync::{LazyLock, Mutex};

use crate::meta_command::{
    MetaCommadSaveError, MetaCommandError, do_meta_command, is_meta_command,
};
use crate::open::{ReadDataFromFileError, read_data_from_file};
use crate::save::WriteTableToDiskError;
use crate::statement::{
    StatementError, StatementOutput, StatementOutputError, execute_statement, prepare_statement,
};
use crate::table::{CreateTableError, TABLE};

const PROMPT: &str = "my_db> ";
const EXIT_SUCCESS: i32 = 0;
const EXIT_ERROR: i32 = -1;

const POISONED_TABLE_ERROR_STR: &str = "An error occured while loading the save file.";
const POISONED_FILE_PATH_ERROR_STR: &str = "Couldn't load save file.";

static FILE_PATH: LazyLock<Mutex<Option<String>>> = LazyLock::new(|| Mutex::new(None));

fn main() -> ! {
    let args: Vec<String> = env::args().collect();
    if args.len() == 2 {
        if let Ok(mut file_path) = FILE_PATH.lock() {
            *file_path = Some(args[1].clone());
        }
    }

    if let Err(e) = create_table() {
        handle_create_table_error(e);
    }

    main_loop()
}

fn handle_create_table_error(e: CreateTableError) {
    match e {
        CreateTableError::PoisonedFilePath => println!("{POISONED_FILE_PATH_ERROR_STR}"),
        CreateTableError::IoError(io_error) => {
            println!("{io_error}");
            std::process::exit(EXIT_ERROR)
        }
        CreateTableError::NotEnoughData => println!("The file did not contains enough data."),
        CreateTableError::FileIsCorrupted => {
            println!("The file was read but was malfomed, proceed with caution.");
        }
        CreateTableError::PoisonedTable => {
            println!("{POISONED_TABLE_ERROR_STR}");
            std::process::exit(EXIT_ERROR)
        }
    }
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
}

fn remove_trailing_newline(buffer: &mut String) {
    let _ = buffer.pop();
}

/// # Errors
///
/// Will return `Err` if:
/// - Can't access the `FILE_PATH` mutex
/// - `IoError` happen
/// - The file does not contains enough data.
/// - The file ends too soon, the Table will be filled with anything reteived.
/// - Can't access the `TABLE` mutex.
pub fn create_table() -> Result<(), CreateTableError> {
    let Ok(guard) = FILE_PATH.lock() else {
        return Err(CreateTableError::PoisonedFilePath);
    };

    if let Some(path) = guard.as_ref() {
        let mut is_save_file_corrupted = false;
        let (nb_rows, pages_vec) = match read_data_from_file(path) {
            Ok(result) => result,
            Err(ReadDataFromFileError::IoError(e)) => return Err(CreateTableError::IoError(e)),
            Err(ReadDataFromFileError::NotEnoughData) => {
                return Err(CreateTableError::NotEnoughData);
            }
            Err(ReadDataFromFileError::FileIsCorrupted(nb_rows, pages_vec)) => {
                is_save_file_corrupted = true;
                (nb_rows, pages_vec)
            }
        };

        let Ok(mut table) = TABLE.lock() else {
            return Err(CreateTableError::PoisonedTable);
        };

        table.set_nb_rows(nb_rows);
        for (i, page) in pages_vec.into_iter().enumerate() {
            table.set_page(i, page);
        }

        if is_save_file_corrupted {
            return Err(CreateTableError::FileIsCorrupted);
        }
    }

    Ok(())
}

#[cfg(test)]
mod my_db_test {}
