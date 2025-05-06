#![allow(dead_code, unused_variables)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

use std::io::Write;
use std::sync::LazyLock;

use regex::Regex;

const PROMPT: &str = "my_db> ";
const EXIT_SUCCESS: i32 = 0;

const INSERT_REGEX_STR: &str = r"insert (?<id>\b\d+\b) (?<username>\w+) (?<email>.+)";
static INSERT_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    #[allow(clippy::expect_used)]
    Regex::new(INSERT_REGEX_STR).expect("Unable to parse regex.")
});

type RowBytesArray = [u8; Row::SIZE];

struct UnknownMetaCommandError;

enum StatementError {
    UnrecognizedStatement,
    InvalidInsert,
    StringTooLong(String, usize),
}

#[cfg_attr(debug_assertions, derive(Debug))]
enum DeserializeError {
    FromUtf8Error(std::string::FromUtf8Error),
    TryFromSliceError(std::array::TryFromSliceError),
}

enum StatementType {
    Select,
    Insert,
}

struct Row {
    id: usize,
    username: String,
    email: String,
}

impl Row {
    const ID_OFFSET: usize = 0;
    const ID_SIZE: usize = (usize::BITS / 8) as usize;

    const USERNAME_OFFSET: usize = Self::ID_SIZE;
    const USERNAME_SIZE: usize = 32;

    const EMAIL_OFFSET: usize = Self::USERNAME_OFFSET + Self::USERNAME_SIZE;
    const EMAIL_SIZE: usize = 255;

    const SIZE: usize = Self::ID_SIZE + Self::USERNAME_SIZE + Self::EMAIL_SIZE;
}

impl std::convert::From<Row> for RowBytesArray {
    fn from(row: Row) -> Self {
        let Row {
            id,
            username,
            email,
        } = row;

        let mut result = [0_u8; Row::SIZE];

        let id_range = Row::ID_OFFSET..(Row::ID_OFFSET + Row::ID_SIZE);
        let username_range = Row::USERNAME_OFFSET..(Row::USERNAME_OFFSET + username.len());
        let email_range = Row::EMAIL_OFFSET..(Row::EMAIL_OFFSET + email.len());

        result[id_range].clone_from_slice(&id.to_be_bytes());
        result[username_range].clone_from_slice(username.as_bytes());
        result[email_range].clone_from_slice(email.as_bytes());

        result
    }
}

impl std::convert::TryFrom<RowBytesArray> for Row {
    type Error = DeserializeError;

    fn try_from(arr: RowBytesArray) -> Result<Self, Self::Error> {
        let id_range = Row::ID_OFFSET..(Row::ID_OFFSET + Row::ID_SIZE);
        let username_range = Row::USERNAME_OFFSET..(Row::USERNAME_OFFSET + Row::USERNAME_SIZE);
        let email_range = Row::EMAIL_OFFSET..(Row::EMAIL_OFFSET + Row::EMAIL_SIZE);

        let id = usize::from_be_bytes(
            arr[id_range].try_into()
                .map_err(DeserializeError::TryFromSliceError)?
        );

        let mut username_bytes: Vec<u8> = Vec::new();
        arr[username_range].clone_into(&mut username_bytes);
        let username = String::from_utf8(username_bytes)
            .map_err(DeserializeError::FromUtf8Error)?
            .trim_matches(char::from(0))
            .to_string();

        let mut email_bytes: Vec<u8> = Vec::new();
        arr[email_range].clone_into(&mut email_bytes);
        let email = String::from_utf8(email_bytes)
            .map_err(DeserializeError::FromUtf8Error)?
            .trim_matches(char::from(0))
            .to_string();

        Ok(Self {
            id,
            username,
            email,
        })
    }
}

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / Row::SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

struct Table {
    rows_count: usize,
    pages: [Option<Box<[u8; PAGE_SIZE]>>; TABLE_MAX_PAGES],
}

impl Table {
    fn get_mut_row_bytes_array(&mut self, row_number: usize) -> &mut RowBytesArray {
        let page_num = row_number / ROWS_PER_PAGE;
        let page: &mut Option<Box<[u8; PAGE_SIZE]>> = &mut self.pages[page_num];
        let page: &mut [u8; PAGE_SIZE] = page.get_or_insert(Box::new([0; PAGE_SIZE]));
        let row_offset = row_number % ROWS_PER_PAGE;

        let row_range = (Row::SIZE * row_offset)..Row::SIZE;
        page[row_range].try_into().unwrap()
    }
}

fn main() -> ! {
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

        if is_meta_command(&buffer) {
            match do_meta_command(&buffer) {
                Ok(()) => {}
                Err(UnknownMetaCommandError) => println!("Unrecognized command: '{buffer}'."),
            }
            continue;
        }

        let statement = prepare_statement(&buffer);
        match statement {
            Ok(statement) => {
                execute_statement(&statement);
                println!("Executed.");
            }
            Err(StatementError::UnrecognizedStatement) => {
                println!("Unrecognized keyword at start of '{buffer}'.");
            },
            Err(StatementError::InvalidInsert) => {
                println!("Insert statement malformed.");
            },
            Err(StatementError::StringTooLong(name, max)) => {
                println!("'{name}' is too long, max: '{max}'.");
            }
        }
    }
}

fn remove_trailing_newline(buffer: &mut String) {
    let _ = buffer.pop();
}

fn is_meta_command(buffer: &str) -> bool {
    buffer.starts_with('.')
}

fn do_meta_command(buffer: &str) -> Result<(), UnknownMetaCommandError> {
    if buffer == ".exit" {
        std::process::exit(EXIT_SUCCESS)
    }

    Err(UnknownMetaCommandError)
}

fn prepare_statement(buffer: &str) -> Result<StatementType, StatementError> {
    let lowercase: String = buffer.to_lowercase();
    if lowercase.starts_with("select") {
        return Ok(StatementType::Select);
    }
    if lowercase.starts_with("insert") {
        let Some(caps) = INSERT_REGEX.captures(buffer) else {
            return Err(StatementError::InvalidInsert);
        };

        let Ok(id) = caps["id"].parse::<usize>() else {
            return Err(StatementError::InvalidInsert);
        };

        let username = caps["username"].to_owned();
        if username.len() > Row::USERNAME_SIZE {
            return Err(StatementError::StringTooLong(
                "username".to_string(), Row::USERNAME_SIZE
            ));
        }

        let email = caps["email"].to_owned();
        if email.len() > Row::EMAIL_SIZE {
            return Err(StatementError::StringTooLong(
                "email".to_string(), Row::EMAIL_SIZE
            ));
        }

        let row = Row {
            id,
            username,
            email,
        };

        return Ok(StatementType::Insert);
    }

    Err(StatementError::UnrecognizedStatement)
}

fn execute_statement(statement: &StatementType) {
    match statement {
        StatementType::Select => todo!("Select statement"),
        StatementType::Insert => todo!("Insert statement"),
    }
}

#[cfg(test)]
mod my_db_test {
    use super::*;

    #[test]
    fn test_row_from_into_u8_array() {
        let id = 42;
        let username = "abigaël".to_string();
        let email = "abigaël@yahoo.com".to_string();

        let row = Row {
            id: id.clone(),
            username: username.clone(),
            email: email.clone(),
        };

        let arr: RowBytesArray = row.into();

        let id_range = Row::ID_OFFSET..(Row::ID_OFFSET + Row::ID_SIZE);
        let username_range = Row::USERNAME_OFFSET..(Row::USERNAME_OFFSET + username.len());
        let email_range = Row::EMAIL_OFFSET..(Row::EMAIL_OFFSET + email.len());

        assert_eq!(arr[id_range], id.to_be_bytes());
        assert_eq!(&arr[username_range], username.as_bytes());
        assert_eq!(&arr[email_range], email.as_bytes());

        let Row {
            id,
            username,
            email,
        } = arr.try_into().unwrap();
        assert_eq!(id, 42);
        assert_eq!(username, "abigaël");
        assert_eq!(email, "abigaël@yahoo.com");
    }
}
