#![allow(dead_code, unused_variables)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

use std::io::Write;
use std::sync::LazyLock;
use std::ops::Range;

use regex::Regex;

const PROMPT: &str = "my_db> ";
const EXIT_SUCCESS: i32 = 0;

const INSERT_REGEX_STR: &str = r"insert (?<id>\b\d+\b) (?<username>\w+) (?<email>.+)";
static INSERT_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    // Si le regex est invalide le programme ne peut pas fonctionner.
    #[allow(clippy::expect_used)]
    Regex::new(INSERT_REGEX_STR).expect("Unable to parse regex.")
});

struct UnknownMetaCommandError;

enum StatementError {
    UnrecognizedStatement,
    InvalidInsert,
    StringTooLong(String, usize),
}

#[cfg_attr(debug_assertions, derive(Debug))]
enum DeserializeError {
    InvalidBytesSlice(usize),
    FromUtf8Error(std::string::FromUtf8Error),
    TryFromSliceError(std::array::TryFromSliceError),
}

enum StatementType {
    Select,
    Insert,
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq, Clone)]
struct Id(usize);
impl Id {
    const SIZE: usize = 8;
}
impl std::convert::From<Id> for Vec<u8> {
    fn from(id: Id) -> Vec<u8> {
        id.to_be_bytes().into_iter().collect()
    }
}
// TODO: ne fonctionne pas sur un système 32 bits.
impl std::convert::From<[u8; Self::SIZE]> for Id {
    fn from(arr: [u8; Self::SIZE]) -> Self {
        Self(usize::from_be_bytes(arr))
    }
}
impl std::ops::Deref for Id {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq, Clone)]
struct Username(String);
impl Username {
    const SIZE: usize = 32;
}
impl std::convert::From<Username> for Vec<u8> {
    fn from(username: Username) -> Vec<u8> {
        (*username).clone().into_bytes()
    }
}
impl std::convert::TryFrom<[u8; Self::SIZE]> for Username {
    type Error = DeserializeError;

    fn try_from(arr: [u8; Self::SIZE]) -> Result<Self, Self::Error> {
        let username = String::from_utf8(Vec::<u8>::from(arr))
        .map_err(DeserializeError::FromUtf8Error)?
        .trim_matches(char::from(0))
        .to_string();

        Ok(Username(username))
    }
}
impl std::ops::Deref for Username {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq, Clone)]
struct Email(String);
impl Email {
    const SIZE: usize = 255;
}
impl std::convert::From<Email> for Vec<u8> {
    fn from(email: Email) -> Vec<u8> {
        (*email).clone().into_bytes()
    }
}
impl std::convert::TryFrom<[u8; Self::SIZE]> for Email {
    type Error = DeserializeError;

    fn try_from(arr: [u8; Self::SIZE]) -> Result<Self, Self::Error> {
        let email = String::from_utf8(Vec::<u8>::from(arr))
        .map_err(DeserializeError::FromUtf8Error)?
        .trim_matches(char::from(0))
        .to_string();

        Ok(Email(email))
    }
}
impl std::ops::Deref for Email {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

struct Row {
    id: Id,
    username: Username,
    email: Email,
}
impl Row {
    const ID_OFFSET: usize = 0;
    const ID_RANGE: Range<usize> = Row::ID_OFFSET..(Row::ID_OFFSET + Id::SIZE);

    const USERNAME_OFFSET: usize = Self::ID_OFFSET + Id::SIZE;
    const USERNAME_RANGE: Range<usize> = Row::USERNAME_OFFSET..(Row::USERNAME_OFFSET + Username::SIZE);

    const EMAIL_OFFSET: usize = Self::USERNAME_OFFSET + Username::SIZE;
    const EMAIL_RANGE: Range<usize> = Row::EMAIL_OFFSET..(Row::EMAIL_OFFSET + Email::SIZE);

    const SIZE: usize = Id::SIZE + Username::SIZE + Email::SIZE;
}
impl std::convert::From<Row> for Vec<u8> {
    fn from(row: Row) -> Vec<u8> {
        let Row {
            id,
            username,
            email,
        } = row;

        let mut bytes = Vec::<u8>::from(id);
        bytes.append(&mut Vec::<u8>::from(username));
        bytes.append(&mut Vec::<u8>::from(email));
        bytes
    }
}
impl std::convert::TryFrom<&[u8]> for Row {
    type Error = DeserializeError;

    fn try_from(arr: &[u8]) -> Result<Self, Self::Error> {
        if arr.len() < Self::SIZE {
            return Err(DeserializeError::InvalidBytesSlice(arr.len()));
        }

        // Les indexation sont valide grâce à la vérification au-dessus.

        let id_bytes: [u8; Id::SIZE] = arr[Self::ID_RANGE]
        .try_into()
        .map_err(DeserializeError::TryFromSliceError)?;
        let id = Id::from(id_bytes);

        let username_bytes: [u8; Username::SIZE] = arr[Self::USERNAME_RANGE]
        .try_into()
        .map_err(DeserializeError::TryFromSliceError)?;
        let username = Username::try_from(username_bytes)?;

        let email_bytes: [u8; Email::SIZE] = arr[Self::USERNAME_RANGE]
        .try_into()
        .map_err(DeserializeError::TryFromSliceError)?;
        let email = Email::try_from(email_bytes)?;

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
    fn get_row(&mut self, row_number: usize) -> Result<Row, DeserializeError> {
        let page_num = row_number / ROWS_PER_PAGE;
        let page: &mut Option<Box<[u8; PAGE_SIZE]>> = &mut self.pages[page_num];
        let page: &mut [u8; PAGE_SIZE] = page.get_or_insert(Box::new([0; PAGE_SIZE]));
        let row_offset = row_number % ROWS_PER_PAGE;

        let row_range = (Row::SIZE * row_offset)..Row::SIZE;
        Row::try_from(&page[row_range])
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
        if username.len() > Username::SIZE {
            return Err(StatementError::StringTooLong(
                "username".to_string(), Username::SIZE
            ));
        }

        let email = caps["email"].to_owned();
        if email.len() > Email::SIZE {
            return Err(StatementError::StringTooLong(
                "email".to_string(), Email::SIZE
            ));
        }

        let row = Row {
            id: Id(id),
            username: Username(username),
            email: Email(email),
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
    fn test_id_from_into_u8_slice() {

    }

    #[test]
    fn test_row_from_into_u8_slice() {
        let id = Id(42);
        let username = Username("abigaël".to_string());
        let email = Email("abigaël@yahoo.com".to_string());

        let row = Row {
            id: id.clone(),
            username: username.clone(),
            email: email.clone(),
        };

        let arr = Vec::<u8>::from(row);

        assert_eq!(&arr[Row::ID_RANGE], &id.to_be_bytes());
        assert_eq!(&arr[Row::USERNAME_RANGE], username.as_bytes());
        assert_eq!(&arr[Row::EMAIL_RANGE], email.as_bytes());

        let Row {
            id: id_deser,
            username: username_deser,
            email: email_deser,
        } = Row::try_from(&arr[..]).unwrap();

        assert_eq!(id_deser, id);
        assert_eq!(username_deser, username);
        assert_eq!(email_deser, email);
    }
}
