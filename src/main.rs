#![allow(dead_code, unused_variables)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

use std::env;
use std::fs::File;
use std::io;
use std::io::{Read, Write};
use std::ops::Range;
use std::path::Path;
use std::sync::{LazyLock, Mutex};

use regex::Regex;

const PROMPT: &str = "my_db> ";
const EXIT_SUCCESS: i32 = 0;
const EXIT_ERROR: i32 = -1;

const INSERT_REGEX_STR: &str = r"insert (?<id>\b\d+\b) (?<username>\w+) (?<email>.+)";
static INSERT_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    // Si le regex est invalide le programme ne peut pas fonctionner.
    #[allow(clippy::expect_used)]
    Regex::new(INSERT_REGEX_STR).expect("Unable to parse regex.")
});

static TABLE: LazyLock<Mutex<Table>> = LazyLock::new(|| Mutex::new(Table::default()));

static FILE_PATH: LazyLock<Mutex<Option<String>>> = LazyLock::new(|| Mutex::new(None));

trait MapOkErr<T, E> {
    type Output<U, F>;

    fn map_ok_err<O, P, U, F>(self, ok_op: O, err_op: P) -> Self::Output<U, F>
    where
        O: FnOnce(T) -> U,
        P: FnOnce(E) -> F;
}

impl<T, E> MapOkErr<T, E> for Result<T, E> {
    type Output<U, F> = Result<U, F>;

    fn map_ok_err<O, P, U, F>(self, ok_op: O, err_op: P) -> Self::Output<U, F>
    where
        O: FnOnce(T) -> U,
        P: FnOnce(E) -> F,
    {
        self.map(ok_op).map_err(err_op)
    }
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq)]
struct UnknownMetaCommandError;

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq)]
enum StatementError {
    UnrecognizedStatement,
    InvalidInsert,
    StringTooLong(String, usize),
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq)]
enum DeserializeError {
    InvalidBytesSlice(usize),
    FromUtf8Error(std::string::FromUtf8Error),
    TryFromSliceError {
        name: String,
        expected_size: usize,
        obtained_size: usize,
    },
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq)]
struct TableFullError;

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq)]
enum StatementOutputError {
    TableFullError,
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq)]
enum StatementType {
    Select,
    Insert(Row),
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq)]
enum StatementOutput {
    Select(Vec<Row>),
    InsertSuccessfull,
}

#[cfg_attr(debug_assertions, derive(Debug))]
enum CreateTableError {
    PoisonedFilePath,
    IoError(io::Error),
    NotEnoughData,
    FileIsCorrupted,
    PoisonedTable,
}

#[cfg_attr(debug_assertions, derive(Debug))]
enum ReadDataFromFileError {
    IoError(io::Error),
    NotEnoughData,
    FileIsCorrupted(usize, Vec<Box<[u8; Table::PAGE_SIZE]>>),
}

type ReadDataFromFileResult =
    Result<(usize, Vec<Box<[u8; Table::PAGE_SIZE]>>), ReadDataFromFileError>;

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq, Clone)]
struct Id(usize);
impl Id {
    const MAX_SIZE: usize = 8;
}
impl std::convert::From<Id> for [u8; Id::MAX_SIZE] {
    fn from(id: Id) -> [u8; Id::MAX_SIZE] {
        id.to_be_bytes()
    }
}
impl std::convert::From<[u8; Self::MAX_SIZE]> for Id {
    fn from(arr: [u8; Self::MAX_SIZE]) -> Self {
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
    const MAX_SIZE: usize = 32;
}
impl std::convert::From<Username> for [u8; Username::MAX_SIZE] {
    fn from(username: Username) -> [u8; Username::MAX_SIZE] {
        let mut bytes = username.0.into_bytes();
        bytes.resize_with(Username::MAX_SIZE, || 0);
        // La liste est garantie d'être Username::MAX_SIZE.
        #[allow(clippy::unwrap_used)]
        <[u8; Username::MAX_SIZE]>::try_from(bytes).unwrap()
    }
}
impl std::convert::TryFrom<[u8; Self::MAX_SIZE]> for Username {
    type Error = DeserializeError;

    fn try_from(arr: [u8; Self::MAX_SIZE]) -> Result<Self, Self::Error> {
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
    const MAX_SIZE: usize = 255;
}
impl std::convert::From<Email> for [u8; Email::MAX_SIZE] {
    fn from(email: Email) -> [u8; Email::MAX_SIZE] {
        let mut bytes = email.0.into_bytes();
        bytes.resize_with(Email::MAX_SIZE, || 0);
        // La liste est garantie d'être Email::MAX_SIZE.
        #[allow(clippy::unwrap_used)]
        <[u8; Email::MAX_SIZE]>::try_from(bytes).unwrap()
    }
}
impl std::convert::TryFrom<[u8; Self::MAX_SIZE]> for Email {
    type Error = DeserializeError;

    fn try_from(arr: [u8; Self::MAX_SIZE]) -> Result<Self, Self::Error> {
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

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq, Clone)]
struct Row {
    id: Id,
    username: Username,
    email: Email,
}
impl Row {
    const ID_OFFSET: usize = 0;
    const ID_RANGE: Range<usize> = Row::ID_OFFSET..(Row::ID_OFFSET + Id::MAX_SIZE);

    const USERNAME_OFFSET: usize = Self::ID_OFFSET + Id::MAX_SIZE;
    const USERNAME_RANGE: Range<usize> =
        Row::USERNAME_OFFSET..(Row::USERNAME_OFFSET + Username::MAX_SIZE);

    const EMAIL_OFFSET: usize = Self::USERNAME_OFFSET + Username::MAX_SIZE;
    const EMAIL_RANGE: Range<usize> = Row::EMAIL_OFFSET..(Row::EMAIL_OFFSET + Email::MAX_SIZE);

    const MAX_SIZE: usize = Id::MAX_SIZE + Username::MAX_SIZE + Email::MAX_SIZE;
}
impl std::convert::From<Row> for [u8; Row::MAX_SIZE] {
    fn from(row: Row) -> [u8; Row::MAX_SIZE] {
        let Row {
            id,
            username,
            email,
        } = row;

        let mut bytes = [0; Row::MAX_SIZE];
        bytes[Row::ID_RANGE].copy_from_slice(&<[u8; Id::MAX_SIZE]>::from(id));
        bytes[Row::USERNAME_RANGE].copy_from_slice(&<[u8; Username::MAX_SIZE]>::from(username));
        bytes[Row::EMAIL_RANGE].copy_from_slice(&<[u8; Email::MAX_SIZE]>::from(email));
        bytes
    }
}
impl std::convert::TryFrom<&[u8]> for Row {
    type Error = DeserializeError;

    fn try_from(arr: &[u8]) -> Result<Self, Self::Error> {
        if arr.len() < Self::MAX_SIZE {
            return Err(DeserializeError::InvalidBytesSlice(arr.len()));
        }

        // Les indexation sont valide grâce à la vérification au-dessus.

        let id_bytes: [u8; Id::MAX_SIZE] =
            arr[Self::ID_RANGE]
                .try_into()
                .map_err(|_| DeserializeError::TryFromSliceError {
                    name: "id".to_owned(),
                    expected_size: Username::MAX_SIZE,
                    obtained_size: arr[Self::ID_RANGE].len(),
                })?;
        let id = Id::from(id_bytes);

        let username_bytes: [u8; Username::MAX_SIZE] = arr[Self::USERNAME_RANGE]
            .try_into()
            .map_err(|_| DeserializeError::TryFromSliceError {
                name: "username".to_owned(),
                expected_size: Username::MAX_SIZE,
                obtained_size: arr[Self::USERNAME_RANGE].len(),
            })?;
        let username = Username::try_from(username_bytes)?;

        let email_bytes: [u8; Email::MAX_SIZE] =
            arr[Self::EMAIL_RANGE]
                .try_into()
                .map_err(|_| DeserializeError::TryFromSliceError {
                    name: "email".to_owned(),
                    expected_size: Username::MAX_SIZE,
                    obtained_size: arr[Self::EMAIL_RANGE].len(),
                })?;
        let email = Email::try_from(email_bytes)?;

        Ok(Self {
            id,
            username,
            email,
        })
    }
}
impl std::fmt::Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {}, {})", *self.id, *self.username, *self.email)
    }
}

struct Table {
    nb_rows: usize,
    pages: [Option<Box<[u8; Self::PAGE_SIZE]>>; Self::MAX_PAGES],
}
impl Table {
    const PAGE_SIZE: usize = 4096;
    const MAX_PAGES: usize = 100;
    const ROWS_PER_PAGE: usize = Self::PAGE_SIZE / Row::MAX_SIZE;
    const MAX_ROWS: usize = Self::ROWS_PER_PAGE * Self::MAX_PAGES;

    fn get_nb_rows(&self) -> usize {
        self.nb_rows
    }

    fn get_row(&self, row_number: usize) -> Option<Result<Row, DeserializeError>> {
        if row_number >= self.nb_rows {
            return None;
        }

        let page_num = row_number / Self::ROWS_PER_PAGE;
        let page: &Option<Box<[u8; Self::PAGE_SIZE]>> = &self.pages[page_num];
        let Some(page) = page else {
            return None;
        };

        let row_offset = (row_number % Self::ROWS_PER_PAGE) * Row::MAX_SIZE;
        let row_range = row_offset..(row_offset + Row::MAX_SIZE);
        Some(Row::try_from(&page[row_range]))
    }

    fn write_row(&mut self, row: Row) -> Result<(), TableFullError> {
        if self.nb_rows == Self::MAX_ROWS {
            return Err(TableFullError);
        }

        let page_num = self.nb_rows / Self::ROWS_PER_PAGE;
        let page: &mut Option<Box<[u8; Self::PAGE_SIZE]>> = &mut self.pages[page_num];
        let page: &mut Box<[u8; Self::PAGE_SIZE]> =
            page.get_or_insert(Box::new([0; Self::PAGE_SIZE]));

        let row_offset = (self.nb_rows % Self::ROWS_PER_PAGE) * Row::MAX_SIZE;
        let row_range = row_offset..(row_offset + Row::MAX_SIZE);

        let serialized_row = <[u8; Row::MAX_SIZE]>::from(row);
        page[row_range].copy_from_slice(&serialized_row);
        self.nb_rows += 1;

        Ok(())
    }
}
impl Default for Table {
    fn default() -> Self {
        Self {
            nb_rows: 0,
            pages: [const { None }; Self::MAX_PAGES],
        }
    }
}

fn main() -> ! {
    let args: Vec<String> = env::args().collect();
    if args.len() == 2 {
        if let Ok(mut file_path) = FILE_PATH.lock() {
            *file_path = Some(args[1].clone());
        }
    }

    if let Err(e) = create_table() {
        match e {
            CreateTableError::PoisonedFilePath => println!("Couldn't load save file."),
            CreateTableError::IoError(e) => {
                println!("e");
                std::process::exit(EXIT_ERROR)
            },
            CreateTableError::NotEnoughData => println!("The file did not contains enough data."),
            CreateTableError::FileIsCorrupted => println!("The file was read but was malfomed, proceed with caution."),
            CreateTableError::PoisonedTable => {
                println!("An error occured while loading the save file.");
                std::process::exit(EXIT_ERROR)
            }
        }
    }

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
            match do_meta_command(&buffer) {
                Ok(()) => {}
                Err(UnknownMetaCommandError) => println!("Unrecognized command: '{buffer}'."),
            }
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
    if buffer.starts_with(".save") {
        // TODO: factoriser.
        if let Ok(path) = FILE_PATH.lock() {
            if let Some(path) = path.as_ref() {
                write_table_to_disk(path);
                println!("Save done");
                return Ok(());
            }
            let path: &str = buffer.split(' ').collect::<Vec<&str>>()[1];
            write_table_to_disk(path);
        } else {
            println!("Invalid file path.");
        }
    }

    Err(UnknownMetaCommandError)
}

fn create_table() -> Result<(), CreateTableError> {
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

        table.nb_rows = nb_rows;
        for (i, page) in pages_vec.into_iter().enumerate() {
            table.pages[i] = Some(page);
        }

        if is_save_file_corrupted {
            return Err(CreateTableError::FileIsCorrupted);
        }
    }

    Ok(())
}

fn read_data_from_file(path: &str) -> ReadDataFromFileResult {
    let mut file = File::open(path).map_err(ReadDataFromFileError::IoError)?;

    let mut bytes = Vec::<u8>::new();
    let bytes_read = file
        .read_to_end(&mut bytes)
        .map_err(ReadDataFromFileError::IoError)?;

    assert_eq!(bytes.len(), bytes_read);

    let arr: [u8; 8] = bytes[..8][..]
        .try_into()
        .map_err(|_| ReadDataFromFileError::NotEnoughData)?;
    let nb_rows = usize::from_be_bytes(arr);

    let mut data_are_valid = true;
    let mut result = Vec::<Box<[u8; Table::PAGE_SIZE]>>::new();
    for offset in (8..bytes.len()).step_by(Table::PAGE_SIZE) {
        let page_range = offset..(offset + Table::PAGE_SIZE);

        if let Ok(arr) = <[u8; Table::PAGE_SIZE]>::try_from(&bytes[page_range][..]) {
            let page = Box::new(arr);
            result.push(page);
        } else {
            data_are_valid = false;
        }
    }

    if data_are_valid {
        Ok((nb_rows, result))
    } else {
        Err(ReadDataFromFileError::FileIsCorrupted(nb_rows, result))
    }
}

fn write_table_to_disk(path: &str) {
    // TODO: de-indenter cette fonction.
    if let Ok(mut file) = File::create(Path::new(path)) {
        if let Ok(table) = TABLE.lock() {
            let nb_rows_bytes_written = file.write(&table.nb_rows.to_be_bytes());
            for i in 0..table.nb_rows {
                if let Some(bytes) = &table.pages[i] {
                    if let Ok(bytes_written) = file.write(&bytes[..]) {
                        if bytes_written != bytes.len() {
                            println!("Coudln't write all bytes.");
                        }
                    } else {
                        println!("Error while saving to file.");
                    }
                } else {
                    // Avancer l'écriture de Table::PAGE_SIZE octets.
                }
            }
        } else {
            println!("The table is corrupted.");
        }
    } else {
        // TODO: mettre une meilleur trace.
        println!("Invalid file");
    }
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
        if username.len() > Username::MAX_SIZE {
            return Err(StatementError::StringTooLong(
                "username".to_string(),
                Username::MAX_SIZE,
            ));
        }

        let email = caps["email"].to_owned();
        if email.len() > Email::MAX_SIZE {
            return Err(StatementError::StringTooLong(
                "email".to_string(),
                Email::MAX_SIZE,
            ));
        }

        let row = Row {
            id: Id(id),
            username: Username(username),
            email: Email(email),
        };

        return Ok(StatementType::Insert(row));
    }

    Err(StatementError::UnrecognizedStatement)
}

fn execute_statement(statement: StatementType) -> Result<StatementOutput, StatementOutputError> {
    match statement {
        StatementType::Select => Ok(execute_select()),
        StatementType::Insert(row) => execute_insert(row),
    }
}

fn execute_select() -> StatementOutput {
    #[allow(clippy::expect_used)]
    let table: &Table = &TABLE.lock().expect("The table is corrupted.");

    let mut result = Vec::<Row>::new();
    for row_i in 0..table.nb_rows {
        if let Some(Ok(row)) = table.get_row(row_i) {
            result.push(row);
        }
    }
    StatementOutput::Select(result)
}

fn execute_insert(row: Row) -> Result<StatementOutput, StatementOutputError> {
    #[allow(clippy::expect_used)]
    TABLE
        .lock()
        .expect("The table is corrupted.")
        .write_row(row)
        .map_ok_err(
            |()| StatementOutput::InsertSuccessfull,
            |_| StatementOutputError::TableFullError,
        )
}

#[cfg(test)]
mod my_db_test {
    use super::*;

    #[test]
    fn test_id_from_into_u8_array() {
        let id_arr = <[u8; Id::MAX_SIZE]>::from(Id(42));
        assert_eq!(id_arr, [0, 0, 0, 0, 0, 0, 0, 42]);
        assert_eq!(Id::from(id_arr), Id(42));

        let id_arr = <[u8; Id::MAX_SIZE]>::from(Id(usize::MIN));
        assert_eq!(id_arr, [0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(Id::from(id_arr), Id(usize::MIN));

        let id_arr = <[u8; Id::MAX_SIZE]>::from(Id(usize::MAX));
        assert_eq!(id_arr, [255, 255, 255, 255, 255, 255, 255, 255]);
        assert_eq!(Id::from(id_arr), Id(usize::MAX));
    }

    #[test]
    fn test_username_from_into_u8_array() {
        let username = Username("abigaël".to_owned());
        let username_array = <[u8; Username::MAX_SIZE]>::from(username.clone());
        assert_eq!(
            username_array[..username.len()],
            [97, 98, 105, 103, 97, 195, 171, 108]
        );

        let username_deser =
            Username::try_from(<[u8; Username::MAX_SIZE]>::try_from(username_array).unwrap())
                .unwrap();
        assert_eq!(username_deser, username);
    }

    #[test]
    fn test_email_from_into_u8_array() {
        let email = Email("abigaël@yahoo.com".to_owned());
        let email_bytes = <[u8; Email::MAX_SIZE]>::from(email.clone());
        assert_eq!(
            email_bytes[..email.len()],
            [
                97, 98, 105, 103, 97, 195, 171, 108, 64, 121, 97, 104, 111, 111, 46, 99, 111, 109
            ]
        );

        let email_deser =
            Email::try_from(<[u8; Email::MAX_SIZE]>::try_from(email_bytes).unwrap()).unwrap();
        assert_eq!(email_deser, email);
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

        let arr = <[u8; Row::MAX_SIZE]>::from(row);

        assert_eq!(&arr[Row::ID_RANGE], &id.to_be_bytes());
        assert_eq!(
            &arr[Row::USERNAME_OFFSET..Row::USERNAME_OFFSET + username.len()],
            username.as_bytes()
        );
        assert_eq!(
            &arr[Row::EMAIL_OFFSET..Row::EMAIL_OFFSET + email.len()],
            email.as_bytes()
        );

        let Row {
            id: id_deser,
            username: username_deser,
            email: email_deser,
        } = Row::try_from(&arr[..]).unwrap();

        assert_eq!(id_deser, id);
        assert_eq!(username_deser, username);
        assert_eq!(email_deser, email);
    }

    #[test]
    fn test_table_write_get_row() {
        todo!()
    }

    #[test]
    fn test_insert_table_full() {
        for i in 0..Table::MAX_ROWS {
            let statement = prepare_statement(&format!("insert {i} a_{i} b_{i}")).unwrap();
            assert_eq!(
                execute_statement(statement).unwrap(),
                StatementOutput::InsertSuccessfull
            );
        }
        let statement =
            prepare_statement(&format!("insert {i} a_{i} b_{i}", i = Table::MAX_ROWS)).unwrap();
        assert_eq!(
            execute_statement(statement).unwrap_err(),
            StatementOutputError::TableFullError
        );
    }

    #[test]
    fn test_refuse_username_email_too_long() {
        let username = String::from_utf8(['a' as u8; Username::MAX_SIZE + 1].into()).unwrap();
        assert_eq!(
            prepare_statement(&format!("insert 1 {username} a")).unwrap_err(),
            StatementError::StringTooLong("username".to_owned(), Username::MAX_SIZE)
        );

        let email = String::from_utf8(['b' as u8; Email::MAX_SIZE + 1].into()).unwrap();
        assert_eq!(
            prepare_statement(&format!("insert 2 b {email}")).unwrap_err(),
            StatementError::StringTooLong("email".to_owned(), Email::MAX_SIZE)
        );
    }
}
