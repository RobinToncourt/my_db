use std::sync::LazyLock;
use std::{cell::RefCell, rc::Rc};

use regex::Regex;

use crate::cursor::Cursor;
use crate::row::{Email, Id, Row, Username};
use crate::table::{GetRowError, Table, WriteRowError};

const INSERT_REGEX_STR: &str = r"insert (?<id>\b\d+\b) (?<username>\w+) (?<email>.+)";
static INSERT_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    // Si le regex est invalide le programme ne peut pas fonctionner.
    #[allow(clippy::expect_used)]
    Regex::new(INSERT_REGEX_STR).expect("Unable to parse regex.")
});

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
pub enum StatementType {
    Select,
    Insert(Row),
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq)]
pub enum PrepareStatementError {
    UnrecognizedStatement,
    InvalidInsert,
    StringTooLong(String, usize),
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq)]
pub enum StatementOutput {
    Select(Vec<Row>),
    InsertSuccessfull,
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub enum StatementOutputError {
    PoisonedTable,
    Select(Vec<Row>, GetRowError),
    Insert(WriteRowError),
}

pub fn prepare_statement(buffer: &str) -> Result<StatementType, PrepareStatementError> {
    let lowercase: String = buffer.to_lowercase();
    if lowercase.starts_with("select") {
        return Ok(StatementType::Select);
    }
    if lowercase.starts_with("insert") {
        let Some(caps) = INSERT_REGEX.captures(buffer) else {
            return Err(PrepareStatementError::InvalidInsert);
        };

        let Ok(id) = caps["id"].parse::<usize>() else {
            return Err(PrepareStatementError::InvalidInsert);
        };

        let username = caps["username"].to_owned();
        if username.len() > Username::MAX_SIZE {
            return Err(PrepareStatementError::StringTooLong(
                "username".to_string(),
                Username::MAX_SIZE,
            ));
        }

        let email = caps["email"].to_owned();
        if email.len() > Email::MAX_SIZE {
            return Err(PrepareStatementError::StringTooLong(
                "email".to_string(),
                Email::MAX_SIZE,
            ));
        }

        let row = Row::new(Id::new(id), Username::new(username), Email::new(email));

        return Ok(StatementType::Insert(row));
    }

    Err(PrepareStatementError::UnrecognizedStatement)
}

pub fn execute_statement(
    table: Rc<RefCell<Table>>,
    statement: StatementType,
) -> Result<StatementOutput, StatementOutputError> {
    match statement {
        StatementType::Select => Ok(execute_select(table)),
        StatementType::Insert(row) => execute_insert(table, row),
    }
}

pub fn execute_select(table: Rc<RefCell<Table>>) -> StatementOutput {
    let mut cursor = Cursor::at_start(table.clone());

    let mut result = Vec::<Row>::new();
    while !cursor.is_end_of_table() {
        let bytes = cursor.get();
        let row = Row::try_from(bytes).unwrap();
        result.push(row);
        cursor.advance();
    }

    StatementOutput::Select(result)
}

pub fn execute_insert(
    table: Rc<RefCell<Table>>,
    row: Row,
) -> Result<StatementOutput, StatementOutputError> {
    let mut cursor = Cursor::at_end(table.clone());
    let row_bytes = <[u8; Row::MAX_SIZE]>::from(row);
    cursor.get_mut().copy_from_slice(&row_bytes[..]);
    {
        let mut table_mut = table.borrow_mut();
        let nb_rows = table_mut.get_nb_rows();
        table_mut.set_nb_rows(nb_rows + 1);
    }
    Ok(StatementOutput::InsertSuccessfull)
}

#[cfg(test)]
mod statement_test {}
