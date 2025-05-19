use std::sync::LazyLock;

use regex::Regex;

use crate::row::{Email, Id, Row, Username};
use crate::table::{TABLE, Table, WriteRowError};

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
pub enum StatementError {
    UnrecognizedStatement,
    InvalidInsert,
    StringTooLong(String, usize),
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub enum StatementOutputError {
    Insert(WriteRowError),
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq)]
pub enum StatementType {
    Select,
    Insert(Row),
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq)]
pub enum StatementOutput {
    Select(Vec<Row>),
    InsertSuccessfull,
}

pub fn prepare_statement(buffer: &str) -> Result<StatementType, StatementError> {
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

        let row = Row::new(Id::new(id), Username::new(username), Email::new(email));

        return Ok(StatementType::Insert(row));
    }

    Err(StatementError::UnrecognizedStatement)
}

pub fn execute_statement(
    statement: StatementType,
) -> Result<StatementOutput, StatementOutputError> {
    match statement {
        StatementType::Select => Ok(execute_select()),
        StatementType::Insert(row) => execute_insert(row),
    }
}

pub fn execute_select() -> StatementOutput {
    #[allow(clippy::expect_used)]
    let table: &Table = &TABLE.lock().expect("The table is corrupted.");

    let mut result = Vec::<Row>::new();
    for row_i in 0..table.get_nb_rows() {
        if let Some(Ok(row)) = table.get_row(row_i) {
            result.push(row);
        }
    }
    StatementOutput::Select(result)
}

pub fn execute_insert(row: Row) -> Result<StatementOutput, StatementOutputError> {
    TABLE
        .lock()
        .expect("The table is corrupted.")
        .write_row(row)
        .map_ok_err(
            |()| StatementOutput::InsertSuccessfull,
            StatementOutputError::Insert,
        )
}

#[cfg(test)]
mod statement_test {
    use super::*;

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
