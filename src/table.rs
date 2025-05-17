use std::io;
use std::slice::Iter;
use std::sync::{LazyLock, Mutex};

use crate::row::{DeserializeError, Row};

pub static TABLE: LazyLock<Mutex<Table>> = LazyLock::new(|| Mutex::new(Table::default()));

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq)]
pub struct TableFullError;

#[cfg_attr(debug_assertions, derive(Debug))]
pub enum CreateTableError {
    PoisonedFilePath,
    IoError(io::Error),
    NotEnoughData,
    FileIsCorrupted,
    PoisonedTable,
}

pub struct Table {
    nb_rows: usize,
    pages: [Option<Box<[u8; Self::PAGE_SIZE]>>; Self::MAX_PAGES],
}
impl Table {
    pub const PAGE_SIZE: usize = 4096;
    pub const MAX_PAGES: usize = 100;
    pub const ROWS_PER_PAGE: usize = Self::PAGE_SIZE / Row::MAX_SIZE;
    pub const MAX_ROWS: usize = Self::ROWS_PER_PAGE * Self::MAX_PAGES;

    pub fn get_nb_rows(&self) -> usize {
        self.nb_rows
    }

    pub fn set_nb_rows(&mut self, nb_rows: usize) {
        self.nb_rows = nb_rows;
    }

    pub fn iter(&self) -> Iter<'_, Option<Box<[u8; Self::PAGE_SIZE]>>> {
        self.pages.iter()
    }

    pub fn set_page(&mut self, index: usize, page: Box<[u8; Table::PAGE_SIZE]>) {
        self.pages[index] = Some(page);
    }

    pub fn get_row(&self, row_number: usize) -> Option<Result<Row, DeserializeError>> {
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

    pub fn write_row(&mut self, row: Row) -> Result<(), TableFullError> {
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

#[cfg(test)]
mod table_test {
    use super::*;
    use crate::row::{Email, Id, Username};
    use crate::statement::{
        StatementOutput, StatementOutputError, execute_statement, prepare_statement,
    };

    #[test]
    fn test_table_write_get_row() {
        let mut table = TABLE.lock().unwrap();

        let row = Row::new(
            Id::new(42),
            Username::new("abigaël".to_string()),
            Email::new("abigaël@yahoo.com".to_string()),
        );

        assert_eq!(table.write_row(row.clone()), Ok(()));

        let r = table.get_row(0).unwrap();

        assert_eq!(r, Ok(row));

        assert!(table.get_row(1).is_none());
    }

    #[test]
    fn test_insert_table_full() {
        println!("{},", Table::MAX_ROWS);
        for i in 1..Table::MAX_ROWS {
            let statement = prepare_statement(&format!("insert {i} a_{i} b_{i}")).unwrap();
            assert_eq!(
                execute_statement(statement),
                Ok(StatementOutput::InsertSuccessfull),
                "insert {i} a_{i} b_{i}"
            );
        }
        let statement =
            prepare_statement(&format!("insert {i} a_{i} b_{i}", i = Table::MAX_ROWS)).unwrap();
        assert_eq!(
            execute_statement(statement).unwrap_err(),
            StatementOutputError::TableFullError
        );
    }
}
