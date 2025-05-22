use std::sync::{LazyLock, Arc, Mutex};

use crate::pager::{GetPageError, PAGER, Page, Pager};
use crate::row::{DeserializeError, Row};

pub static TABLE: LazyLock<Arc<Mutex<Table>>> = LazyLock::new(|| Arc::new(Mutex::new(Table::default())));

#[cfg_attr(debug_assertions, derive(Debug))]
pub enum GetRowError {
    PoisonedPager,
    GetPage(GetPageError),
    Deserialize(DeserializeError),
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub enum WriteRowError {
    TableFull,
    PoisonedPager,
    GetPage(GetPageError),
}

pub struct Table {
    pager: Arc<Mutex<Pager>>,
    nb_rows: usize,
}
impl Table {
    pub const ROWS_PER_PAGE: usize = Page::SIZE / Row::MAX_SIZE;
    pub const MAX_ROWS: usize = Self::ROWS_PER_PAGE * Pager::MAX_PAGES;

    pub fn get_nb_rows(&self) -> usize {
        self.nb_rows
    }

    pub fn set_nb_rows(&mut self, nb_rows: usize) {
        self.nb_rows = nb_rows;
    }

    pub fn get(&mut self, row_number: usize) -> &[u8] {
        assert!(row_number < self.nb_rows, "Max row reached.");

        let pager: &mut Pager = Arc::get_mut(&mut self.pager).unwrap().get_mut().unwrap();

        let page_num = row_number / Self::ROWS_PER_PAGE;
        let page: &[u8] = pager.get(page_num);

        let row_offset = (row_number % Self::ROWS_PER_PAGE) * Row::MAX_SIZE;
        let row_range = row_offset..(row_offset + Row::MAX_SIZE);
        &page[row_range]
    }

    pub fn get_row(&self, row_number: usize) -> Option<Result<Row, GetRowError>> {
        if row_number >= self.nb_rows {
            return None;
        }

        let Ok(mut pager) = PAGER.lock() else {
            return Some(Err(GetRowError::PoisonedPager));
        };

        let page_num = row_number / Self::ROWS_PER_PAGE;
        let get_page_result = pager.get_page(page_num);
        let page: &mut Page = match get_page_result {
            Ok(page) => page,
            Err(e) => return Some(Err(GetRowError::GetPage(e))),
        };

        let row_offset = (row_number % Self::ROWS_PER_PAGE) * Row::MAX_SIZE;
        let row_range = row_offset..(row_offset + Row::MAX_SIZE);
        Some(Row::try_from(&page[row_range]).map_err(GetRowError::Deserialize))
    }

    pub fn write_row(&mut self, row: Row) -> Result<(), WriteRowError> {
        if self.nb_rows == Self::MAX_ROWS {
            return Err(WriteRowError::TableFull);
        }

        let Ok(mut pager) = PAGER.lock() else {
            return Err(WriteRowError::PoisonedPager);
        };

        let page_num = self.nb_rows / Self::ROWS_PER_PAGE;
        let page: &mut Page = pager.get_page(page_num).map_err(WriteRowError::GetPage)?;

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
            pager: PAGER.clone(),
            nb_rows: PAGER.lock().unwrap().get_file_nb_rows(),
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
