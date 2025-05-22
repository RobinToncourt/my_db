use std::sync::{Arc, Mutex};

use crate::table::{TABLE, Table};

pub enum CursorError {
    PoisonedTable,
}

pub struct Cursor {
    table: Arc<Mutex<Table>>,
    row_num: usize,
}
impl Cursor {
    pub fn at_start(table: Arc<Mutex<Table>>) -> Self {
        Self {
            table,
            row_num: 0,
        }
    }

    pub fn at_end(table: Arc<Mutex<Table>>) -> Result<Self, CursorError> {
        let Ok(table_unlock) = TABLE.lock() else {
            return Err(CursorError::PoisonedTable);
        };

        Ok(Self {
            table,
            row_num: table_unlock.get_nb_rows(),
        })
    }

    pub fn is_end_of_table(&self) -> bool {
        let table = TABLE.lock().expect("Table is poisoned.");

        table.get_nb_rows() <= self.row_num
    }

    pub fn get(&mut self) -> &[u8] {
        let table: &mut Table = Arc::get_mut(&mut self.table).unwrap().get_mut().unwrap();
        table.get(self.row_num)
    }

    pub fn advance(&mut self) {
        self.row_num += 1;
    }
}
