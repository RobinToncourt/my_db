use std::{cell::RefCell, rc::Rc};

use crate::table::Table;

pub enum CursorError {
    PoisonedTable,
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Cursor {
    table: Rc<RefCell<Table>>,
    row_num: usize,
}
impl Cursor {
    pub fn at_start(table: Rc<RefCell<Table>>) -> Self {
        Self { table, row_num: 0 }
    }

    pub fn at_end(table: Rc<RefCell<Table>>) -> Self {
        let row_num = table.borrow().get_nb_rows();

        Self { table, row_num }
    }

    pub fn is_end_of_table(&self) -> bool {
        self.table.borrow().get_nb_rows() <= self.row_num
    }

    pub fn get(&self) -> &[u8] {
        let slice_pointer = self.table.borrow().get(self.row_num);
        <&[u8]>::from(slice_pointer)
    }

    pub fn get_mut(&mut self) -> &mut [u8] {
        let slice_pointer_mut = self.table.borrow_mut().get_mut(self.row_num);
        <&mut [u8]>::from(slice_pointer_mut)
    }

    pub fn advance(&mut self) {
        self.row_num += 1;
    }
}
