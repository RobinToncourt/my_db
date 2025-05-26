use std::{cell::RefCell, rc::Rc};

use crate::pager::{GetPageError, Page, Pager};
use crate::row::{DeserializeError, Row};
use crate::slice_pointer::{SlicePointer, SlicePointerMut};

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

#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Table {
    pager: Rc<RefCell<Pager>>,
    nb_rows: usize,
}
impl Table {
    pub const ROWS_PER_PAGE: usize = Page::SIZE / Row::MAX_SIZE;
    pub const MAX_ROWS: usize = Self::ROWS_PER_PAGE * Pager::MAX_PAGES;

    pub fn new(pager: Rc<RefCell<Pager>>) -> Self {
        let nb_rows = 0;
        Self { pager, nb_rows }
    }

    pub fn get_nb_rows(&self) -> usize {
        self.nb_rows
    }

    pub fn get_pager(&self) -> Rc<RefCell<Pager>> {
        self.pager.clone()
    }

    pub fn set_nb_rows(&mut self, nb_rows: usize) {
        self.nb_rows = nb_rows;
    }

    pub fn get(&self, row_number: usize) -> SlicePointer {
        assert!(row_number < self.nb_rows, "Max row reached.");

        let page_num = row_number / Self::ROWS_PER_PAGE;
        let mut page: SlicePointer = self.pager.borrow_mut().get(page_num);

        let row_offset: usize = (row_number % Self::ROWS_PER_PAGE) * Row::MAX_SIZE;
        page += row_offset;
        page.set_len(Row::MAX_SIZE);
        page
    }

    pub fn get_mut(&mut self, row_number: usize) -> SlicePointerMut {
        assert!(row_number >= self.nb_rows, "Max row reached.");

        let page_num = row_number / Self::ROWS_PER_PAGE;
        let mut page: SlicePointerMut = self.pager.borrow_mut().get_mut(page_num);

        let row_offset: usize = (row_number % Self::ROWS_PER_PAGE) * Row::MAX_SIZE;
        page += row_offset;
        page.set_len(Row::MAX_SIZE);
        page
    }

    pub fn get_row(&self, _row_number: usize) -> Option<Result<Row, GetRowError>> {
        unimplemented!()
        // if row_number >= self.nb_rows {
        //     return None;
        // }
        //
        // let Ok(mut pager) = PAGER.lock() else {
        //     return Some(Err(GetRowError::PoisonedPager));
        // };
        //
        // let page_num = row_number / Self::ROWS_PER_PAGE;
        // let get_page_result = pager.get_page(page_num);
        // let page: &mut Page = match get_page_result {
        //     Ok(page) => page,
        //     Err(e) => return Some(Err(GetRowError::GetPage(e))),
        // };
        //
        // let row_offset = (row_number % Self::ROWS_PER_PAGE) * Row::MAX_SIZE;
        // let row_range = row_offset..(row_offset + Row::MAX_SIZE);
        // Some(Row::try_from(&page[row_range]).map_err(GetRowError::Deserialize))
    }

    pub fn write_row(&mut self, row: Row) -> Result<(), WriteRowError> {
        if self.nb_rows == Self::MAX_ROWS {
            return Err(WriteRowError::TableFull);
        }

        let page_num = self.nb_rows / Self::ROWS_PER_PAGE;
        let mut binding = self.pager.borrow_mut();
        let page: &mut Page = binding.get_page(page_num).map_err(WriteRowError::GetPage)?;

        let row_offset = (self.nb_rows % Self::ROWS_PER_PAGE) * Row::MAX_SIZE;
        let row_range = row_offset..(row_offset + Row::MAX_SIZE);

        let serialized_row = <[u8; Row::MAX_SIZE]>::from(row);
        page[row_range].copy_from_slice(&serialized_row);
        self.nb_rows += 1;

        Ok(())
    }
}

#[cfg(test)]
mod table_test {}
