use std::io;
use std::io::{Seek, SeekFrom, Read, Write};
use std::sync::{LazyLock, Mutex};
use std::fs::{File, OpenOptions};

use crate::table::TABLE;

pub static PAGER: LazyLock<Mutex<Pager>> = LazyLock::new(|| Mutex::new(Pager::new()));

type PageType = Box<[u8; Page::SIZE]>;

pub struct Page(PageType);
impl Page {
    pub const SIZE: usize = 4096;

    pub fn new(b: PageType) -> Self {
        Self(b)
    }
}
impl std::ops::Deref for Page {
    type Target = PageType;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for Page {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl Default for Page {
    fn default() -> Self {
        Self(Box::new([0; Self::SIZE]))
    }
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub enum GetPageError {
    MaxPageExceeded,
    IoError(io::Error),
    NotAllBytesRead,
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub enum SaveToDiskError {
    NoFileToWriteProvided,
    PoisonedTable,
    IoError(io::Error),
    NotAllBytesWritten,
}

pub struct Pager {
    save_file: Option<File>,
    pages: [Option<Page>; Self::MAX_PAGES],
}
impl Pager {
    pub const MAX_PAGES: usize = 100;

    pub fn new() -> Self {
        Self {
            save_file: None,
            pages: [const { None }; Self::MAX_PAGES],
        }
    }

    pub fn set_open_save_file(&mut self, file_path: &str) -> io::Result<()> {
        let file = OpenOptions::new().read(true).write(true).open(file_path)?;
        self.save_file = Some(file);
        self.pages = [const { None }; Self::MAX_PAGES];
        Ok(())
    }

    fn get_file_length(&self) -> Option<io::Result<u64>> {
        let save_file: &File = self.save_file.as_ref()?;
        match save_file.metadata() {
            Ok(metadata) => Some(Ok(metadata.len())),
            Err(io_error) => Some(Err(io_error)),
        }
    }

    pub fn get_page(&mut self, page_num: usize) -> Result<&mut Page, GetPageError> {
        if page_num >= Self::MAX_PAGES {
            return Err(GetPageError::MaxPageExceeded);
        }

        if self.pages[page_num].is_some() {
            return Ok(self.pages[page_num].as_mut().unwrap());
        }

        let page = if let Some(save_file) = self.save_file.as_mut() {
            let offset = 8 + Page::SIZE * page_num;
            let seek_from = SeekFrom::Start(offset as u64);
            let _ = save_file.seek(seek_from).map_err(GetPageError::IoError)?;
            let mut page = Page::default();
            let _ = save_file.read_exact(&mut page[..]).map_err(GetPageError::IoError)?;
            page
        } else {
            Page::default()
        };

        self.pages[page_num] = Some(page);
        Ok(self.pages[page_num].as_mut().unwrap())
    }

    pub fn save_to_disk(&mut self, file_path: Option<&str>) -> Result<(), SaveToDiskError> {
        let save_file = if let Some(path) = file_path {
            &mut File::create(path).map_err(SaveToDiskError::IoError)?
        } else if let Some(file) = self.save_file.as_mut() {
            file
        } else {
            return Err(SaveToDiskError::NoFileToWriteProvided);
        };

        let Ok(table) = TABLE.lock() else {
            return Err(SaveToDiskError::PoisonedTable);
        };

        let table_nb_row_bytes = table.get_nb_rows().to_be_bytes();
        let table_nb_row_bytes_written = save_file
            .write(&table_nb_row_bytes)
            .map_err(SaveToDiskError::IoError)?;

        if table_nb_row_bytes.len() != table_nb_row_bytes_written {
            return Err(SaveToDiskError::NotAllBytesWritten);
        }

        for page_bytes in self.pages.iter().flatten() {
            let table_page_bytes_written = save_file
            .write(&page_bytes[..])
            .map_err(SaveToDiskError::IoError)?;
            if page_bytes.len() != table_page_bytes_written {
                return Err(SaveToDiskError::NotAllBytesWritten);
            }
        }

        Ok(())
    }
}
