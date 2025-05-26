use std::fs::{File, OpenOptions};
use std::io;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};

use crate::slice_pointer::{SlicePointer, SlicePointerMut};

type PageType = Box<[u8; Page::SIZE]>;

#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Page(PageType);
impl Page {
    pub const SIZE: usize = 4096;
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
pub enum SetOpenSaveFileError {
    IoError(io::Error),
    PoisonedTable,
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub enum GetPageError {
    MaxPageReached,
    IoError(io::Error),
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub enum SaveToDiskError {
    NoFileToWriteProvided,
    PoisonedTable,
    IoError(io::Error),
    NotAllBytesWritten,
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Pager {
    save_file: Option<File>,
    pages: [Option<Page>; Self::MAX_PAGES],
}
impl Pager {
    pub const MAX_PAGES: usize = 100;

    pub fn new(file_path: Option<&str>) -> Self {
        let save_file = if let Some(file_path) = file_path {
            Some(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(file_path)
                    .unwrap(),
            )
        } else {
            None
        };

        Self {
            save_file,
            pages: [const { None }; Self::MAX_PAGES],
        }
    }

    pub fn set_open_save_file(&mut self, file_path: &str) -> Result<(), SetOpenSaveFileError> {
        // TODO: sauvegarder le chemin même si le fichier n'existe pas.
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file_path)
            .map_err(SetOpenSaveFileError::IoError)?;

        self.save_file = Some(file);

        self.pages = [const { None }; Self::MAX_PAGES];
        Ok(())
    }

    fn load_or_create_page(&mut self, page_num: usize) -> Page {
        if let Some(save_file) = self.save_file.as_mut() {
            let offset = Page::SIZE * page_num;
            let seek_from = SeekFrom::Start(offset as u64);
            let _ = save_file.seek(seek_from).unwrap();
            let mut page = Page::default();
            save_file.read_exact(&mut page[..]).unwrap();
            page
        } else {
            Page::default()
        }
    }

    pub fn get(&mut self, page_num: usize) -> SlicePointer {
        assert!(page_num < Self::MAX_PAGES, "Max page reached.");

        if self.pages[page_num].is_some() {
            let page = self.pages[page_num].as_mut().unwrap();
            return SlicePointer::from(&page[..]);
        }

        let page = self.load_or_create_page(page_num);

        self.pages[page_num] = Some(page);
        let page = self.pages[page_num].as_mut().unwrap();
        SlicePointer::from(&page[..])
    }

    pub fn get_mut(&mut self, page_num: usize) -> SlicePointerMut {
        assert!(page_num < Self::MAX_PAGES, "Max page reached.");

        if self.pages[page_num].is_some() {
            let page = self.pages[page_num].as_mut().unwrap();
            return SlicePointerMut::from(&mut page[..]);
        }

        let page = self.load_or_create_page(page_num);

        self.pages[page_num] = Some(page);
        let page = self.pages[page_num].as_mut().unwrap();
        SlicePointerMut::from(&mut page[..])
    }

    pub fn get_page(&mut self, page_num: usize) -> Result<&mut Page, GetPageError> {
        if page_num >= Self::MAX_PAGES {
            return Err(GetPageError::MaxPageReached);
        }

        if self.pages[page_num].is_some() {
            // Je ne peux pas utiliser le modèle `if let` sinon j'ai une ref.
            #[allow(clippy::unwrap_used)]
            return Ok(self.pages[page_num].as_mut().unwrap());
        }

        let page = if let Some(save_file) = self.save_file.as_mut() {
            let offset = 8 + Page::SIZE * page_num;
            let seek_from = SeekFrom::Start(offset as u64);
            let _ = save_file.seek(seek_from).map_err(GetPageError::IoError)?;
            let mut page = Page::default();
            if let Err(io_error) = save_file.read_exact(&mut page[..]) {
                if io_error.kind() != ErrorKind::UnexpectedEof {
                    return Err(GetPageError::IoError(io_error));
                }
            }
            page
        } else {
            Page::default()
        };

        self.pages[page_num] = Some(page);
        // L'option ici est nécessairement `Some`.
        #[allow(clippy::unwrap_used)]
        Ok(self.pages[page_num].as_mut().unwrap())
    }

    pub fn save_to_disk(
        &mut self,
        file_path: Option<&str>,
    ) -> Result<(), SaveToDiskError> {
        let save_file = if let Some(path) = file_path {
            &mut File::create(path).map_err(SaveToDiskError::IoError)?
        } else if let Some(file) = self.save_file.as_mut() {
            let () = file.set_len(0).map_err(SaveToDiskError::IoError)?;
            let seek_from = SeekFrom::Start(0);
            let _ = file.seek(seek_from).map_err(SaveToDiskError::IoError)?;
            file
        } else {
            return Err(SaveToDiskError::NoFileToWriteProvided);
        };

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
impl Default for Pager {
    fn default() -> Self {
        Self {
            save_file: None,
            pages: [const { None }; Self::MAX_PAGES],
        }
    }
}
