use std::fs::File;
use std::io;
use std::io::Read;

use crate::table::Table;

#[cfg_attr(debug_assertions, derive(Debug))]
pub enum ReadDataFromFileError {
    IoError(io::Error),
    NotEnoughData,
    FileIsCorrupted(usize, Vec<Box<[u8; Table::PAGE_SIZE]>>),
}

pub type ReadDataFromFileResult =
    Result<(usize, Vec<Box<[u8; Table::PAGE_SIZE]>>), ReadDataFromFileError>;

pub fn read_data_from_file(path: &str) -> ReadDataFromFileResult {
    let mut file = File::open(path).map_err(ReadDataFromFileError::IoError)?;

    let mut bytes = Vec::<u8>::new();
    let bytes_read = file
        .read_to_end(&mut bytes)
        .map_err(ReadDataFromFileError::IoError)?;

    debug_assert_eq!(bytes.len(), bytes_read);

    let arr: [u8; 8] = bytes[..8][..]
        .try_into()
        .map_err(|_| ReadDataFromFileError::NotEnoughData)?;
    let nb_rows = usize::from_be_bytes(arr);

    let mut data_are_valid = true;
    let mut pages_vec = Vec::<Box<[u8; Table::PAGE_SIZE]>>::new();
    for offset in (8..bytes.len()).step_by(Table::PAGE_SIZE) {
        let page_range = offset..(offset + Table::PAGE_SIZE);

        if let Ok(arr) = <[u8; Table::PAGE_SIZE]>::try_from(&bytes[page_range][..]) {
            let page = Box::new(arr);
            pages_vec.push(page);
        } else {
            data_are_valid = false;
        }
    }

    if data_are_valid {
        Ok((nb_rows, pages_vec))
    } else {
        Err(ReadDataFromFileError::FileIsCorrupted(nb_rows, pages_vec))
    }
}
