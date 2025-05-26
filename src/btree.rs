use std::mem;

use crate::pager::Page;
use crate::row::Row;
use crate::slice_pointer::{SlicePointer, SlicePointerMut};

pub struct Cell(SlicePointer);
impl Cell {
    /*
     * Disposition du corps des nœuds aux extrémités de l'arbre.
     */
    pub const KEY_SIZE: usize = mem::size_of::<u32>();
    pub const KEY_OFFSET: usize = 0;

    pub const VALUE_SIZE: usize = Row::MAX_SIZE;
    pub const VALUE_OFFSET: usize = Self::KEY_OFFSET + Self::KEY_SIZE;

    pub const SIZE: usize = Self::KEY_SIZE + Self::VALUE_SIZE;
}
impl Cell {
    pub fn get_key(&self) -> SlicePointer {
        let mut key = self.0.clone();
        key.set_len(Self::KEY_SIZE);
        key
    }

    pub fn get_value(&self) -> SlicePointer {
        let mut value = self.0.clone();
        value += Self::KEY_SIZE;
        value.set_len(Self::VALUE_SIZE);
        value
    }
}
pub struct CellMut(SlicePointerMut);
impl CellMut {
    pub fn get_mut_key(&self) -> SlicePointerMut {
        let mut key = self.0.clone();
        key.set_len(Cell::KEY_SIZE);
        key
    }

    pub fn get_mut_value(&self) -> SlicePointerMut {
        let mut value = self.0.clone();
        value += Cell::KEY_SIZE;
        value.set_len(Cell::VALUE_SIZE);
        value
    }
}

pub enum Node {
    Internal(SlicePointer),
    Leaf(SlicePointer),
}
impl Node {
    /*
     * Disposition de l'entête commune des nœuds.
     */
    pub const NODE_TYPE_SIZE: usize = mem::size_of::<u8>();
    pub const NODE_TYPE_OFFSET: usize = 0;

    pub const IS_ROOT_SIZE: usize = mem::size_of::<u8>();
    pub const IS_ROOT_OFFSET: usize = Self::NODE_TYPE_SIZE;

    pub const PARENT_POINTER_SIZE: usize = mem::size_of::<u32>();
    pub const PARENT_POINTER_OFFSET: usize = Self::IS_ROOT_OFFSET + Self::IS_ROOT_SIZE;

    pub const COMMON_NODE_HEADER_SIZE: usize =
        Self::NODE_TYPE_SIZE + Self::IS_ROOT_SIZE + Self::PARENT_POINTER_SIZE;

    /*
     * Disposition de l'entête des nœuds aux extrémités de l'arbre.
     */
    pub const LEAF_NODE_NB_CELLS_SIZE: usize = mem::size_of::<u32>();
    pub const LEAF_NODE_NB_CELLS_OFFSET: usize = Self::COMMON_NODE_HEADER_SIZE;
    pub const LEAF_NODE_HEADER_SIZE: usize =
        Self::COMMON_NODE_HEADER_SIZE + Self::LEAF_NODE_NB_CELLS_SIZE;

    /*
     * Disposition du corps des nœuds aux extrémités de l'arbre.
     */
    pub const LEAF_NODE_SPACE_FOR_CELLS: usize = Page::SIZE - Node::LEAF_NODE_HEADER_SIZE;
    pub const LEAF_NODE_MAX_CELLS: usize =
        Self::LEAF_NODE_SPACE_FOR_CELLS / Cell::SIZE;
}
impl Node {
    pub fn leaf_node_get_nb_cells(&self) -> u32 {
        let Node::Leaf(slice_pointer) = self else {
            panic!("Not a leaf");
        };

        let mut num_cells_ptr = slice_pointer + Self::LEAF_NODE_NB_CELLS_OFFSET;
        num_cells_ptr.set_len(Self::LEAF_NODE_NB_CELLS_SIZE);

        let num_cells_bytes = <&[u8]>::from(num_cells_ptr);
        let num_cells_bytes = <[u8; 4]>::try_from(num_cells_bytes).unwrap();
        u32::from_be_bytes(num_cells_bytes)
    }

    pub fn leaf_node_get_cell(&self, cell_num: usize) -> Cell {
        let Node::Leaf(slice_pointer) = self else {
            panic!("Not a leaf");
        };

        let offset = Self::LEAF_NODE_HEADER_SIZE + cell_num * Cell::SIZE;
        let mut cell_ptr = slice_pointer + offset;
        cell_ptr.set_len(Cell::SIZE);

        Cell(cell_ptr)
    }

    pub fn leaf_node_get_mut_cell(&self, cell_num: usize) -> CellMut {
        let Node::Leaf(slice_pointer) = self else {
            panic!("Not a leaf");
        };

        let offset = Self::LEAF_NODE_HEADER_SIZE + cell_num * Cell::SIZE;
        let mut slice_pointer_mut = SlicePointerMut::from(slice_pointer);
        slice_pointer_mut += offset;
        slice_pointer_mut.set_len(Cell::SIZE);

        CellMut(slice_pointer_mut)
    }
}
