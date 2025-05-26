macro_rules! impl_slice_pointer {
    ($slice_pointer:ty) => {
        impl $slice_pointer {
            pub fn len(&self) -> usize {
                self.new_len
            }

            pub fn set_len(&mut self, new_len: usize) {
                unsafe {
                    assert!(
                        self.new_pointer.add(new_len)
                            <= self.orig_pointer.add(self.orig_len),
                        "Pointer out of bounds."
                    );
                }
                self.new_len = new_len;
            }
        }
        impl std::ops::Add<usize> for &$slice_pointer {
            type Output = SlicePointer;

            fn add(self, i: usize) -> Self::Output {
                let new_pointer = unsafe { self.new_pointer.add(i) };

                unsafe {
                    assert!(new_pointer <= self.orig_pointer.add(self.orig_len));
                }

                Self::Output {
                    orig_pointer: self.orig_pointer,
                    orig_len: self.orig_len,
                    new_pointer,
                    new_len: self.new_len,
                }
            }
        }
        impl std::ops::AddAssign<usize> for $slice_pointer {
            fn add_assign(&mut self, i: usize) {
                unsafe {
                    self.new_pointer = self.new_pointer.add(i);
                }
            }
        }
        impl std::ops::AddAssign<usize> for &mut $slice_pointer {
            fn add_assign(&mut self, i: usize) {
                unsafe {
                    self.new_pointer = self.new_pointer.add(i);
                }
            }
        }
    };
}

#[derive(Clone)]
pub struct SlicePointer {
    orig_pointer: *const u8,
    orig_len: usize,
    new_pointer: *const u8,
    new_len: usize,
}
impl std::convert::From<&[u8]> for SlicePointer {
    fn from(slice: &[u8]) -> Self {
        Self {
            orig_pointer: slice as *const _ as *const u8,
            orig_len: slice.len(),
            new_pointer: slice as *const _ as *const u8,
            new_len: slice.len(),
        }
    }
}
impl std::convert::From<SlicePointer> for &[u8] {
    fn from(slice_pointer: SlicePointer) -> Self {
        let SlicePointer {
            new_pointer,
            new_len,
            ..
        } = slice_pointer;
        unsafe { std::slice::from_raw_parts(new_pointer as *const _, new_len) }
    }
}
impl_slice_pointer!(SlicePointer);

#[derive(Clone)]
pub struct SlicePointerMut {
    orig_pointer: *mut u8,
    orig_len: usize,
    new_pointer: *mut u8,
    new_len: usize,
}
impl std::convert::From<&mut [u8]> for SlicePointerMut {
    fn from(slice: &mut [u8]) -> Self {
        Self {
            orig_pointer: slice as *mut _ as *mut u8,
            orig_len: slice.len(),
            new_pointer: slice as *mut _ as *mut u8,
            new_len: slice.len(),
        }
    }
}
impl std::convert::From<SlicePointerMut> for &mut [u8] {
    fn from(slice_pointer_mut: SlicePointerMut) -> Self {
        let SlicePointerMut {
            new_pointer,
            new_len,
            ..
        } = slice_pointer_mut;
        unsafe { std::slice::from_raw_parts_mut(new_pointer as *mut _, new_len) }
    }
}
impl std::convert::From<&SlicePointer> for SlicePointerMut {
    fn from(slice_pointer: &SlicePointer) -> Self {
        Self {
            orig_pointer: slice_pointer.orig_pointer as *mut u8,
            orig_len: slice_pointer.orig_len,
            new_pointer: slice_pointer.new_pointer as *mut u8,
            new_len: slice_pointer.new_len,
        }
    }
}
impl_slice_pointer!(SlicePointerMut);
