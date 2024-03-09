use crate::unique_id::*;
use std::any::*;
use std::marker::*;
use std::mem::*;
use std::ops::*;
use std::ptr::*;

/// A copyable handle to an entry of type `T` in a [`DynVec`].
pub struct DynEntry<T: 'static + ?Sized> {
    /// The ID of the vector.
    vec_id: u64,
    /// The offset of the entry within the vector.
    offset: *mut T
}

impl<T: 'static + ?Sized> Clone for DynEntry<T> {
    fn clone(&self) -> Self {
        Self {
            vec_id: self.vec_id,
            offset: self.offset
        }
    }
}

impl<T: 'static + ?Sized + Unsize<U>, U: ?Sized> CoerceUnsized<DynEntry<U>> for DynEntry<T> {}

impl<T: 'static + ?Sized> Copy for DynEntry<T> {}

impl<T: 'static + ?Sized> std::fmt::Debug for DynEntry<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple(type_name::<DynEntry<T>>()).field(&self.offset).finish()
    }
}

unsafe impl<T: 'static + ?Sized> Send for DynEntry<T> {}

unsafe impl<T: 'static + ?Sized> Sync for DynEntry<T> {}

/// A heterogenous collection of types, stored in a continuous segment of memory.
/// All of the objects are dropped at once, when the vector is dropped.
#[derive(Debug)]
pub struct DynVec {
    /// The inner buffer containing the data for the objects.
    inner: Vec<u8>,
    /// The position of the last entry in the vector.
    last_entry: usize,
    /// The ID of the vector.
    id: u64
}

impl DynVec {
    /// Creates a new, empty dynamic vector.
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Creates a new, empty dynamic vector which can hold at least
    /// `capacity` bytes' worth of objects.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Vec::with_capacity(capacity),
            last_entry: usize::MAX,
            id: unique_id()
        }
    }

    /// Clears this dynamic vector, dropping all objects in it.
    /// The dynamic vector is not deallocated.
    pub fn clear(&mut self) {
        unsafe {
            if !self.inner.is_empty() {
                let mut next_entry = 0;
                while next_entry != usize::MAX {
                    let value = self.get_mut::<TypedDynEntry<()>>(next_entry).assume_init_ref();
                    (value.drop)(&value.value as *const _ as *mut _);
                    next_entry = value.next;
                }
                self.inner.clear();
                self.id = unique_id();
            }
        }
    }

    /// Pushes a new object into the dynamic vector, returning a handle to the allocation.
    pub fn push<T: 'static + Send + Sync>(&mut self, value: T) -> DynEntry<T> {
        unsafe {
            let alignment = align_of::<TypedDynEntry<T>>();
            let alignment_mask = !(alignment - 1);
            let next_position = (self.inner.len() + alignment - 1) & alignment_mask;
            let new_len = next_position + size_of::<TypedDynEntry<T>>();

            self.inner.reserve(new_len.saturating_sub(self.inner.len()));
            self.get_mut(next_position).write(TypedDynEntry {
                next: usize::MAX,
                drop: transmute(drop_in_place::<T> as unsafe fn(*mut T)),
                value
            });
            self.inner.set_len(new_len);

            if self.last_entry != usize::MAX {
                self.get_mut(self.last_entry).write(next_position);
            }

            self.last_entry = next_position;

            DynEntry { vec_id: self.id, offset: (next_position + 2 * size_of::<usize>()) as *mut T, }
        }
    }

    /// Gets a mutable reference to an object of type `T` at `offset` bytes into the vector.
    /// 
    /// # Safety
    /// 
    /// The entirety of the object at the offset must be within the vector's bounds
    /// and satisfy the alignment of `T`.
    unsafe fn get_mut<T: 'static>(&mut self, offset: usize) -> &mut MaybeUninit<T> {
        &mut *self.inner.as_mut_ptr().add(offset).cast()
    }
}

impl Default for DynVec {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ?Sized> Index<DynEntry<T>> for DynVec {
    type Output = T;

    fn index(&self, index: DynEntry<T>) -> &Self::Output {
        unsafe {
            assert!(self.id == index.vec_id, "Attempted to get dynamic entry from different vector.");
            &*from_raw_parts(self.inner.as_ptr().add(index.offset.cast::<u8>() as usize).cast(), metadata(index.offset))
        }
    }
}

impl<T> IndexMut<DynEntry<T>> for DynVec {
    fn index_mut(&mut self, index: DynEntry<T>) -> &mut Self::Output {
        unsafe {
            assert!(self.id == index.vec_id, "Attempted to get dynamic entry from different vector.");
            &mut *from_raw_parts_mut(self.inner.as_mut_ptr().add(index.offset.cast::<u8>() as usize).cast(), metadata(index.offset))
        }
    }
}

impl Drop for DynVec {
    fn drop(&mut self) {
        self.clear();
    }
}

/// A node in a linked list within the dynamic vector used to record
/// the value of an object and how to dro pit.
#[repr(C)]
struct TypedDynEntry<T: 'static> {
    /// The offset of the next typed entry in the vector.
    next: usize,
    /// The function that should be used to drop the object.
    drop: unsafe fn(*mut ()),
    /// The value of the object.
    value: T
}