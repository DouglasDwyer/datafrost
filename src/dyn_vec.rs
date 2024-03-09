use crate::unique_id::*;
use std::alloc::*;
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
    offset: *mut T,
}

impl<T: 'static + ?Sized> Clone for DynEntry<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: 'static + ?Sized + Unsize<U>, U: ?Sized> CoerceUnsized<DynEntry<U>> for DynEntry<T> {}

impl<T: 'static + ?Sized> Copy for DynEntry<T> {}

impl<T: 'static + ?Sized> std::fmt::Debug for DynEntry<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple(type_name::<DynEntry<T>>())
            .field(&self.offset)
            .finish()
    }
}

unsafe impl<T: 'static + ?Sized> Send for DynEntry<T> {}

unsafe impl<T: 'static + ?Sized> Sync for DynEntry<T> {}

/// A heterogenous collection of types, stored in a continuous segment of memory.
/// All of the objects are dropped at once, when the vector is dropped.
#[derive(Debug)]
pub struct DynVec {
    /// The inner buffer containing the data for the objects.
    inner: *mut u8,
    /// The current alignment of the buffer.
    alignment: usize,
    /// The current length of the buffer.
    len: usize,
    /// The capacity of the buffer.
    capacity: usize,
    /// The position of the last entry in the vector.
    last_entry: usize,
    /// The ID of the vector.
    id: u64,
}

impl DynVec {
    /// Creates a new, empty dynamic vector.
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Creates a new, empty dynamic vector which can hold at least
    /// `capacity` bytes' worth of objects.
    pub fn with_capacity(capacity: usize) -> Self {
        unsafe {
            /// The initial alignment of the buffer, in bytes.
            const INITIAL_ALIGNMENT: usize = 16;
            let capacity = capacity.next_multiple_of(INITIAL_ALIGNMENT);

            Self {
                inner: if capacity > 0 {
                    alloc(Layout::from_size_align_unchecked(
                        capacity,
                        INITIAL_ALIGNMENT,
                    ))
                } else {
                    null_mut()
                },
                len: 0,
                alignment: INITIAL_ALIGNMENT,
                capacity,
                last_entry: usize::MAX,
                id: unique_id(),
            }
        }
    }

    /// Clears this dynamic vector, dropping all objects in it.
    /// The dynamic vector is not deallocated.
    pub fn clear(&mut self) {
        unsafe {
            if self.len != 0 {
                let mut next_entry = 0;
                while next_entry != usize::MAX {
                    let entry = self.inner.add(next_entry);
                    let value = &*entry.cast_const().cast::<TypedDynEntry<()>>();
                    next_entry = value.next;
                    let dropper = value.drop;
                    dropper(entry.cast());
                }
                self.id = unique_id();
            }
        }
    }

    /// Pushes a new object into the dynamic vector, returning a handle to the allocation.
    pub fn push<T: 'static + Send + Sync>(&mut self, value: T) -> DynEntry<T> {
        unsafe {
            let next_position = self.reserve(Layout::new::<TypedDynEntry<T>>());
            self.get_mut(next_position).write(TypedDynEntry {
                next: usize::MAX,
                drop: transmute(TypedDynEntry::<T>::drop_entry as unsafe fn(*mut TypedDynEntry<T>)),
                value,
            });

            if self.last_entry != usize::MAX {
                self.get_mut(self.last_entry).write(next_position);
            }

            self.last_entry = next_position;

            DynEntry {
                vec_id: self.id,
                offset: (next_position + offset_of!(TypedDynEntry<T>, value)) as *mut T,
            }
        }
    }

    /// Reserves space for an allocation with the provided layout in the vector,
    /// returning the offset of the allocation.
    fn reserve(&mut self, layout: Layout) -> usize {
        unsafe {
            let new_len = self.len + 2 * layout.size();
            if self.alignment < layout.align() || self.capacity < new_len {
                let old_capacity = self.capacity;
                let old_alignment = self.alignment;
                self.alignment = layout.align();
                self.capacity = new_len.next_multiple_of(self.alignment);

                let new = alloc(Layout::from_size_align_unchecked(
                    self.capacity,
                    self.alignment,
                ));
                if !self.inner.is_null() {
                    std::ptr::copy_nonoverlapping(self.inner.cast_const(), new, self.len);
                    dealloc(
                        self.inner,
                        Layout::from_size_align_unchecked(old_capacity, old_alignment),
                    );
                }
                self.inner = new;
            }

            self.len += Layout::from_size_align_unchecked(self.inner.add(self.len) as usize, 1)
                .padding_needed_for(layout.align());
            let position = self.len;
            self.len += layout.size();
            position
        }
    }

    /// Gets a mutable reference to an object of type `T` at `offset` bytes into the vector.
    ///
    /// # Safety
    ///
    /// The entirety of the object at the offset must be within the vector's bounds
    /// and satisfy the alignment of `T`.
    unsafe fn get_mut<T: 'static>(&mut self, offset: usize) -> &mut MaybeUninit<T> {
        &mut *self.inner.add(offset).cast()
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
            assert!(
                self.id == index.vec_id,
                "Attempted to get dynamic entry from different vector."
            );
            &*from_raw_parts(
                self.inner.add(index.offset.cast::<u8>() as usize).cast(),
                metadata(index.offset),
            )
        }
    }
}

impl<T: ?Sized> IndexMut<DynEntry<T>> for DynVec {
    fn index_mut(&mut self, index: DynEntry<T>) -> &mut Self::Output {
        unsafe {
            assert!(
                self.id == index.vec_id,
                "Attempted to get dynamic entry from different vector."
            );
            &mut *from_raw_parts_mut(
                self.inner.add(index.offset.cast::<u8>() as usize).cast(),
                metadata(index.offset),
            )
        }
    }
}

impl Drop for DynVec {
    fn drop(&mut self) {
        unsafe {
            self.clear();
            dealloc(
                self.inner,
                Layout::from_size_align_unchecked(self.capacity, self.alignment),
            );
        }
    }
}

unsafe impl Send for DynVec {}
unsafe impl Sync for DynVec {}

/// A node in a linked list within the dynamic vector used to record
/// the value of an object and how to dro pit.
#[repr(C)]
struct TypedDynEntry<T: 'static> {
    /// The offset of the next typed entry in the vector.
    next: usize,
    /// The function that should be used to drop the object.
    drop: unsafe fn(*mut ()),
    /// The value of the object.
    value: T,
}

impl<T: 'static> TypedDynEntry<T> {
    /// Drops the value contained in the referenced entry.
    unsafe fn drop_entry(entry: *mut Self) {
        addr_of_mut!((*entry).value).drop_in_place();
    }
}
