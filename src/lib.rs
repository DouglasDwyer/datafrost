#![feature(alloc_layout_extra)]
#![feature(coerce_unsized)]
#![feature(downcast_unchecked)]
#![feature(non_null_convenience)]
#![feature(ptr_metadata)]
#![feature(sync_unsafe_cell)]
#![feature(unsize)]
#![allow(private_interfaces)]
#![deny(missing_docs)]
#![deny(clippy::missing_docs_in_private_items)]

//! #### Data format and acceleration structure management
//!
//! `datafrost` is a data-oriented resource management and scheduling library. It implements a graphics API-inspired interface that allows one to cleanly and efficiently:
//!
//! - Create primary data objects, and define "derived" datatypes whose contents are generated from the primary format.
//! - Track how a primary object changes and automatically update the affected parts of the derived formats.
//! - Schedule commands to asynchronously and concurrently read or modify data objects.
//!   - `datafrost` guarantees optimal scheduling by building a directed acyclic graph to represent pending operations.
//!   - Multiple commands which reference different data, or immutably reference the same data, will execute in parallel.
//!   - Commands which mutably access the same data run in sequence, without the possibility of data races.
//! - Map the contents of data objects and read their results on the main thread.
//!
//! ## Usage
//!
//! The following is an abridged example of how to use `datafrost`. The full code may be found in the
//! [examples folder](/examples/derived.rs). To begin, we define the data formats that our code will use:
//!
//! ```ignore
//! use datafrost::*;
//! use std::ops::*;
//!
//! /// First, we define a general "kind" of data that our program will use.
//! /// In this case, let's imagine that we want to efficiently deal with
//! /// arrays of numbers.
//! pub struct NumberArray;
//!
//! /// Defines the layout of an array of numbers.
//! pub struct NumberArrayDescriptor {
//!     /// The length of the array.
//!     pub len: usize
//! }
//!
//! impl Kind for NumberArray { .. }
//!
//! /// Next, we define the primary data format that we would like
//! /// to use and modify - an array of specifically `u32`s.
//! pub struct PrimaryArray(Vec<u32>);
//!
//! impl Format for PrimaryArray { .. }
//!
//! /// Now, let's imagine that we want to efficiently maintain an
//! /// acceleration structure containing all of the numbers in
//! /// the array, but doubled. So, we define the format.
//! pub struct DoubledArray(Vec<u32>);
//!
//! impl Format for DoubledArray { .. }
//!
//! /// Our goal is for `datafrost` to automatically update the doubled
//! /// array whenever the primary array changes. Thus, we implement
//! /// a way for it do so.
//! pub struct DoublePrimaryArray;
//!
//! impl DerivedDescriptor<PrimaryArray> for DoublePrimaryArray {
//!     type Format = DoubledArray;
//!
//!     fn update(&self, data: &mut DoubledArray, parent: &PrimaryArray, usages: &[&Range<usize>]) {
//!         // Loop over all ranges of the array that have changed, and
//!         // for each value in the range, recompute the data.
//!         for range in usages.iter().copied() {
//!             for i in range.clone() {
//!                 data.0[i] = 2 * parent.0[i];
//!             }
//!         }
//!     }
//! }
//! ```
//!
//! Now that our data and its derived formats are defined, we can create instances of
//! it and schedule commands to act upon the data:
//!
//! ```ignore
//! // Create a new context.
//! let ctx = DataFrostContext::new(ContextDescriptor {
//!     label: Some("my context")
//! });
//!
//! // Allocate a new primary array object, which has a doubled
//! // array as a derived format.
//! let data = ctx.allocate::<PrimaryArray>(AllocationDescriptor {
//!     descriptor: NumberArrayDescriptor { len: 7 },
//!     label: Some("my data"),
//!     derived_formats: &[Derived::new(DoublePrimaryArray)]
//! });
//!
//! // Create a command buffer to record operations to execute
//! // on our data.
//! let mut command_buffer = CommandBuffer::new(CommandBufferDescriptor { label: Some("my command buffer") });
//!
//! // Schedule a command to fill the primary number array with some data.
//! let view = data.view::<PrimaryArray>();
//! let view_clone = view.clone();
//! command_buffer.schedule(CommandDescriptor {
//!     label: Some("fill array"),
//!     views: &[&view.as_mut(4..6)],
//!     command: move |ctx| ctx.get_mut(&view_clone).0[4..6].fill(33)
//! });
//!
//! // Schedule a command to map the contents of the derived acceleration structure
//! // so that we may view them synchronously.
//! let derived = command_buffer.map(&data.view::<DoubledArray>().as_const());
//!
//! // Submit the buffer for processing.
//! ctx.submit(Some(command_buffer));
//!
//! // The doubled acceleration structure automatically contains the
//! // correct, up-to-date data!
//! assert_eq!(&[0, 0, 0, 0, 66, 66, 0], &ctx.get(&derived).0[..]);
//! ```

use crate::dyn_vec::*;
use crate::graph::*;
use crate::unique_id::*;
pub use mutability_marker::*;
use private::*;
use slab::*;
use std::any::*;
use std::cell::*;
use std::hint::*;
use std::marker::*;
use std::mem::*;
use std::ops::*;
use std::pin::*;
use std::sync::atomic::*;
use std::sync::mpsc::*;
use std::sync::*;
use sync_rw_cell::*;
use task_pool::*;
#[allow(unused_imports)]
use wasm_sync::{Condvar, Mutex};

/// Defines a dynamic vector type for efficient allocation of variable-sized, hetegenous objects.
mod dyn_vec;

/// Implements a directed acyclic graph structure for work scheduling.
mod graph;

/// Defines a way to create unique IDs.
mod unique_id;

/// Denotes a general class of formats, which all share similar data.
/// Formats of the same kind may be derived from one another.
pub trait Kind: 'static + Send + Sync {
    /// A structure which holds properties common to all formats of this kind.
    type FormatDescriptor: Send + Sync;

    /// A structure which describes the parts of a format that have changed.
    type UsageDescriptor: Send + Sync + Sized;
}

/// A certain format of data.
pub trait Format: 'static + Send + Sync + Sized {
    /// The kind of data that this format represents.
    type Kind: Kind;

    /// Allocates a new object of this format for the provided descriptor with
    /// unspecified contents.
    fn allocate(descriptor: &<Self::Kind as Kind>::FormatDescriptor) -> Self;
}

/// Allows a format to act as a maintained "copy" of a parent
/// by synchronizing its contents with those of the parent.
pub trait DerivedDescriptor<F: Format>: 'static + Send + Sync + Sized {
    /// The format that should be created for this descriptor.
    type Format: Format<Kind = F::Kind>;

    /// Updates the given data based upon the portions of the parent that have changed.
    fn update(
        &self,
        data: &mut Self::Format,
        parent: &F,
        usages: &[&<F::Kind as Kind>::UsageDescriptor],
    );
}

/// A type-erased instance of [`View`] that is used to specify
/// data usage in a [`CommandDescriptor`].
pub trait ViewUsage: Send + Sync + ViewUsageInner {}

/// Determines how a new data object will be allocated.
pub struct AllocationDescriptor<'a, F: Format> {
    /// The descriptor describing the layout of the data.
    pub descriptor: <F::Kind as Kind>::FormatDescriptor,
    /// An optional label to associate with the object.
    pub label: Option<&'static str>,
    /// Derived formats that should be set up to track the parent format.
    pub derived_formats: &'a [Derived<F>],
}

/// Records a list of operations that should be executed on formatted data.
pub struct CommandBuffer {
    /// The internal dynamic vector that stores command information.
    command_list: DynVec,
    /// A handle to the first command in the list.
    first_command_entry: Option<DynEntry<CommandEntry>>,
    /// The label of this command buffer.
    label: Option<&'static str>,
    /// A handle to the last command in the list.
    last_command_entry: Option<DynEntry<CommandEntry>>,
}

impl CommandBuffer {
    /// Creates a new command buffer with the provided descriptor.
    pub fn new(descriptor: CommandBufferDescriptor) -> Self {
        /// The default amount of space to allocate for a new command buffer.
        const DEFAULT_ALLOCATION_SIZE: usize = 2048;

        Self {
            command_list: DynVec::with_capacity(DEFAULT_ALLOCATION_SIZE),
            label: descriptor.label,
            first_command_entry: None,
            last_command_entry: None,
        }
    }

    /// Requires that all referenced views are available and ready
    /// at this point in the buffer.
    pub fn fence(&mut self, views: &[&dyn ViewUsage]) {
        unsafe {
            let computation = SyncUnsafeCell::new(Some(Computation::Execute {
                command: self.command_list.push(()),
            }));
            let first_view_entry = self.push_views(views);
            let next_command = self.command_list.push(CommandEntry {
                computation,
                first_view_entry,
                label: Some("Fence"),
                next_instance: None,
            });
            self.update_first_last_command_entries(next_command);
        }
    }

    /// Maps a format for synchronous viewing.
    pub fn map<M: UsageMutability, F: Format>(
        &mut self,
        view: &ViewDescriptor<M, F>,
    ) -> Mapped<M, F> {
        unsafe {
            assert!(
                TypeId::of::<M>() == TypeId::of::<Const>() || !view.view.derived,
                "Attempted to mutably map derived view of object{} in command buffer{}",
                FormattedLabel(" ", view.view.inner.inner.label, ""),
                FormattedLabel(" ", self.label, "")
            );

            let inner = Arc::new(MappedInner {
                context_id: view.view.inner.inner.context_id,
                command_context: UnsafeCell::new(MaybeUninit::uninit()),
                label: view.view.inner.inner.label,
                map_state: MapObjectState::default(),
            });

            let computation = SyncUnsafeCell::new(Some(Computation::Map {
                inner: Some(inner.clone()),
            }));

            let first_view_entry = self.push_views(&[view]);
            let next_command = self.command_list.push(CommandEntry {
                computation,
                first_view_entry,
                label: Some("Map format"),
                next_instance: None,
            });

            self.update_first_last_command_entries(next_command);

            Mapped {
                inner,
                view: view.view.clone(),
                marker: PhantomData,
            }
        }
    }

    /// Schedules a command to execute on format data.
    pub fn schedule(
        &mut self,
        descriptor: CommandDescriptor<impl Send + Sync + FnOnce(CommandContext)>,
    ) {
        unsafe {
            let computation = SyncUnsafeCell::new(Some(Computation::Execute {
                command: self
                    .command_list
                    .push(SyncUnsafeCell::new(Some(descriptor.command))),
            }));
            let first_view_entry = self.push_views(descriptor.views);
            let next_command = self.command_list.push(CommandEntry {
                computation,
                first_view_entry,
                label: descriptor.label,
                next_instance: None,
            });
            self.update_first_last_command_entries(next_command);
        }
    }

    /// Creates a linked list of views in the command list.
    fn push_views(&mut self, list: &[&dyn ViewUsage]) -> Option<DynEntry<ViewEntry>> {
        unsafe {
            let mut view_iter = list.iter();
            if let Some(first) = view_iter.next() {
                let view = first.add_to_list(&mut self.command_list);
                let first_entry = self.command_list.push(ViewEntry {
                    next_instance: None,
                    view,
                });
                let mut previous_entry = first_entry;

                for to_add in view_iter {
                    let view = to_add.add_to_list(&mut self.command_list);
                    let next_entry = self.command_list.push(ViewEntry {
                        next_instance: None,
                        view,
                    });

                    self.command_list
                        .get_unchecked_mut(previous_entry)
                        .next_instance = Some(next_entry);
                    previous_entry = next_entry;
                }

                Some(first_entry)
            } else {
                None
            }
        }
    }

    /// Updates the first and last command entries after the provided command
    /// has been added to the command list.
    ///
    /// # Safety
    ///
    /// The `self.last_command_entry` field, if `Some`, must refer to a valid command list value.
    unsafe fn update_first_last_command_entries(&mut self, next_command: DynEntry<CommandEntry>) {
        unsafe {
            if self.first_command_entry.is_none() {
                self.first_command_entry = Some(next_command);
            } else if let Some(entry) = self.last_command_entry {
                self.command_list.get_unchecked_mut(entry).next_instance = Some(next_command);
            }
            self.last_command_entry = Some(next_command);
        }
    }
}

/// Specifies the parameters of a new [`CommandBuffer`].
#[derive(Copy, Clone, Debug, Default)]
pub struct CommandBufferDescriptor {
    /// An optional label to associate with the object.
    pub label: Option<&'static str>,
}

/// Describes the present status of a submitted command buffer.
#[derive(Copy, Clone, Debug, Default)]
pub struct CommandBufferStatus {
    /// The number of commands left to process in this command buffer.
    pub incomplete_commands: u32,
}

impl CommandBufferStatus {
    /// Whether all commands in this command buffer have been processed.
    pub fn complete(&self) -> bool {
        self.incomplete_commands == 0
    }
}

/// Uniquely identifies a submitted command buffer,
/// allowing one to query its completion status.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CommandBufferSubmission {
    /// The unique ID of the command buffer.
    unique_id: u64,
    /// The ID of the context that created this command buffer.
    context_id: u64,
    /// The ID of the command buffer within the `Slab`.
    command_buffer_id: u16,
}

/// Allows for interacting with format data during command execution.
/// When this object is dropped, a command is considered complete.

pub struct CommandContext {
    /// The inner context state.
    inner: ManuallyDrop<CommandContextInner>,
}

impl CommandContext {
    /// Immutably gets the data referenced by the view.
    pub fn get<F: Format>(&self, view: &View<F>) -> ViewRef<Const, F> {
        ViewRef {
            reference: self.find_view::<Const, _>(view).borrow(),
            marker: PhantomData,
        }
    }

    /// Mutably gets the data referenced by the view, and records the usage for updating derived formats.
    /// This function will panic if `view` refers to a derived format.
    pub fn get_mut<F: Format>(&self, view: &View<F>) -> ViewRef<Mut, F> {
        ViewRef {
            reference: self.find_view::<Mut, _>(view).borrow_mut(),
            marker: PhantomData,
        }
    }

    /// Gets the proper reference to the view, or panics if the view was invalid.
    fn find_view<M: Mutability, F: Format>(&self, view: &View<F>) -> &RwCell<*mut ()> {
        let mutable = TypeId::of::<Mut>() == TypeId::of::<M>();
        if let Some(command_view) = self
            .inner
            .views
            .iter()
            .find(|x| x.id == view.id && x.mutable == mutable)
        {
            &command_view.value
        } else {
            panic!(
                "View{} was not referenced by command{}{}",
                FormattedLabel(" ", view.inner.inner.label, ""),
                FormattedLabel(" ", self.inner.label, ""),
                FormattedLabel(
                    " (from command buffer ",
                    self.inner.command_buffer_label,
                    ")"
                )
            );
        }
    }
}

impl std::fmt::Debug for CommandContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandContext").finish()
    }
}

impl Drop for CommandContext {
    fn drop(&mut self) {
        unsafe {
            self.inner
                .context
                .inner
                .lock()
                .expect("Failed to lock context.")
                .complete_command(self.inner.command_id, &self.inner.context);
            ManuallyDrop::drop(&mut self.inner);
        }
    }
}

/// Describes a command to execute.
pub struct CommandDescriptor<'a, C: 'static + Send + Sync + FnOnce(CommandContext)> {
    /// The label associated with this command.
    pub label: Option<&'static str>,
    /// The command to execute asynchronously.
    pub command: C,
    /// A list of views that the command will access via the [`CommandContext`].
    pub views: &'a [&'a dyn ViewUsage],
}

impl<'a, C: 'static + Send + Sync + FnOnce(CommandContext)> std::fmt::Debug
    for CommandDescriptor<'a, C>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CommandDescriptor")
            .field(&self.label)
            .finish()
    }
}

/// Determines how a [`DataFrostContext`] should behave.
#[derive(Copy, Clone, Debug, Default)]
pub struct ContextDescriptor {
    /// An optional label to associate with the object.
    pub label: Option<&'static str>,
}

/// References an object of a specific [`Kind`]. The object is backed
/// by a single primary format and some number of derived formats.
pub struct Data<K: Kind> {
    /// The inner representation of this object.
    inner: Arc<DataInner<K>>,
}

impl<K: Kind> Data<K> {
    /// Creates a view of this data of the given format and mutability.
    /// The format must either be the primary format or one of the derived
    /// formats specified during object creation. If this is a mutable view,
    /// then the format must be the primary format. Using views with invalid
    /// mutability or formats will lead to panics when [`DataFrostContext::submit`]
    /// is called.
    pub fn view<F: Format<Kind = K>>(&self) -> View<F> {
        let (id, derived) = if TypeId::of::<F>() == self.inner.format_id {
            (self.inner.id, false)
        } else if let Some((_, id)) = self
            .inner
            .derived_formats
            .iter()
            .copied()
            .find(|&(id, _)| id == TypeId::of::<F>())
        {
            //assert!(TypeId::of::<M>() == TypeId::of::<Const>(), "Attempted to mutably access derived format {} of object{}", type_name::<F>(), FormattedLabel(" ", self.inner.label, ""));
            (id, true)
        } else {
            panic!(
                "Derived format {} of object{} did not exist",
                type_name::<F>(),
                FormattedLabel(" ", self.inner.label, "")
            )
        };

        View {
            inner: self.clone(),
            id,
            derived,
            marker: PhantomData,
        }
    }
}

impl<K: Kind> Clone for Data<K> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K: Kind> std::fmt::Debug for Data<K>
where
    K::FormatDescriptor: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(label) = self.inner.label {
            f.debug_tuple("Data")
                .field(&label)
                .field(&self.inner.descriptor)
                .finish()
        } else {
            f.debug_tuple("Data").field(&self.inner.descriptor).finish()
        }
    }
}

impl<K: Kind> Deref for Data<K> {
    type Target = K::FormatDescriptor;

    fn deref(&self) -> &Self::Target {
        &self.inner.descriptor
    }
}

impl<K: Kind> PartialEq for Data<K> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl<K: Kind> Eq for Data<K> {}

/// Manages a collection of formatted data, coordinates the execution of commands
/// against the data, and automatically updates derived formats using the results
/// of those commands.
#[derive(Clone)]
pub struct DataFrostContext {
    /// The shared data that composes this context.
    holder: Arc<ContextHolder>,
}

impl DataFrostContext {
    /// Allocates a new, empty context.
    pub fn new(_: ContextDescriptor) -> Self {
        let (object_update_sender, object_updates) = channel();

        let change_notifier = ChangeNotifier::default();
        let change_listener = Condvar::new();
        let context_id = unique_id();
        let inner = Mutex::new(ContextInner {
            active_command_buffers: Slab::new(),
            context_id,
            compute_graph: DirectedAcyclicGraph::new(),
            critical_nodes: DirectedAcyclicGraphFlags::new(),
            critical_top_level_nodes: DirectedAcyclicGraphFlags::new(),
            objects: Slab::new(),
            object_update_sender,
            object_updates,
            stalled: true,
            temporary_node_buffer: Vec::new(),
            top_level_nodes: DirectedAcyclicGraphFlags::new(),
        });

        let holder = Arc::new(ContextHolder {
            change_notifier,
            change_listener,
            context_id,
            inner,
        });

        Self { holder }
    }

    /// Creates a new object from the provided descriptor. Objects are also created for each derived format,
    /// and will automatically update whenever this object is changed.
    pub fn allocate<F: Format>(&self, descriptor: AllocationDescriptor<F>) -> Data<F::Kind> {
        self.inner().allocate(descriptor)
    }

    /// Immutably gets the data referenced by the mapping. This function will block if the mapping is not yet available.
    pub fn get<'a, M: Mutability, F: Format>(
        &self,
        mapping: &'a Mapped<M, F>,
    ) -> ViewRef<'a, Const, F> {
        unsafe {
            self.wait_for_mapping(mapping);
            return (*mapping.inner.command_context.get())
                .assume_init_ref()
                .get(&mapping.view);
        }
    }

    /// Mutably gets the data referenced by the mapping. This function will block if the mapping is not yet available.
    pub fn get_mut<'a, F: Format>(&self, mapping: &'a mut Mapped<Mut, F>) -> ViewRef<'a, Mut, F> {
        unsafe {
            self.wait_for_mapping(mapping);
            return (*mapping.inner.command_context.get())
                .assume_init_mut()
                .get_mut(&mapping.view);
        }
    }

    /// Checks the current status of an executing command buffer.
    pub fn query(&self, submission: &CommandBufferSubmission) -> CommandBufferStatus {
        assert!(
            submission.context_id == self.holder.context_id,
            "Submission was not owned by this context."
        );
        self.inner()
            .active_command_buffers
            .get(submission.command_buffer_id as usize)
            .filter(|x| x.unique_id == submission.unique_id)
            .map(|x| CommandBufferStatus {
                incomplete_commands: x.remaining_commands,
            })
            .unwrap_or_default()
    }

    /// Schedules the provided command buffer for execution.
    pub fn submit(&self, buffer: CommandBuffer) -> CommandBufferSubmission {
        self.inner().submit(buffer, &self.holder)
    }

    /// Gets the inner data of this context.
    fn inner(&self) -> MutexGuard<ContextInner> {
        self.holder
            .inner
            .lock()
            .expect("Failed to obtain inner context.")
    }

    /// Waits for a mapping to become available.
    fn wait_for_mapping<M: Mutability, F: Format>(&self, mapping: &Mapped<M, F>) {
        unsafe {
            assert!(
                mapping.inner.context_id == self.holder.context_id,
                "Mapping was not from this context."
            );

            let query = mapping.inner.map_state.get();
            if query.queued {
                if !query.complete {
                    let mut inner = self.inner();
                    while !mapping.inner.map_state.get().complete {
                        if inner.top_level_nodes.get_unchecked(query.node) {
                            inner
                                .critical_top_level_nodes
                                .set_unchecked(query.node, false);
                            inner.top_level_nodes.set_unchecked(query.node, false);
                            inner
                                .compute_graph
                                .get_unchecked_mut(query.node)
                                .computation = Computation::Map { inner: None };
                            *mapping.inner.command_context.get() = MaybeUninit::new(
                                inner.create_command_context(&self.holder, query.node),
                            );
                            mapping.inner.map_state.set_complete();
                            return;
                        }

                        match inner.prepare_next_command::<true>(&self.holder) {
                            Some(Some(command)) => {
                                drop(inner);
                                command.execute();
                                inner = self.inner();
                            }
                            Some(None) => continue,
                            None => {
                                inner = self
                                    .holder
                                    .change_listener
                                    .wait(inner)
                                    .expect("Failed to lock mutex.")
                            }
                        }
                    }
                }
            } else {
                panic!(
                    "Attempted to map object{} before submitting the associated command buffer.",
                    FormattedLabel(" ", mapping.inner.label, "")
                )
            }
        }
    }
}

impl std::fmt::Debug for DataFrostContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DataFrostContext").finish()
    }
}

impl Default for DataFrostContext {
    fn default() -> Self {
        Self::new(ContextDescriptor::default())
    }
}

impl WorkProvider for DataFrostContext {
    fn change_notifier(&self) -> &ChangeNotifier {
        &self.holder.change_notifier
    }

    fn next_task(&self) -> Option<Box<dyn '_ + WorkUnit>> {
        let mut inner = self.inner();
        loop {
            match inner.prepare_next_command::<false>(&self.holder) {
                Some(Some(command)) => return Some(command),
                Some(None) => continue,
                None => return None,
            }
        }
    }
}

/// Marks a derived format that should be accessible and kept
/// automatically up-to-date when the parent object changes.
pub struct Derived<F: Format> {
    /// The inner implementation used to allocate and update derived objects of this format.
    inner: Arc<dyn DerivedFormatUpdater>,
    /// A marker for generic bounds.
    marker: PhantomData<fn() -> F>,
}

impl<F: Format> Derived<F> {
    /// Specifies how a derived object should be created to track an object of type `F`.
    pub fn new<D: DerivedDescriptor<F>>(descriptor: D) -> Self {
        Self {
            inner: Arc::new(TypedDerivedFormatUpdater {
                descriptor,
                marker: PhantomData,
            }),
            marker: PhantomData,
        }
    }
}

impl<F: Format> Clone for Derived<F> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            marker: PhantomData,
        }
    }
}

impl<F: Format> std::fmt::Debug for Derived<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Derived").field(&type_name::<F>()).finish()
    }
}

/// A handle created by calling [`CommandBuffer::map`] which allows
/// for synchronously accessing the data of a [`View`].
pub struct Mapped<M: Mutability, F: Format> {
    /// The inner state used to track the mapping.
    inner: Arc<MappedInner>,
    /// The view associating with this mapping.
    view: View<F>,
    /// Marker data.
    marker: PhantomData<fn() -> M>,
}

/// References a specified format underlying a [`Data`] instance.
pub struct View<F: Format> {
    /// The inner representation of this object.
    inner: Data<F::Kind>,
    /// The index to which this format refers.
    id: u32,
    /// Whether this is a derived format.
    derived: bool,
    /// A marker for generic bounds.
    marker: PhantomData<fn() -> F>,
}

impl<F: Format> View<F> {
    /// Marks this view as being used immutably from a command.
    pub fn as_const(&self) -> ViewDescriptor<Const, F> {
        ViewDescriptor {
            view: self,
            descriptor: SyncUnsafeCell::new(Some(())),
            taken: AtomicBool::new(false),
        }
    }

    /// Marks this view as being used mutably from a command, with the given usage.
    pub fn as_mut(&self, usage: <F::Kind as Kind>::UsageDescriptor) -> ViewDescriptor<Mut, F> {
        ViewDescriptor {
            view: self,
            descriptor: SyncUnsafeCell::new(Some(usage)),
            taken: AtomicBool::new(false),
        }
    }

    /// Gets the data to which this view refers.
    pub fn data(&self) -> &Data<F::Kind> {
        &self.inner
    }
}

impl<F: Format> Clone for View<F> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            id: self.id,
            derived: self.derived,
            marker: PhantomData,
        }
    }
}

impl<F: Format> std::fmt::Debug for View<F>
where
    <F::Kind as Kind>::FormatDescriptor: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("View")
            .field(&type_name::<F>())
            .field(&self.inner)
            .finish()
    }
}

impl<F: Format> Deref for View<F> {
    type Target = <F::Kind as Kind>::FormatDescriptor;

    fn deref(&self) -> &Self::Target {
        &self.inner.inner.descriptor
    }
}

impl<F: Format> PartialEq for View<F> {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<F: Format> Eq for View<F> {}

/// Declares how a command will use a view. Created by calling [`View::as_const`]
/// or [`View::as_mut`].
pub struct ViewDescriptor<'a, M: UsageMutability, F: Format> {
    /// The underlying view.
    view: &'a View<F>,
    /// The usage for the view.
    descriptor: SyncUnsafeCell<Option<M::Descriptor<F>>>,
    /// Whether the usage has been moved into the command buffer yet.
    taken: AtomicBool,
}

impl<'a, M: UsageMutability, F: Format> ViewUsage for ViewDescriptor<'a, M, F> {}

impl<'a, M: UsageMutability, F: Format> ViewUsageInner for ViewDescriptor<'a, M, F> {
    fn add_to_list(&self, command_list: &mut DynVec) -> DynEntry<dyn ViewHolder> {
        unsafe {
            assert!(
                !self.taken.swap(true, Ordering::Relaxed),
                "Attempted to reuse view descriptor{}",
                FormattedLabel(" ", self.view.inner.inner.label, "")
            );
            command_list.push(TypedViewHolder::<M, F> {
                view: self.view.clone(),
                descriptor: take(&mut *self.descriptor.get()).unwrap_unchecked(),
            })
        }
    }
}

/// A guard which allows access to the data of a format.
pub struct ViewRef<'a, M: Mutability, F: Format> {
    /// The inner reference to the data.
    reference: RwCellGuard<'a, M, *mut ()>,
    /// Marker data.
    marker: PhantomData<&'a F>,
}

impl<'a, M: Mutability, F: Format + std::fmt::Debug> std::fmt::Debug for ViewRef<'a, M, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (**self).fmt(f)
    }
}

impl<'a, M: Mutability, F: Format> Deref for ViewRef<'a, M, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.reference.cast_const().cast() }
    }
}

impl<'a, F: Format> DerefMut for ViewRef<'a, Mut, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.reference.cast() }
    }
}

/// Implements the ability to create and update a derived resource.
trait DerivedFormatUpdater: 'static + Send + Sync {
    /// Allocates an instance of the derived format.
    unsafe fn allocate(&self, descriptor: *const ()) -> Box<UnsafeCell<dyn Any + Send + Sync>>;

    /// The type ID of the derived format.
    fn format_type_id(&self) -> TypeId;

    /// Updates the derived format for the given usages.
    ///
    /// # Safety
    ///
    /// This function must be called with a context that contains an immutable
    /// view (the parent) and a mutable view (the derived) object.
    /// The set of usages must match the usage descriptor type shared
    /// by both the parent and child.
    unsafe fn update(&self, context: CommandContext, usages: *const [*const ()]);
}

/// A user-specified command that may be executed.
trait ExecutableCommand: 'static + Send + Sync {
    /// Executes this command.
    ///
    /// # Safety
    ///
    /// This function must only be called once.
    unsafe fn execute(&self, ctx: CommandContext);
}

/// Determines how a command will view an object.
trait ViewHolder: 'static + Send + Sync {
    /// The ID of the context associated with this view.
    fn context_id(&self) -> u64;

    /// The ID of the object refernced by this view.
    fn id(&self) -> u32;

    /// Whether this is a mutable view.
    fn mutable(&self) -> bool;

    /// Gets a pointer to the usage of this view, if any.
    fn usage(&self) -> *const ();
}

impl ExecutableCommand for () {
    unsafe fn execute(&self, _: CommandContext) {}
}

impl<F: 'static + Send + Sync + FnOnce(CommandContext)> ExecutableCommand
    for SyncUnsafeCell<Option<F>>
{
    unsafe fn execute(&self, ctx: CommandContext) {
        take(&mut *self.get()).unwrap_unchecked()(ctx);
    }
}

/// Holds a command buffer which describes work to perform on objects.
struct ActiveCommandBuffer {
    /// A reference to the list of commands used by this buffer.
    command_list: Arc<DynVec>,
    /// An optional label for the command buffer.
    label: Option<&'static str>,
    /// The set of commands that are left before this command buffer may be discarded.
    remaining_commands: u32,
    /// A unique ID identifying this buffer instance.
    unique_id: u64,
}

/// Holds the inner state for a command context.
struct CommandContextInner {
    /// The ID of the command that is currently executing.
    pub command_id: NodeId,
    /// A reference to the context.
    pub context: Arc<ContextHolder>,
    /// An optional label describing the command.
    pub label: Option<&'static str>,
    /// An optional label describing the command buffer.
    pub command_buffer_label: Option<&'static str>,
    /// The list of views that this context references.
    pub views: Vec<CommandContextView>,
}

/// Stores information about a view accessible from a command context.
struct CommandContextView {
    /// The ID of the view.
    pub id: u32,
    /// Whether the view is mutable.
    pub mutable: bool,
    /// A cell containing a pointer to the view.
    pub value: RwCell<*mut ()>,
}

/// Describes a single command within a command list.
struct CommandEntry {
    /// The computation to complete.
    pub computation: SyncUnsafeCell<Option<Computation>>,
    /// The first view entry in the views linked list.
    pub first_view_entry: Option<DynEntry<ViewEntry>>,
    /// The label of this command.
    pub label: Option<&'static str>,
    /// The next command entry in the command list.
    pub next_instance: Option<DynEntry<CommandEntry>>,
}

/// Describes an operation which corresponds to a node
/// in the computation graph.
#[derive(Clone)]
enum Computation {
    /// A command must be executed against object data.
    Execute {
        /// The command to execute.
        command: DynEntry<dyn ExecutableCommand>,
    },
    /// An object should be mapped and made available synchronously.
    Map {
        /// The inner context to map.
        inner: Option<Arc<MappedInner>>,
    },
    /// A derived object's data must be updated.
    Update {
        /// The object to update.
        object: u32,
        /// The list of updates that this object should reference.
        updates: Vec<(Arc<DynVec>, DynEntry<dyn ViewHolder>)>,
    },
}

/// Describes an operation within the computation graph.
struct ComputationNode {
    /// The computation to complete.
    pub computation: Computation,
    /// The command buffer containing the computation.
    pub command_buffer: u16,
    /// Information about the update which will take place, if
    /// this node is updating derived data.
    pub derived_update: Option<DerivedFormatUpdate>,
    /// A label describing this computation.
    pub label: Option<&'static str>,
    /// The set of views used by this computation.
    pub views: Vec<ComputationViewReference>,
}

/// Describes a view used by a computation.
struct ComputationViewReference {
    /// The ID of the view object.
    pub id: u32,
    /// Whether this is a mutable view.
    pub mutable: bool,
    /// The holder used to access the view.
    pub view_holder: DynEntry<dyn ViewHolder>,
}

/// Holds the inner data for a context.
struct ContextHolder {
    /// The notifier to use when new work is available from the context.
    change_notifier: ChangeNotifier,
    /// The listener to use when waiting for changes from the context.
    change_listener: Condvar,
    /// The ID of the context.
    context_id: u64,
    /// The inner mutable context data.
    inner: Mutex<ContextInner>,
}

/// Manages a set of objects and efficiently schedules
/// computations on them.
struct ContextInner {
    /// The list of command buffers that are referenced by commands.
    active_command_buffers: Slab<ActiveCommandBuffer>,
    /// The compute graph containing operation nodes.
    compute_graph: DirectedAcyclicGraph<ComputationNode>,
    /// The ID of the context.
    context_id: u64,
    /// The set of nodes that must complete for mappings to be available.
    critical_nodes: DirectedAcyclicGraphFlags,
    /// The set of nodes that are both critical and immediately schedulable.
    critical_top_level_nodes: DirectedAcyclicGraphFlags,
    /// The set of objects within the context.
    objects: Slab<DataHolder>,
    /// A sender used to alert the context of object updates.
    object_update_sender: Sender<ObjectUpdate>,
    /// Receives updates about the objects within the context.
    object_updates: std::sync::mpsc::Receiver<ObjectUpdate>,
    /// Whether an update should be sent out on the notifier when new work is available.
    stalled: bool,
    /// A buffer used to temporarily store node IDs without reallocation.
    temporary_node_buffer: Vec<NodeId>,
    /// The set of nodes the are immediately schedulable.
    top_level_nodes: DirectedAcyclicGraphFlags,
}

impl ContextInner {
    /// Creates a new object from the provided descriptor. Objects are also created for each derived format,
    /// and will automatically update whenever this object is changed.
    pub fn allocate<F: Format>(&mut self, descriptor: AllocationDescriptor<F>) -> Data<F::Kind> {
        unsafe {
            let mut derived_formats = Vec::with_capacity(descriptor.derived_formats.len());
            let mut derived_states: Vec<DerivedFormatState> =
                Vec::with_capacity(descriptor.derived_formats.len());

            let object = F::allocate(&descriptor.descriptor);
            let id = self.objects.insert(DataHolder {
                immutable_references: Vec::new(),
                mutable_references: Vec::new(),
                label: descriptor.label,
                derive_state: FormatDeriveState::Base {
                    derived_formats: Vec::with_capacity(descriptor.derived_formats.len()),
                },
                value: Box::pin(UnsafeCell::new(object)),
            }) as u32;

            for (index, derived) in descriptor.derived_formats.iter().enumerate() {
                let derived_object = derived
                    .inner
                    .allocate(&descriptor.descriptor as *const _ as *const _);
                let id = self.objects.insert(DataHolder {
                    immutable_references: Vec::new(),
                    mutable_references: Vec::new(),
                    label: descriptor.label,
                    derive_state: FormatDeriveState::Derived {
                        index: index as u8,
                        parent: id,
                        updater: derived.inner.clone(),
                    },
                    value: Box::into_pin(derived_object),
                }) as u32;
                assert!(
                    derived.inner.format_type_id() != TypeId::of::<F>(),
                    "Derived format cannot be the same type as parent."
                );
                assert!(
                    derived_states
                        .iter()
                        .all(|x| x.format_id != derived.inner.format_type_id()),
                    "Duplicate derived formats."
                );

                let format_id = derived.inner.format_type_id();
                derived_formats.push((format_id, id));
                derived_states.push(DerivedFormatState {
                    format_id,
                    id,
                    next_update: None,
                });
            }

            if let FormatDeriveState::Base { derived_formats } =
                &mut self.objects.get_unchecked_mut(id as usize).derive_state
            {
                *derived_formats = derived_states;
            } else {
                unreachable_unchecked()
            }

            let inner = Arc::new(DataInner {
                context_id: self.context_id,
                derived_formats,
                descriptor: descriptor.descriptor,
                format_id: TypeId::of::<F>(),
                id,
                label: descriptor.label,
                object_updater: self.object_update_sender.clone(),
            });

            Data { inner }
        }
    }

    /// Schedules the provided command buffer for execution.
    pub fn submit(
        &mut self,
        buffer: CommandBuffer,
        context: &ContextHolder,
    ) -> CommandBufferSubmission {
        self.update_objects();
        let (submission, added_top_level_node) = self.submit_buffer(buffer);
        self.critical_top_level_nodes
            .and(&self.critical_nodes, &self.top_level_nodes);
        if added_top_level_node {
            self.notify_new_top_level_commands(context);
        }
        submission
    }

    /// Creates a command context to execute the provided computation.
    ///
    /// # Safety
    ///
    /// The command ID must refer to a vald node in the compute graph.
    fn create_command_context(
        &self,
        context: &Arc<ContextHolder>,
        command_id: NodeId,
    ) -> CommandContext {
        unsafe {
            let computation = self.compute_graph.get_unchecked(command_id);
            CommandContext {
                inner: ManuallyDrop::new(CommandContextInner {
                    command_id,
                    command_buffer_label: self
                        .active_command_buffers
                        .get_unchecked(computation.command_buffer as usize)
                        .label,
                    context: context.clone(),
                    label: computation.label,
                    views: computation
                        .views
                        .iter()
                        .map(|x| CommandContextView {
                            id: x.id,
                            mutable: x.mutable,
                            value: RwCell::new(
                                self.objects.get_unchecked(x.id as usize).value.get().cast(),
                            ),
                        })
                        .collect(),
                }),
            }
        }
    }

    /// Attempts to get the next command to execute. Returns `None` if no work is presently available,
    /// `Some(None)` if new mappings were made available, and `Some(Some(_))` if work must be done.
    fn prepare_next_command<const CRITICAL_ONLY: bool>(
        &mut self,
        context: &Arc<ContextHolder>,
    ) -> Option<Option<Box<dyn WorkUnit>>> {
        unsafe {
            if let Some(node) = self.pop_command::<CRITICAL_ONLY>() {
                let ctx = self.create_command_context(context, node);
                let computation = self.compute_graph.get_unchecked_mut(node);

                match &mut computation.computation {
                    Computation::Execute { command } => {
                        let command = *command;
                        let command_buffer = self
                            .active_command_buffers
                            .get_unchecked(computation.command_buffer as usize)
                            .command_list
                            .clone();
                        Some(Some(Box::new(move || {
                            command_buffer.get_unchecked(command).execute(ctx)
                        })))
                    }
                    Computation::Map { inner } => {
                        let value = take(inner).unwrap_unchecked();
                        *value.command_context.get() = MaybeUninit::new(ctx);
                        value.map_state.set_complete();

                        // If command context must immediately be dropped,
                        // then drop it without reacquiring mutex.
                        if let Some(mut value) = Arc::into_inner(value) {
                            self.complete_command(node, context);
                            let command_context = value.command_context.get_mut().assume_init_mut();
                            ManuallyDrop::drop(&mut command_context.inner);
                            forget(value);
                        }

                        Some(None)
                    }
                    Computation::Update { object, updates } => {
                        let derived = *object;
                        let value = self.objects.get_unchecked(derived as usize);
                        let FormatDeriveState::Derived {
                            updater,
                            parent,
                            index,
                            ..
                        } = &value.derive_state
                        else {
                            unreachable_unchecked()
                        };

                        let parent = *parent as usize;
                        let index = *index as usize;
                        let updater = updater.clone();

                        let format_state = if let FormatDeriveState::Base { derived_formats } =
                            &mut self.objects.get_unchecked_mut(parent).derive_state
                        {
                            derived_formats.get_unchecked_mut(index)
                        } else {
                            unreachable_unchecked()
                        };

                        if format_state.next_update == Some(node) {
                            format_state.next_update = None;
                        }

                        let updates = take(updates);
                        Some(Some(Box::new(move || {
                            let mut update_list = Vec::with_capacity(updates.len());
                            update_list.extend(
                                updates
                                    .iter()
                                    .map(|(buffer, entry)| buffer.get_unchecked(*entry).usage()),
                            );
                            updater.update(ctx, &update_list[..] as *const _);
                        })))
                    }
                }
            } else {
                if !self.compute_graph.is_empty() {
                    self.stalled = true;
                }

                None
            }
        }
    }

    /// Determines the next command to schedule, removes it from the graph,
    /// and returns it. Prioritizes critical nodes.
    fn pop_command<const CRITICAL_ONLY: bool>(&mut self) -> Option<NodeId> {
        unsafe {
            if let Some(node) = self.critical_top_level_nodes.first_set_node() {
                debug_assert!(
                    self.compute_graph.parents(node).next().is_none(),
                    "Attempted to pop non-parent node."
                );
                self.critical_top_level_nodes.set_unchecked(node, false);
                self.top_level_nodes.set_unchecked(node, false);
                Some(node)
            } else if let Some(node) = (!CRITICAL_ONLY)
                .then(|| self.top_level_nodes.first_set_node())
                .flatten()
            {
                debug_assert!(
                    self.compute_graph.parents(node).next().is_none(),
                    "Attempted to pop non-parent node."
                );
                self.top_level_nodes.set_unchecked(node, false);
                Some(node)
            } else {
                None
            }
        }
    }

    /// Schedules all commands in the provided buffer for processing.
    fn submit_buffer(&mut self, buffer: CommandBuffer) -> (CommandBufferSubmission, bool) {
        unsafe {
            let mut added_top_level_node = false;
            let unique_id = unique_id();

            let command_buffer_id = if let Some(first_entry) = buffer.first_command_entry {
                let command_buffer_id = self.active_command_buffers.insert(ActiveCommandBuffer {
                    command_list: Arc::new(buffer.command_list),
                    label: buffer.label,
                    remaining_commands: 0,
                    unique_id,
                }) as u16;

                let mut command_entry = Some(first_entry);
                while let Some(entry) = command_entry {
                    let next = self
                        .active_command_buffers
                        .get_unchecked(command_buffer_id as usize)
                        .command_list
                        .get_unchecked(entry);
                    command_entry = next.next_instance;
                    added_top_level_node |= self.schedule_command(
                        command_buffer_id,
                        buffer.label,
                        take(&mut *next.computation.get()).unwrap_unchecked(),
                        next.label,
                        next.first_view_entry,
                    );
                }

                command_buffer_id
            } else {
                0
            };

            let submission = CommandBufferSubmission {
                command_buffer_id,
                context_id: self.context_id,
                unique_id,
            };

            (submission, added_top_level_node)
        }
    }

    /// Schedules a command to execute.
    fn schedule_command(
        &mut self,
        command_buffer_id: u16,
        command_buffer_label: Option<&'static str>,
        computation: Computation,
        label: Option<&'static str>,
        first_view_entry: Option<DynEntry<ViewEntry>>,
    ) -> bool {
        unsafe {
            let command_buffer = self
                .active_command_buffers
                .get_unchecked(command_buffer_id as usize)
                .command_list
                .clone();
            let node = self.compute_graph.vacant_node();

            // Iterate over all used views to find any conflicting computations
            self.temporary_node_buffer.clear();
            let mut views = Vec::new();
            let mut view_entry = first_view_entry;

            while let Some(entry) = view_entry {
                let next = command_buffer.get_unchecked(entry);
                let next_view = command_buffer.get_unchecked(next.view);
                assert!(
                    next_view.context_id() == self.context_id,
                    "View did not belong to this context."
                );
                views.push(ComputationViewReference {
                    id: next_view.id(),
                    view_holder: next.view,
                    mutable: next_view.mutable(),
                });

                view_entry = next.next_instance;
                let object = self.objects.get_unchecked_mut(next_view.id() as usize);

                // Determine which other commands are dependencies of this one
                for computation in object.mutable_references.iter().copied() {
                    assert!(!next_view.mutable() || computation != node,
                        "Attempted to use two conflicting views of object{} with command{} in buffer{}",
                        FormattedLabel(" ", object.label, ""),
                        FormattedLabel(" ", label, ""),
                        FormattedLabel(" ", command_buffer_label, ""));
                    self.temporary_node_buffer.push(computation);
                }

                if next_view.mutable() {
                    for computation in object.immutable_references.iter().copied() {
                        assert!(computation != node,
                            "Attempted to use two conflicting views of object{} with command{} in buffer{}",
                            FormattedLabel(" ", object.label, ""),
                            FormattedLabel(" ", label, ""),
                            FormattedLabel(" ", command_buffer_label, ""));

                        if let Some(derived) =
                            &self.compute_graph.get_unchecked(computation).derived_update
                        {
                            if derived.parent == next_view.id() {
                                let FormatDeriveState::Base { derived_formats } =
                                    &mut object.derive_state
                                else {
                                    unreachable_unchecked()
                                };

                                if derived_formats
                                    .get_unchecked(derived.index as usize)
                                    .next_update
                                    == Some(computation)
                                {
                                    continue;
                                }
                            }
                        }

                        self.temporary_node_buffer.push(computation);
                    }
                }

                // Add view to object
                if next_view.mutable() {
                    &mut object.mutable_references
                } else {
                    &mut object.immutable_references
                }
                .push(node);
            }

            // Add the new computation to the graph
            self.compute_graph.insert_unchecked(
                ComputationNode {
                    computation: computation.clone(),
                    command_buffer: command_buffer_id,
                    derived_update: None,
                    label,
                    views,
                },
                &self.temporary_node_buffer,
            );

            // Update any derived views that this command affects
            for i in 0..self.compute_graph.get_unchecked(node).views.len() {
                let view = &self
                    .compute_graph
                    .get_unchecked(node)
                    .views
                    .get_unchecked(i);
                let view_id = view.id;
                let view_holder = view.view_holder;
                let mutable = view.mutable;
                let object = self.objects.get_unchecked_mut(view.id as usize);
                let mut derived_nodes_to_add = Vec::new();
                match &mut object.derive_state {
                    FormatDeriveState::Base { derived_formats } => {
                        if mutable {
                            for (index, format) in derived_formats.iter_mut().enumerate() {
                                let derived = if let Some(derived) = format.next_update {
                                    self.compute_graph.add_parent_unchecked(node, derived);
                                    self.top_level_nodes.set_unchecked(derived, false);
                                    derived
                                } else {
                                    let derived_computation = self.compute_graph.insert_unchecked(
                                        ComputationNode {
                                            computation: Computation::Update {
                                                object: format.id,
                                                updates: Vec::with_capacity(1),
                                            },
                                            command_buffer: command_buffer_id,
                                            derived_update: Some(DerivedFormatUpdate {
                                                parent: view_id,
                                                index: index as u32,
                                            }),
                                            label: None,
                                            views: vec![
                                                ComputationViewReference {
                                                    id: view_id,
                                                    view_holder,
                                                    mutable: false,
                                                },
                                                ComputationViewReference {
                                                    id: format.id,
                                                    view_holder,
                                                    mutable: true,
                                                },
                                            ],
                                        },
                                        &[node],
                                    );
                                    format.next_update = Some(derived_computation);
                                    object.immutable_references.push(derived_computation);
                                    derived_nodes_to_add.push((format.id, derived_computation));
                                    self.active_command_buffers
                                        .get_unchecked_mut(command_buffer_id as usize)
                                        .remaining_commands += 1;
                                    derived_computation
                                };

                                let Computation::Update { updates, .. } =
                                    &mut self.compute_graph.get_unchecked_mut(derived).computation
                                else {
                                    unreachable_unchecked()
                                };

                                updates.push((command_buffer.clone(), view_holder));
                            }
                        }
                    }
                    &mut FormatDeriveState::Derived { parent, index, .. } => {
                        if let FormatDeriveState::Base { derived_formats } =
                            &mut self.objects.get_unchecked_mut(parent as usize).derive_state
                        {
                            derived_formats
                                .get_unchecked_mut(index as usize)
                                .next_update = None;
                        } else {
                            unreachable_unchecked()
                        }
                    }
                }

                for (id, node) in derived_nodes_to_add {
                    self.objects
                        .get_unchecked_mut(id as usize)
                        .mutable_references
                        .push(node);
                }
            }

            self.top_level_nodes.resize_for(&self.compute_graph);
            self.critical_nodes.resize_for(&self.compute_graph);

            let top_level = if self.temporary_node_buffer.is_empty() {
                self.top_level_nodes.set_unchecked(node, true);
                true
            } else {
                false
            };

            if let Computation::Map { inner } = computation {
                let inner_ref = inner.as_ref().unwrap_unchecked();
                assert!(
                    inner_ref.context_id == self.context_id,
                    "Attempted to map object in incorrect context."
                );
                inner_ref.map_state.set_queued(node);
                self.mark_critical(node);
            }

            self.active_command_buffers
                .get_unchecked_mut(command_buffer_id as usize)
                .remaining_commands += 1;

            top_level
        }
    }

    /// Marks the provided node, and all of its parents, as critical.
    ///
    /// # Safety
    ///
    /// For this function call to be sound, node must refer to a valid
    /// node in the computation graph.
    unsafe fn mark_critical(&mut self, node: NodeId) {
        self.critical_nodes.set_unchecked(node, true);
        while let Some(parent) = self.temporary_node_buffer.pop() {
            if !self.critical_nodes.get_unchecked(parent) {
                self.temporary_node_buffer
                    .extend(self.compute_graph.parents(parent));
                self.critical_nodes.set_unchecked(parent, true);
            }
        }
    }

    /// Marks a command as complete and removes it from the node graph.
    unsafe fn complete_command(&mut self, id: NodeId, context: &ContextHolder) {
        self.temporary_node_buffer.clear();
        self.temporary_node_buffer
            .extend(self.compute_graph.children_unchecked(id));
        self.critical_nodes.set_unchecked(id, false);
        let computation = self.compute_graph.pop_unchecked(id);

        for child in self.temporary_node_buffer.iter().copied() {
            if self.compute_graph.parents_unchecked(child).next().is_none() {
                self.top_level_nodes.set_unchecked(child, true);
            }
        }

        for view in computation.views {
            let object = self.objects.get_unchecked_mut(view.id as usize);
            let view_vec = if view.mutable {
                &mut object.mutable_references
            } else {
                &mut object.immutable_references
            };
            view_vec.swap_remove(view_vec.iter().position(|x| *x == id).unwrap_unchecked());
        }

        self.critical_top_level_nodes
            .and(&self.top_level_nodes, &self.critical_nodes);

        let command_list = self
            .active_command_buffers
            .get_unchecked_mut(computation.command_buffer as usize);
        command_list.remaining_commands -= 1;
        if command_list.remaining_commands == 0 {
            self.active_command_buffers
                .remove(computation.command_buffer as usize);
        }

        if !self.temporary_node_buffer.is_empty() {
            self.notify_new_top_level_commands(context);
        }
    }

    /// Updates the objects of this context, discarding any which have been dropped.
    fn update_objects(&mut self) {
        unsafe {
            while let Ok(update) = self.object_updates.try_recv() {
                match update {
                    ObjectUpdate::DropData { id } => {
                        let FormatDeriveState::Base { derived_formats } =
                            self.objects.remove(id as usize).derive_state
                        else {
                            unreachable_unchecked()
                        };

                        for format in derived_formats {
                            self.objects.remove(format.id as usize);
                        }
                    }
                }
            }
        }
    }

    /// Notifies all waiting threads that new commands are available for processing.
    fn notify_new_top_level_commands(&mut self, context: &ContextHolder) {
        if self.stalled {
            self.stalled = false;
            context.change_notifier.notify();
        }

        if self.critical_top_level_nodes.first_set_node().is_some() {
            context.change_listener.notify_all();
        }
    }
}

/// Holds an object within a context.
struct DataHolder {
    /// Information about the base or derived state of this object.
    pub derive_state: FormatDeriveState,
    /// An optional label describing this object.
    pub label: Option<&'static str>,
    /// A list of computations which use this object immutably.
    pub immutable_references: Vec<NodeId>,
    /// A list of computations which use this object mutably.
    pub mutable_references: Vec<NodeId>,
    /// A refernce to the inner object.
    pub value: Pin<Box<UnsafeCell<dyn Any + Send + Sync>>>,
}

/// Holds the inner information for an allocated object.
struct DataInner<K: Kind> {
    /// The ID of the context associated with this data.
    pub context_id: u64,
    /// A list of any derived formats for this data.
    pub derived_formats: Vec<(TypeId, u32)>,
    /// The descriptor of this data.
    pub descriptor: K::FormatDescriptor,
    /// The ID of the format associated with this data.
    pub format_id: TypeId,
    /// The ID of this object.
    pub id: u32,
    /// An optional label describing this object.
    pub label: Option<&'static str>,
    /// The updater to notify when this data is dropped.
    pub object_updater: Sender<ObjectUpdate>,
}

impl<K: Kind> Drop for DataInner<K> {
    fn drop(&mut self) {
        let _ = self
            .object_updater
            .send(ObjectUpdate::DropData { id: self.id });
    }
}

/// Provides information about how a format update should take place.
struct DerivedFormatUpdate {
    /// The parent of this format.
    pub parent: u32,
    /// The index of this format within the parent's derived list.
    pub index: u32,
}

/// Stores information about the state of a base's derived format.
struct DerivedFormatState {
    /// The type ID of this format.
    pub format_id: TypeId,
    /// The ID of this format.
    pub id: u32,
    /// The next update that is scheduled for this derived format.
    pub next_update: Option<NodeId>,
}

/// Describes whether an object is a base object
/// or derived data.
enum FormatDeriveState {
    /// The object is a mutable base object.
    Base {
        /// The set of objects derived from this one.
        derived_formats: Vec<DerivedFormatState>,
    },
    /// The object derives its data from another.
    Derived {
        /// The ID of the parent object.
        parent: u32,
        /// The index of the object in the parent's derived array.
        index: u8,
        /// The object to use when updating this format.
        updater: Arc<dyn DerivedFormatUpdater>,
    },
}

/// Prints a formatted object label with a prefix and suffix,
/// or prints nothing if the object did not exist.
struct FormattedLabel(pub &'static str, pub Option<&'static str>, pub &'static str);

impl std::fmt::Display for FormattedLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(label) = &self.1 {
            f.write_fmt(format_args!("{}'{}'{}", self.0, label, self.2))
        } else {
            Ok(())
        }
    }
}

/// The state associated with mapped data.
struct MappedInner {
    /// The ID of the context associated with the mapping.
    pub context_id: u64,
    /// The command context associated with the mapping.
    pub command_context: UnsafeCell<MaybeUninit<CommandContext>>,
    /// The label of the object to be mapped.
    pub label: Option<&'static str>,
    /// Whether this object has been submitted or mapped yet.
    pub map_state: MapObjectState,
}

impl Drop for MappedInner {
    fn drop(&mut self) {
        unsafe {
            self.command_context.get_mut().assume_init_drop();
        }
    }
}

unsafe impl Send for MappedInner {}
unsafe impl Sync for MappedInner {}

/// Provides the results of a query against a `MapObjectState`.
struct MapObjectStateQuery {
    /// Indicates that the mapping is ready to be accessed.
    pub complete: bool,
    /// The node ID associated with the mapping, assuming that it has been queued.
    pub node: NodeId,
    /// Indicates that the mapping has been submitted for processing.
    pub queued: bool,
}

/// Tracks whether an object is presently mapped.
#[derive(Default)]
struct MapObjectState(AtomicU32);

impl MapObjectState {
    /// A flag bit indicating that the mapping has been queued for execution.
    const MAPPING_QUEUED: u32 = 1 << 16;
    /// A flag bit indicating that the mapping is completed.
    const MAPPING_COMPLETE: u32 = 1 << 17;

    /// Moves this mapping to the queued state, and sets the node ID.
    pub fn set_queued(&self, node: NodeId) {
        self.0.store(
            Self::MAPPING_QUEUED | (u16::from(node) as u32),
            Ordering::Release,
        );
    }

    /// Marks this mapping as complete.
    pub fn set_complete(&self) {
        self.0.fetch_or(Self::MAPPING_COMPLETE, Ordering::Release);
    }

    /// Gets the current state of this mapping.
    pub fn get(&self) -> MapObjectStateQuery {
        let value = self.0.load(Ordering::Acquire);
        let complete = (value & Self::MAPPING_COMPLETE) != 0;
        let queued = (value & Self::MAPPING_QUEUED) != 0;
        let node = (value as u16).into();

        MapObjectStateQuery {
            complete,
            node,
            queued,
        }
    }
}

/// Notifies the context of an update that has occurred to an object.
enum ObjectUpdate {
    /// An object has been dropped.
    DropData {
        /// The ID of the object which has been dropped.
        id: u32,
    },
}

/// A node in a linked list of views used by a command.
struct ViewEntry {
    /// The next view in the linked list, if any.
    pub next_instance: Option<DynEntry<ViewEntry>>,
    /// The view itself.
    pub view: DynEntry<dyn ViewHolder>,
}

/// Implements the ability to update a derived format.
struct TypedDerivedFormatUpdater<F: Format, D: DerivedDescriptor<F>> {
    /// The descriptor determining how to update the format.
    pub descriptor: D,
    /// Marker data.
    pub marker: PhantomData<fn() -> (F, D)>,
}

impl<F: Format, D: DerivedDescriptor<F>> DerivedFormatUpdater for TypedDerivedFormatUpdater<F, D> {
    unsafe fn allocate(&self, descriptor: *const ()) -> Box<UnsafeCell<dyn Any + Send + Sync>> {
        Box::new(UnsafeCell::new(<D::Format as Format>::allocate(
            &*(descriptor as *const _),
        )))
    }

    fn format_type_id(&self) -> TypeId {
        TypeId::of::<D::Format>()
    }

    unsafe fn update(&self, context: CommandContext, usages: *const [*const ()]) {
        self.descriptor.update(
            &mut *context.inner.views.get_unchecked(1).value.borrow().cast(),
            &*context
                .inner
                .views
                .get_unchecked(0)
                .value
                .borrow()
                .cast_const()
                .cast(),
            &*transmute::<_, *const [_]>(usages),
        );
    }
}

/// Acts as a view holder for the provided view and descriptor.
struct TypedViewHolder<M: UsageMutability, F: Format> {
    /// The view that this holder references.
    view: View<F>,
    /// The descriptor determining how this view is being used.
    descriptor: M::Descriptor<F>,
}

impl<M: UsageMutability, F: Format> ViewHolder for TypedViewHolder<M, F> {
    fn context_id(&self) -> u64 {
        self.view.inner.inner.context_id
    }

    fn id(&self) -> u32 {
        self.view.id
    }

    fn mutable(&self) -> bool {
        TypeId::of::<M>() == TypeId::of::<Mut>()
    }

    fn usage(&self) -> *const () {
        &self.descriptor as *const _ as *const _
    }
}

/// Hides implementation details from other crates.
mod private {
    use super::*;

    /// Provides the ability to add a view to a command buffer.
    pub trait ViewUsageInner {
        /// Adds a view holder entry to the given command list.
        fn add_to_list(&self, command_list: &mut DynVec) -> DynEntry<dyn ViewHolder>;
    }

    /// Mutability which optionally stores a descriptor about a view's usage.
    pub trait UsageMutability: Mutability {
        /// The descriptor that must be provided for this mutability.
        type Descriptor<F: Format>: Send + Sync;
    }

    impl UsageMutability for Const {
        type Descriptor<F: Format> = ();
    }

    impl UsageMutability for Mut {
        type Descriptor<F: Format> = <F::Kind as Kind>::UsageDescriptor;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    pub struct DataDescriptor {
        pub initial_value: u32,
    }

    pub struct MyData;

    impl Kind for MyData {
        type FormatDescriptor = DataDescriptor;
        type UsageDescriptor = u32;
    }

    #[derive(Debug)]
    pub struct Primary(pub i32);

    impl Format for Primary {
        type Kind = MyData;

        fn allocate(descriptor: &<Self::Kind as Kind>::FormatDescriptor) -> Self {
            Self(descriptor.initial_value as i32)
        }
    }

    #[derive(Debug)]
    pub struct DerivedAccelerationStructure(pub i32);

    impl Format for DerivedAccelerationStructure {
        type Kind = MyData;

        fn allocate(descriptor: &<Self::Kind as Kind>::FormatDescriptor) -> Self {
            Self(2 * descriptor.initial_value as i32)
        }
    }

    pub struct UpdateAccelerationFromPrimary;

    impl DerivedDescriptor<Primary> for UpdateAccelerationFromPrimary {
        type Format = DerivedAccelerationStructure;

        fn update(&self, data: &mut Self::Format, parent: &Primary, _usage: &[&u32]) {
            // Do some calculation to update the acceleration structure based upon the
            // how the primary format has been modified.
            data.0 = 2 * parent.0;
        }
    }

    #[test]
    #[should_panic]
    fn test_panic_on_conflicting_usage() {
        let ctx = DataFrostContext::new(ContextDescriptor {
            label: Some("my context"),
        });
        let data = ctx.allocate::<Primary>(AllocationDescriptor {
            descriptor: DataDescriptor { initial_value: 23 },
            label: Some("my int"),
            derived_formats: &[],
        });

        let mut command_buffer = CommandBuffer::new(CommandBufferDescriptor {
            label: Some("my command buffer"),
        });
        let view = data.view::<Primary>();

        command_buffer.schedule(CommandDescriptor {
            label: Some("Test command"),
            views: &[&view.as_const(), &view.as_mut(25)],
            command: |_| {},
        });

        ctx.submit(command_buffer);
    }

    #[test]
    fn test_allow_multiple_const_usage() {
        let ctx = DataFrostContext::new(ContextDescriptor {
            label: Some("my context"),
        });
        let data = ctx.allocate::<Primary>(AllocationDescriptor {
            descriptor: DataDescriptor { initial_value: 23 },
            label: Some("my int"),
            derived_formats: &[],
        });

        let mut command_buffer = CommandBuffer::new(CommandBufferDescriptor {
            label: Some("my command buffer"),
        });
        let view_a = data.view::<Primary>();
        let view_b = data.view::<Primary>();

        command_buffer.schedule(CommandDescriptor {
            label: Some("Test command"),
            command: |_| {},
            views: &[&view_a.as_const(), &view_b.as_const()],
        });

        ctx.submit(command_buffer);
    }

    #[test]
    fn test_single_mappings() {
        let ctx = DataFrostContext::new(ContextDescriptor {
            label: Some("my context"),
        });
        let data = ctx.allocate::<Primary>(AllocationDescriptor {
            descriptor: DataDescriptor { initial_value: 23 },
            label: Some("my int"),
            derived_formats: &[Derived::new(UpdateAccelerationFromPrimary)],
        });

        let mut command_buffer = CommandBuffer::new(CommandBufferDescriptor {
            label: Some("my command buffer"),
        });
        let view = data.view::<Primary>();

        let view_clone = view.clone();
        command_buffer.schedule(CommandDescriptor {
            label: Some("Test command"),
            command: move |ctx| {
                let mut vc = ctx.get_mut(&view_clone);
                vc.0 += 4;
            },
            views: &[&view.as_mut(4)],
        });

        let mapping1 = command_buffer.map(&data.view::<DerivedAccelerationStructure>().as_const());

        let view_clone = view.clone();
        command_buffer.schedule(CommandDescriptor {
            label: Some("Test command"),
            command: move |ctx| {
                let mut vc = ctx.get_mut(&view_clone);
                vc.0 += 2;
            },
            views: &[&view.as_mut(2)],
        });

        let mapping2 = command_buffer.map(&data.view::<DerivedAccelerationStructure>().as_const());
        ctx.submit(command_buffer);

        let value = ctx.get(&mapping1);
        assert_eq!(value.0, 54);
        drop(value);
        drop(mapping1);
        let value = ctx.get(&mapping2);
        assert_eq!(value.0, 58);
        drop(value);
        drop(mapping2);
    }

    #[test]
    fn test_skip_irrelevant_command() {
        let execution_count = Arc::new(AtomicU32::new(0));
        let ctx = DataFrostContext::new(ContextDescriptor {
            label: Some("my context"),
        });
        let data = ctx.allocate::<Primary>(AllocationDescriptor {
            descriptor: DataDescriptor { initial_value: 23 },
            label: Some("my int"),
            derived_formats: &[Derived::new(UpdateAccelerationFromPrimary)],
        });

        let mut command_buffer = CommandBuffer::new(CommandBufferDescriptor {
            label: Some("my command buffer"),
        });
        let view = data.view::<Primary>();

        let ex_clone = execution_count.clone();
        let view_clone = view.clone();
        command_buffer.schedule(CommandDescriptor {
            label: Some("Test command"),
            command: move |ctx| {
                let mut vc = ctx.get_mut(&view_clone);
                vc.0 += 4;
                ex_clone.fetch_add(1, Ordering::Relaxed);
            },
            views: &[&view.as_mut(4)],
        });

        let ex_clone = execution_count.clone();
        command_buffer.schedule(CommandDescriptor {
            label: Some("Test command"),
            command: move |_| {
                ex_clone.fetch_add(1, Ordering::Relaxed);
            },
            views: &[&view.as_const()],
        });

        let mapping = command_buffer.map(&data.view::<DerivedAccelerationStructure>().as_const());
        ctx.submit(command_buffer);

        ctx.get(&mapping);
        assert_eq!(execution_count.load(Ordering::Relaxed), 1);
    }
}
