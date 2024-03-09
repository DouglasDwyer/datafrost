#![feature(coerce_unsized)]
#![feature(downcast_unchecked)]
#![feature(non_null_convenience)]
#![feature(ptr_metadata)]
#![feature(sync_unsafe_cell)]
#![feature(unsize)]

#![allow(private_interfaces)]
//#![deny(missing_docs)]

//! Crate docs

use crate::dyn_vec::*;
use crate::graph::*;
use crate::unique_id::*;
pub use mutability_marker::*;
use private::*;
use slab::*;
use std::any::*;
use std::cell::*;
use std::marker::*;
use std::mem::*;
use std::ops::*;
use std::pin::*;
use std::sync::*;
use std::sync::atomic::*;
use std::sync::mpsc::*;
use sync_rw_cell::*;
use task_pool::*;
#[allow(unused_imports)]
use wasm_sync::Mutex;

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
    fn update(&self, data: &mut Self::Format, parent: &F, usages: &[&<F::Kind as Kind>::UsageDescriptor]);
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
    pub derived_formats: &'a [&'a Derived<F>]
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
        Self {
            command_list: DynVec::new(),
            label: descriptor.label,
            first_command_entry: None,
            last_command_entry: None
        }
    }

    /// Maps a format for synchronous viewing.
    pub fn map<M: UsageMutability, F: Format>(&mut self, view: &ViewDescriptor<M, F>) -> Mapped<M, F> {
        let inner = Arc::new(MappedInner::default());
    
        let computation = SyncUnsafeCell::new(Some(Computation::Map { inner: Some(inner.clone()) }));

        let first_view_entry = self.push_views(&[view]);
        let next_command = self.command_list.push(CommandEntry {
            computation,
            first_view_entry,
            label: Some("Map format"),
            next_instance: None
        });

        self.update_first_last_command_entries(next_command);

        Mapped {
            inner,
            view: view.view.clone(),
            marker: PhantomData
        }
    }

    /// Schedules a command to execute on format data.
    pub fn schedule(&mut self, descriptor: CommandDescriptor<impl Send + Sync + FnOnce(CommandContext)>) {
        let computation = SyncUnsafeCell::new(Some(Computation::Execute { command: self.command_list.push(SyncUnsafeCell::new(Some(descriptor.command))) }));
        let first_view_entry = self.push_views(descriptor.views);
        let next_command = self.command_list.push(CommandEntry {
            computation,
            first_view_entry,
            label: descriptor.label,
            next_instance: None
        });
        self.update_first_last_command_entries(next_command);
    }

    /// Creates a linked list of views in the command list.
    fn push_views(&mut self, list: &[&dyn ViewUsage]) -> Option<DynEntry<ViewEntry>> {
        let mut view_iter = list.iter();
        if let Some(first) = view_iter.next() {
            let view = first.add_to_list(&mut self.command_list);
            let first_entry = self.command_list.push(ViewEntry {
                next_instance: None,
                view
            });
            let mut previous_entry = first_entry;

            for to_add in view_iter {
                let view = to_add.add_to_list(&mut self.command_list);
                let next_entry = self.command_list.push(ViewEntry {
                    next_instance: None,
                    view
                });
                
                self.command_list[previous_entry].next_instance = Some(next_entry);
                previous_entry = next_entry;
            }

            Some(first_entry)
        }
        else {
            None
        }
    }

    /// Updates the first and last command entries after the provided command
    /// has been added to the command list.
    fn update_first_last_command_entries(&mut self, next_command: DynEntry<CommandEntry>) {
        if self.first_command_entry.is_none() {
            self.first_command_entry = Some(next_command);
        }
        else if let Some(entry) = self.last_command_entry {
            self.command_list[entry].next_instance = Some(next_command);
        }

        self.last_command_entry = Some(next_command);
    }
}

/// Specifies the parameters of a new [`CommandBuffer`].
#[derive(Copy, Clone, Debug, Default)]
pub struct CommandBufferDescriptor {
    /// An optional label to associate with the object.
    pub label: Option<&'static str>
}

/// Allows for interacting with format data during command execution.
/// When this object is dropped, a command is considered complete.
pub struct CommandContext {
    command_id: NodeId,
    context: Arc<ContextHolder>,
    label: Option<&'static str>,
    command_buffer_label: Option<&'static str>,
    views: Vec<CommandContextView>
}

impl CommandContext {
    /// Immutably gets the data referenced by the view.
    pub fn get<F: Format>(&self, view: &View<F>) -> ViewRef<Const, F> {
        ViewRef {
            reference: self.find_view::<Const, _>(view).borrow(),
            marker: PhantomData
        }
    }

    /// Mutably gets the data referenced by the view, and records the usage for updating derived formats.
    /// This function will panic if `view` refers to a derived format.
    pub fn get_mut<F: Format>(&self, view: &View<F>) -> ViewRef<Mut, F> {
        ViewRef {
            reference: self.find_view::<Mut, _>(view).borrow_mut(),
            marker: PhantomData
        }
    }

    fn find_view<M: Mutability, F: Format>(&self, view: &View<F>) -> &RwCell<*mut ()> {
        let mutable = TypeId::of::<Mut>() == TypeId::of::<M>();
        if let Some(command_view) = self.views.iter().find(|x| x.id == view.id && x.mutable == mutable) {
            &command_view.value
        }
        else {
            panic!("View{} was not referenced by command{}{}",
                FormattedLabel(" ", view.inner.inner.label, ""),
                FormattedLabel(" ", self.label, ""),
                FormattedLabel(" (from command buffer ", self.command_buffer_label, ")"));
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
        self.context.inner.lock().expect("Failed to lock context.").complete_command(self.command_id, &self.context);
    }
}

/// Describes a command to execute.
pub struct CommandDescriptor<'a, C: 'static + Send + Sync + FnOnce(CommandContext)> {
    /// The label associated with this command.
    pub label: Option<&'static str>,
    /// The command to execute asynchronously.
    pub command: C,
    /// A list of views that the command will access via the [`CommandContext`].
    pub views: &'a [&'a dyn ViewUsage]
}

impl<'a, C: 'static + Send + Sync + FnOnce(CommandContext)> std::fmt::Debug for CommandDescriptor<'a, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CommandDescriptor").field(&self.label).finish()
    }
}

/// Determines how a [`DataFrostContext`] should behave.
#[derive(Copy, Clone, Debug, Default)]
pub struct ContextDescriptor {
    /// An optional label to associate with the object.
    pub label: Option<&'static str>
}

/// References an object of a specific [`Kind`]. The object is backed
/// by a single primary format and some number of derived formats.
pub struct Data<K: Kind> {
    /// The inner representation of this object.
    inner: Arc<DataInner<K>>
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
        }
        else if let Some((_, id)) = self.inner.derived_formats.iter().copied().find(|&(id, _)| id == TypeId::of::<F>()) {
            //assert!(TypeId::of::<M>() == TypeId::of::<Const>(), "Attempted to mutably access derived format {} of object{}", type_name::<F>(), FormattedLabel(" ", self.inner.label, ""));
            (id, true)
        }
        else {
            panic!("Derived format {} of object{} did not exist", type_name::<F>(), FormattedLabel(" ", self.inner.label, ""))
        };

        View {
            inner: self.clone(),
            id,
            derived,
            marker: PhantomData
        }
    }
}

impl<K: Kind> Clone for Data<K> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }
    }
}

impl<K: Kind> std::fmt::Debug for Data<K> where K::FormatDescriptor: std::fmt::Debug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(label) = self.inner.label {
            f.debug_tuple("Data").field(&label).field(&self.inner.descriptor).finish()
        }
        else {
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

/// Manages a collection of formatted data, coordinates the execution of commands
/// against the data, and automatically updates derived formats using the results
/// of those commands.
#[derive(Clone)]
pub struct DataFrostContext {
    /// The shared data that composes this context.
    holder: Arc<ContextHolder>
}

impl DataFrostContext {
    /// Allocates a new, empty context.
    pub fn new(_: ContextDescriptor) -> Self {
        let (object_update_sender, object_updates) = channel();

        let change_notifier = ChangeNotifier::default();
        let change_listener = CondvarNotificationListener::new(&change_notifier);
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
            stalled: false,
            temporary_node_buffer: Vec::new(),
            top_level_nodes: DirectedAcyclicGraphFlags::new()
        });

        let holder = Arc::new(ContextHolder {
            change_notifier,
            change_listener,
            context_id,
            inner
        });

        Self {
            holder
        }
    }

    /// Creates a new object from the provided descriptor. Objects are also created for each derived format,
    /// and will automatically update whenever this object is changed.
    pub fn allocate<F: Format>(&self, descriptor: AllocationDescriptor<F>) -> Data<F::Kind> {
        self.inner().allocate(descriptor)
    }

    /// Immutably gets the data referenced by the mapping. This function will block if the mapping is not yet available.
    pub fn get<'a, M: Mutability, F: Format>(&self, mapping: &'a Mapped<M, F>) -> ViewRef<'a, Const, F> {
        unsafe {
            assert!(mapping.inner.context_id.load(Ordering::Acquire) == self.holder.context_id, "Mapping was not from this context.");

            if !mapping.inner.mapped.load(Ordering::Acquire) {
                let mut inner = self.inner();
                while !mapping.inner.mapped.load(Ordering::Acquire) {
                    match inner.prepare_next_command(&self.holder) {
                        Some(Some(command)) => {
                            drop(inner);
                            command.execute();
                            inner = self.inner();
                        },
                        Some(None) => continue,
                        None => inner = self.holder.change_listener.wait(inner),
                    }
                }
            }
            
            return (*mapping.inner.command_context.get()).assume_init_ref().get(&mapping.view);
        }
    }

    /// Mutably gets the data referenced by the mapping. This function will block if the mapping is not yet available.
    pub fn get_mut<'a, F: Format>(&self, mapping: &'a mut Mapped<Mut, F>) -> ViewRef<'a, Mut, F> {
        unsafe {
            assert!(mapping.inner.context_id.load(Ordering::Acquire) == self.holder.context_id, "Mapping was not from this context.");

            if !mapping.inner.mapped.load(Ordering::Acquire) {
                let mut inner = self.inner();
                while !mapping.inner.mapped.load(Ordering::Acquire) {
                    match inner.prepare_next_command(&self.holder) {
                        Some(Some(command)) => {
                            drop(inner);
                            command.execute();
                            inner = self.inner();
                        },
                        Some(None) => continue,
                        None => inner = self.holder.change_listener.wait(inner),
                    }
                }
            }
            
            return (*mapping.inner.command_context.get()).assume_init_mut().get_mut(&mapping.view);
        }
    }

    /// Schedules the provided command buffer for execution.
    pub fn submit(&self, buffers: impl IntoIterator<Item = CommandBuffer>) {
        self.inner().submit(buffers, &self.holder);
    }

    /// Gets the inner data of this context.
    fn inner(&self) -> MutexGuard<ContextInner> {
        self.holder.inner.lock().expect("Failed to obtain inner context.")
    }
}

impl std::fmt::Debug for DataFrostContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DataFrostContext").finish()
    }
}

impl WorkProvider for DataFrostContext {
    fn change_notifier(&self) -> &ChangeNotifier {
        &self.holder.change_notifier
    }

    fn next_task(&self) -> Option<Box<dyn '_ + WorkUnit>> {
        let mut inner = self.inner();
        loop {
            match inner.prepare_next_command(&self.holder) {
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
    marker: PhantomData<fn() -> F>
}

impl<F: Format> Derived<F> {
    /// Specifies how a derived object should be created to track an object of type `F`.
    pub fn new<D: DerivedDescriptor<F>>(descriptor: D) -> Self {
        Self {
            inner: Arc::new(TypedDerivedFormatUpdater {
                descriptor,
                marker: PhantomData
            }),
            marker: PhantomData
        }
    }
}

impl<F: Format> Clone for Derived<F> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            marker: PhantomData
        }
    }
}

impl<F: Format> std::fmt::Debug for Derived<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Derived").field(&type_name::<F>()).finish()
    }
}

pub struct Mapped<M: Mutability, F: Format> {
    inner: Arc<MappedInner>,
    view: View<F>,
    marker: PhantomData<fn() -> M>
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
    marker: PhantomData<fn() -> F>
}

impl<F: Format> View<F> {
    pub fn as_const(&self) -> ViewDescriptor<Const, F> {
        ViewDescriptor {
            view: self,
            descriptor: SyncUnsafeCell::new(Some(())),
            taken: AtomicBool::new(false)
        }
    }

    pub fn as_mut(&self, usage: <F::Kind as Kind>::UsageDescriptor) -> ViewDescriptor<Mut, F> {
        ViewDescriptor {
            view: self,
            descriptor: SyncUnsafeCell::new(Some(usage)),
            taken: AtomicBool::new(false)
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
            marker: PhantomData
        }
    }
}

impl<F: Format> std::fmt::Debug for View<F> where <F::Kind as Kind>::FormatDescriptor: std::fmt::Debug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("View")
            .field(&type_name::<F>())
            .field(&self.inner)
            .finish()
    }
}

pub struct ViewDescriptor<'a, M: UsageMutability, F: Format> {
    view: &'a View<F>,
    descriptor: SyncUnsafeCell<Option<M::Descriptor<F>>>,
    taken: AtomicBool
}

impl<'a, M: UsageMutability, F: Format> ViewUsage for ViewDescriptor<'a, M, F> {}

impl<'a, M: UsageMutability, F: Format> ViewUsageInner for ViewDescriptor<'a, M, F> {
    fn add_to_list(&self, command_list: &mut DynVec) -> DynEntry<dyn ViewHolder> {
        unsafe {
            assert!(!self.taken.swap(true, Ordering::Relaxed), "Attempted to reuse view descriptor{}", FormattedLabel(" ", self.view.inner.inner.label, ""));
            command_list.push(TypedViewHolder::<M, F> {
                view: self.view.clone(),
                descriptor: take(&mut *self.descriptor.get()).unwrap_unchecked()
            })
        }
    }
}

/// A guard which allows access to the data of a format.
pub struct ViewRef<'a, M: Mutability, F: Format> {
    /// The inner reference to the data.
    reference: RwCellGuard<'a, M, *mut ()>,
    marker: PhantomData<(&'a F, fn() -> M)>
}

impl<'a, M: Mutability, F: Format + std::fmt::Debug> std::fmt::Debug for ViewRef<'a, M, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (**self).fmt(f)
    }
}

impl<'a, M: Mutability, F: Format> Deref for ViewRef<'a, M, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        unsafe {
            &*self.reference.cast_const().cast()
        }
    }
}

impl<'a, F: Format> DerefMut for ViewRef<'a, Mut, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            &mut *self.reference.cast()
        }
    }
}

trait DerivedFormatUpdater: 'static + Send + Sync {
    unsafe fn allocate(&self, descriptor: *const ()) -> Box<UnsafeCell<dyn Any + Send + Sync>>;
    fn format_type_id(&self) -> TypeId;
    unsafe fn update(&self, context: CommandContext, usages: *const [*const ()]);
}

trait ExecutableCommand: 'static + Send + Sync {
    unsafe fn execute(&self, ctx: CommandContext);
}

trait ViewHolder: 'static + Send + Sync {
    fn context_id(&self) -> u64;
    fn id(&self) -> u32;
    fn mutable(&self) -> bool;
    fn usage(&self) -> *const ();
}

impl<F: 'static + Send + Sync + FnOnce(CommandContext)> ExecutableCommand for SyncUnsafeCell<Option<F>> {
    unsafe fn execute(&self, ctx: CommandContext) {
        take(&mut *self.get()).unwrap_unchecked()(ctx);
    }
}

struct ActiveCommandBuffer {
    command_list: Arc<DynVec>,
    label: Option<&'static str>,
    remaining_commands: u32
}

struct CommandContextView {
    pub id: u32,
    pub mutable: bool,
    pub value: RwCell<*mut ()>
}

struct CommandEntry {
    pub computation: SyncUnsafeCell<Option<Computation>>,
    pub first_view_entry: Option<DynEntry<ViewEntry>>,
    pub label: Option<&'static str>,
    pub next_instance: Option<DynEntry<CommandEntry>>,
}

#[derive(Clone)]
enum Computation {
    Execute {
        command: DynEntry<dyn ExecutableCommand>
    },
    Map {
        inner: Option<Arc<MappedInner>>
    },
    Update {
        object: u32,
        updates: Vec<(Arc<DynVec>, DynEntry<dyn ViewHolder>)>
    }
}

struct ComputationNode {
    pub computation: Computation,
    pub command_buffer: u16,
    pub derived_update: Option<DerivedFormatUpdate>,
    pub label: Option<&'static str>,
    pub views: Vec<ComputationViewReference>
}

struct ComputationViewReference {
    pub id: u32,
    pub mutable: bool,
    pub view_holder: DynEntry<dyn ViewHolder>
}

struct CondvarNotificationListener {
    #[allow(unused)]
    handle: ChangeNotificationListener,
    inner: Arc<Condvar>,
}

impl CondvarNotificationListener {
    pub fn new(notifier: &ChangeNotifier) -> Self {
        let inner = Arc::new(Condvar::new());
        let inner_clone = inner.clone();
        let handle = notifier.add_listener(move || inner_clone.notify_all());

        Self {
            handle,
            inner
        }
    }

    pub unsafe fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
        self.inner.wait(guard).unwrap_unchecked()
    }
}

struct ContextHolder {
    change_notifier: ChangeNotifier,
    change_listener: CondvarNotificationListener,
    context_id: u64,
    inner: Mutex<ContextInner>
}

struct ContextInner {
    active_command_buffers: Slab<ActiveCommandBuffer>,
    compute_graph: DirectedAcyclicGraph<ComputationNode>,
    context_id: u64,
    critical_nodes: DirectedAcyclicGraphFlags,
    critical_top_level_nodes: DirectedAcyclicGraphFlags,
    objects: Slab<DataHolder>,
    object_update_sender: Sender<ObjectUpdate>,
    object_updates: std::sync::mpsc::Receiver<ObjectUpdate>,
    stalled: bool,
    temporary_node_buffer: Vec<NodeId>,
    top_level_nodes: DirectedAcyclicGraphFlags,
}

impl ContextInner {
    /// Creates a new object from the provided descriptor. Objects are also created for each derived format,
    /// and will automatically update whenever this object is changed.
    pub fn allocate<F: Format>(&mut self, descriptor: AllocationDescriptor<F>) -> Data<F::Kind> {
        unsafe {
            let mut derived_formats = Vec::with_capacity(descriptor.derived_formats.len());
            let mut derived_states: Vec<DerivedFormatState> = Vec::with_capacity(descriptor.derived_formats.len());
    
            let object = F::allocate(&descriptor.descriptor);
            let id = self.objects.insert(DataHolder {
                immutable_references: Vec::new(),
                mutable_references: Vec::new(),
                label: descriptor.label,
                derive_state: FormatDeriveState::Base { derived_formats: Vec::new() },
                value: Box::pin(UnsafeCell::new(object))
            }) as u32;

            for (index, derived) in descriptor.derived_formats.iter().enumerate() {
                let derived_object = derived.inner.allocate(&descriptor.descriptor as *const _ as *const _);
                let id = self.objects.insert(DataHolder {
                    immutable_references: Vec::new(),
                    mutable_references: Vec::new(),
                    label: descriptor.label,
                    derive_state: FormatDeriveState::Derived {
                        index: index as u8,
                        parent: id,
                        updater: derived.inner.clone()
                    },
                    value: Box::into_pin(derived_object)
                }) as u32;
                assert!(derived.inner.format_type_id() != TypeId::of::<F>(), "Derived format cannot be the same type as parent.");
                assert!(derived_states.iter().all(|x| x.format_id != derived.inner.format_type_id()), "Duplicate derived formats.");
                
                let format_id = derived.inner.format_type_id();
                derived_formats.push((format_id, id));
                derived_states.push(DerivedFormatState {
                    format_id,
                    id,
                    next_update: None
                });
            }

            if let FormatDeriveState::Base { derived_formats } = &mut self.objects[id as usize].derive_state {
                *derived_formats = derived_states;
            }
            else {
                unreachable!()
            }
    
            let inner = Arc::new(DataInner {
                context_id: self.context_id,
                derived_formats,
                descriptor: descriptor.descriptor,
                format_id: TypeId::of::<F>(),
                id,
                label: descriptor.label,
                object_updater: self.object_update_sender.clone()
            });

            Data { inner }
        }
    }

    /// Schedules the provided command buffer for execution.
    pub fn submit(&mut self, buffers: impl IntoIterator<Item = CommandBuffer>, context: &ContextHolder) {
        self.update_objects();
        let mut added_top_level_node = false;
        for buffer in buffers {
            added_top_level_node |= self.submit_buffer(buffer);
        }
        self.critical_top_level_nodes.and(&self.critical_nodes, &self.top_level_nodes);
        
        if added_top_level_node && self.stalled {
            self.stalled = false;
            context.change_notifier.notify();
        }
    }

    fn create_command_context(&self, context: &Arc<ContextHolder>, command_id: NodeId) -> CommandContext {
        let computation = &self.compute_graph[command_id];
        CommandContext {
            command_id,
            command_buffer_label: self.active_command_buffers[computation.command_buffer as usize].label,
            context: context.clone(),
            label: computation.label,
            views: computation.views.iter().map(|x| CommandContextView {
                id: x.id,
                mutable: x.mutable,
                value: RwCell::new(self.objects[x.id as usize].value.get().cast())
            }).collect()
        }
    }

    fn prepare_next_command(&mut self, context: &Arc<ContextHolder>) -> Option<Option<Box<dyn WorkUnit>>> {
        unsafe {
            if let Some(node) = self.pop_command() {
                let ctx = self.create_command_context(context, node);
                let computation = &mut self.compute_graph[node];
    
                match &mut computation.computation {
                    Computation::Execute { command } => {
                        let command = *command;
                        let command_buffer = self.active_command_buffers[computation.command_buffer as usize].command_list.clone();
                        Some(Some(Box::new(move || command_buffer[command].execute(ctx))))
                    },
                    Computation::Map { inner } => {
                        let value = take(inner).unwrap_unchecked();
                        *value.command_context.get() = MaybeUninit::new(ctx);
                        value.mapped.store(true, Ordering::Release);
                        Some(None)
                    },
                    Computation::Update { object, updates } => {
                        let derived = *object;
                        let value = &self.objects[derived as usize];
                        let FormatDeriveState::Derived { updater, parent, index, .. } = &value.derive_state 
                        else {
                            unreachable!()
                        };

                        let parent = *parent as usize;
                        let index = *index as usize;
                        let updater = updater.clone();

                        let format_state = if let FormatDeriveState::Base { derived_formats } = &mut self.objects[parent].derive_state {
                            &mut derived_formats[index]
                        }
                        else {
                            unreachable!()
                        };

                        if format_state.next_update == Some(node) {
                            format_state.next_update = None;
                        }

                        let updates = take(updates);
                        Some(Some(Box::new(move || {
                            let mut update_list = Vec::with_capacity(updates.len());
                            update_list.extend(updates.iter().map(|(buffer, entry)| buffer[*entry].usage()));
                            updater.update(ctx, transmute(&update_list[..]));
                        })))
                    },
                }
            }
            else {
                if !self.compute_graph.is_empty() {
                    self.stalled = true;
                }
                
                None
            }
        }
    }

    fn pop_command(&mut self) -> Option<NodeId> {
        if let Some(node) = self.critical_top_level_nodes.first_set_node() {
            self.critical_top_level_nodes.set(node, false);
            self.top_level_nodes.set(node, false);
            Some(node)
        }
        else if let Some(node) = self.top_level_nodes.first_set_node() {
            self.top_level_nodes.set(node, false);
            Some(node)
        }
        else {
            None
        }
    }

    fn submit_buffer(&mut self, buffer: CommandBuffer) -> bool {
        unsafe {
            let mut added_top_level_node = false;
        
            if let Some(first_entry) = buffer.first_command_entry {
                let command_buffer_id = self.active_command_buffers.insert(ActiveCommandBuffer {
                    command_list: Arc::new(buffer.command_list),
                    label: buffer.label,
                    remaining_commands: 0,
                }) as u16;
        
                let mut command_entry = Some(first_entry);
                while let Some(entry) = command_entry {
                    let next = &self.active_command_buffers[command_buffer_id as usize].command_list[entry];
                    command_entry = next.next_instance;
                    added_top_level_node |= self.schedule_command(command_buffer_id, buffer.label, take(&mut *next.computation.get()).unwrap_unchecked(), next.label, next.first_view_entry);
                }
            }
    
            added_top_level_node
        }
    }

    fn schedule_command(&mut self, command_buffer_id: u16, command_buffer_label: Option<&'static str>, computation: Computation, label: Option<&'static str>, first_view_entry: Option<DynEntry<ViewEntry>>) -> bool {
        unsafe {
            let command_buffer = self.active_command_buffers[command_buffer_id as usize].command_list.clone();
            let node = self.compute_graph.vacant_node();

            // Iterate over all used views to find any conflicting computations

            self.temporary_node_buffer.clear();
            let mut views = Vec::new();
            let mut view_entry = first_view_entry;
            
            while let Some(entry) = view_entry {
                let next = &command_buffer[entry];
                let next_view = &command_buffer[next.view];
                assert!(next_view.context_id() == self.context_id, "View did not belong to this context.");
                views.push(ComputationViewReference {
                    id: next_view.id(),
                    view_holder: next.view,
                    mutable: next_view.mutable()
                });

                view_entry = next.next_instance;
                let object = &mut self.objects[next_view.id() as usize];

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

                        if let Some(derived) = &self.compute_graph[computation].derived_update {
                            if derived.parent == next_view.id() {
                                let FormatDeriveState::Base { derived_formats } = &mut object.derive_state
                                else {
                                    unreachable!()
                                };
                                
                                if derived_formats[derived.index as usize].next_update == Some(computation) {
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
                }
                else {
                    &mut object.immutable_references
                }.push(node);
            }

            // Add the new computation to the graph
            self.compute_graph.insert(ComputationNode {
                computation: computation.clone(),
                command_buffer: command_buffer_id as u16,
                derived_update: None,
                label,
                views
            }, &self.temporary_node_buffer);

            self.top_level_nodes.resize_for(&self.compute_graph);
            self.critical_nodes.resize_for(&self.compute_graph);

            for i in 0..self.compute_graph[node].views.len() {
                let view = &self.compute_graph[node].views[i];
                let view_id = view.id;
                let view_holder = view.view_holder;
                let mutable = view.mutable;
                let object = &mut self.objects[view.id as usize];
                let mut derived_nodes_to_add = Vec::new();
                match &mut object.derive_state {
                    FormatDeriveState::Base { derived_formats } => if mutable {
                        for (index, format) in derived_formats.iter_mut().enumerate() {
                            let derived = if let Some(derived) = format.next_update {
                                self.compute_graph.add_parent(node, derived);
                                self.top_level_nodes.set(derived, false);
                                derived
                            }
                            else {
                                let derived_computation = self.compute_graph.insert(ComputationNode {
                                    computation: Computation::Update { object: format.id, updates: Vec::with_capacity(1) },
                                    command_buffer: command_buffer_id as u16,
                                    derived_update: Some(DerivedFormatUpdate {
                                        parent: view_id,
                                        index: index as u32
                                    }),
                                    label: None,
                                    views: vec!(ComputationViewReference {
                                        id: view_id,
                                        view_holder: view_holder,
                                        mutable: false
                                    }, ComputationViewReference {
                                        id: format.id,
                                        view_holder: view_holder,
                                        mutable: true
                                    })
                                }, &[node]);
                                format.next_update = Some(derived_computation);
                                object.immutable_references.push(derived_computation);
                                derived_nodes_to_add.push((format.id, derived_computation));
                                self.active_command_buffers[command_buffer_id as usize].remaining_commands += 1;
                                println!("ADD {derived_computation:?}");
                                derived_computation
                            };

                            let Computation::Update { updates, .. } = &mut self.compute_graph[derived].computation
                            else {
                                unreachable!();
                            };

                            updates.push((command_buffer.clone(), view_holder));
                        }
                    },
                    &mut FormatDeriveState::Derived { parent, index, .. } => {
                        if let FormatDeriveState::Base { derived_formats } = &mut self.objects[parent as usize].derive_state {
                            derived_formats[index as usize].next_update = None;
                        }
                        else {
                            unreachable!();
                        }
                    },
                }

                for (id, node) in derived_nodes_to_add {
                    self.objects[id as usize].mutable_references.push(node);
                }
            }

            if let Computation::Map { inner } = computation {
                inner.unwrap_unchecked().context_id.store(self.context_id, Ordering::Release);
                self.mark_critical(node);
            }

            self.active_command_buffers[command_buffer_id as usize].remaining_commands += 1;

            if self.temporary_node_buffer.is_empty() {
                self.top_level_nodes.set(node, true);
                true
            }
            else {
                false
            }
        }
    }

    fn mark_critical(&mut self, node: NodeId) {
        self.critical_nodes.set(node, true);
        while let Some(parent) = self.temporary_node_buffer.pop() {
            if !self.critical_nodes.get(parent) {
                self.temporary_node_buffer.extend(self.compute_graph.parents(parent));
                self.critical_nodes.set(parent, true);
            }
        }
    }

    fn complete_command(&mut self, id: NodeId, context: &ContextHolder) {
        self.temporary_node_buffer.clear();
        self.temporary_node_buffer.extend(self.compute_graph.children(id));
        self.critical_nodes.set(id, false);
        let computation = self.compute_graph.pop(id);

        for child in self.temporary_node_buffer.iter().copied() {
            if self.compute_graph.parents(child).next().is_none() {
                self.top_level_nodes.set(child, true);
            }
        }

        for view in computation.views {
            let object = &mut self.objects[view.id as usize];
            let view_vec = if view.mutable {
                &mut object.mutable_references
            }
            else {
                &mut object.immutable_references
            };
            view_vec.swap_remove(view_vec.iter().position(|x| *x == id).expect("Failed to remove node from references list."));
        }

        self.critical_top_level_nodes.and(&self.top_level_nodes, &self.critical_nodes);

        if !self.temporary_node_buffer.is_empty() && self.stalled {
            self.stalled = false;
            context.change_notifier.notify();
        }
    }

    fn update_objects(&mut self) {
        while let Ok(update) = self.object_updates.try_recv() {
            match update {
                ObjectUpdate::DropData { id } => { self.objects.remove(id as usize); },
            }
        }
    }
}

struct DataHolder {
    pub derive_state: FormatDeriveState,
    pub label: Option<&'static str>,
    pub immutable_references: Vec<NodeId>,
    pub mutable_references: Vec<NodeId>,
    pub value: Pin<Box<UnsafeCell<dyn Any + Send + Sync>>>,
}

struct DataInner<K: Kind> {
    pub context_id: u64,
    pub derived_formats: Vec<(TypeId, u32)>,
    pub descriptor: K::FormatDescriptor,
    pub format_id: TypeId,
    pub id: u32,
    pub label: Option<&'static str>,
    pub object_updater: Sender<ObjectUpdate>
}

impl<K: Kind> Drop for DataInner<K> {
    fn drop(&mut self) {
        let _ = self.object_updater.send(ObjectUpdate::DropData { id: self.id });
    }
}

struct DerivedFormatUpdate {
    pub parent: u32,
    pub index: u32
}

struct DerivedFormatState {
    pub format_id: TypeId,
    pub id: u32,
    pub next_update: Option<NodeId>
}

enum FormatDeriveState {
    Base {
        derived_formats: Vec<DerivedFormatState>,
    },
    Derived {
        parent: u32,
        index: u8,
        updater: Arc<dyn DerivedFormatUpdater>
    }
}

struct FormattedLabel(pub &'static str, pub Option<&'static str>, pub &'static str);

impl std::fmt::Display for FormattedLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(label) = &self.1 {
            f.write_fmt(format_args!("{}'{}'{}", self.0, label, self.2))
        }
        else {
            Ok(())
        }
    }
}

struct MappedInner {
    pub context_id: AtomicU64,
    pub command_context: UnsafeCell<MaybeUninit<CommandContext>>,
    pub mapped: AtomicBool
}

impl Default for MappedInner {
    fn default() -> Self {
        Self {
            context_id: AtomicU64::new(u64::MAX),
            command_context: UnsafeCell::new(MaybeUninit::uninit()),
            mapped: AtomicBool::new(false)
        }
    }
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

enum ObjectUpdate {
    DropData {
        id: u32
    }
}

struct ViewEntry {
    pub next_instance: Option<DynEntry<ViewEntry>>,
    pub view: DynEntry<dyn ViewHolder>
}

struct TypedDerivedFormatUpdater<F: Format, D: DerivedDescriptor<F>> {
    pub descriptor: D,
    pub marker: PhantomData<fn() -> (F, D)>
}

impl<F: Format, D: DerivedDescriptor<F>> DerivedFormatUpdater for TypedDerivedFormatUpdater<F, D> {
    unsafe fn allocate(&self, descriptor: *const ()) -> Box<UnsafeCell<dyn Any + Send + Sync>> {
        Box::new(UnsafeCell::new(<D::Format as Format>::allocate(&*(descriptor as *const _))))
    }

    fn format_type_id(&self) -> TypeId {
        TypeId::of::<D::Format>()
    }

    unsafe fn update(&self, context: CommandContext, usages: *const [*const ()]) {
        self.descriptor.update(&mut *context.views[1].value.borrow().cast(), &*context.views[1].value.borrow().cast(), transmute(usages));
    }
}

struct TypedViewHolder<M: UsageMutability, F: Format> {
    view: View<F>,
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

    pub trait ViewUsageInner {
        fn add_to_list(&self, command_list: &mut DynVec) -> DynEntry<dyn ViewHolder>;
    }

    pub trait UsageMutability: Mutability {
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
    pub struct IntDescriptor {
        pub initial_value: u32
    }

    pub struct Int;

    impl Kind for Int {
        type FormatDescriptor = IntDescriptor;
        type UsageDescriptor = u32;
    }

    #[derive(Debug)]
    pub struct Int32(pub i32);

    impl Format for Int32 {
        type Kind = Int;

        fn allocate(descriptor: &<Self::Kind as Kind>::FormatDescriptor) -> Self {
            Self(descriptor.initial_value as i32)
        }
    }

    impl Drop for Int32 {
        fn drop(&mut self) {
            println!("Drop int");
        }
    }

    #[derive(Debug)]
    pub struct DoubledInt32(pub i32);

    impl Format for DoubledInt32 {
        type Kind = Int;

        fn allocate(descriptor: &<Self::Kind as Kind>::FormatDescriptor) -> Self {
            Self(2 * descriptor.initial_value as i32)
        }
    }

    impl Drop for DoubledInt32 {
        fn drop(&mut self) {
            println!("Drop doubled");
        }
    }

    pub struct DoubleIntDerive;

    impl DerivedDescriptor<Int32> for DoubleIntDerive {
        type Format = DoubledInt32;

        fn update(&self, data: &mut Self::Format, parent: &Int32, usages: &[&u32]) {
            data.0 = 2 * parent.0;
            println!("{usages:?}");
        }
    }

    #[test]
    #[should_panic]
    fn test_panic_on_conflicting_usage() {
        let ctx = DataFrostContext::new(ContextDescriptor { label: Some("my context") });
        let data = ctx.allocate::<Int32>(AllocationDescriptor {
            descriptor: IntDescriptor { initial_value: 23 },
            label: Some("my int"),
            derived_formats: &[]
        });
        
        let mut command_buffer = CommandBuffer::new(CommandBufferDescriptor { label: Some("my command buffer") });
        let view = data.view::<Int32>();

        command_buffer.schedule(CommandDescriptor {
            label: Some("Test command"),
            views: &[&view.as_const(), &view.as_mut(25)],
            command: |_| {},
        });

        ctx.submit(Some(command_buffer));
    }

    #[test]
    fn test_allow_multiple_const_usage() {
        let ctx = DataFrostContext::new(ContextDescriptor { label: Some("my context") });
        let data = ctx.allocate::<Int32>(AllocationDescriptor {
            descriptor: IntDescriptor { initial_value: 23 },
            label: Some("my int"),
            derived_formats: &[]
        });
        
        let mut command_buffer = CommandBuffer::new(CommandBufferDescriptor { label: Some("my command buffer") });
        let view_a = data.view::<Int32>();
        let view_b = data.view::<Int32>();

        command_buffer.schedule(CommandDescriptor {
            label: Some("Test command"),
            command: |_| {},
            views: &[&view_a.as_const(), &view_b.as_const()]
        });

        ctx.submit(Some(command_buffer));
    }

    #[test]
    fn test_api() {
        let ctx = DataFrostContext::new(ContextDescriptor { label: Some("my context") });
        let data = ctx.allocate::<Int32>(AllocationDescriptor {
            descriptor: IntDescriptor { initial_value: 23 },
            label: Some("my int"),
            derived_formats: &[&Derived::new(DoubleIntDerive)]
        });
        
        let mut command_buffer = CommandBuffer::new(CommandBufferDescriptor { label: Some("my command buffer") });
        let view = data.view::<Int32>();
        
        let view_clone = view.clone();
        command_buffer.schedule(CommandDescriptor {
            label: Some("Test command"),
            command: move |ctx| {
                let mut vc = ctx.get_mut(&view_clone);
                println!("Execute {vc:?}");
                vc.0 += 4;
            },
            views: &[&view.as_mut(4)]
        });

        //let mapping1 = command_buffer.map(&data.view::<DoubledInt32>().as_const());
        
        if true {
            let view_clone = view.clone();
            command_buffer.schedule(CommandDescriptor {
                label: Some("Test command"),
                command: move |ctx| {
                    let mut vc = ctx.get_mut(&view_clone);
                    println!("Execute2 {vc:?}");
                    vc.0 += 2;
                },
                views: &[&view.as_mut(2)]
            });
        }

        let mapping2 = command_buffer.map(&data.view::<DoubledInt32>().as_const());
        ctx.submit(Some(command_buffer));

        //let value = ctx.get(&mapping1);
        //println!("Mapped {}", value.0);
        //drop(value);
        //drop(mapping1);
        let value = ctx.get(&mapping2);
        println!("Mapped {}", value.0);
        drop(value);
        drop(mapping2);
    }
}