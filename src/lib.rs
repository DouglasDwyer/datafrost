#![feature(coerce_unsized)]
#![feature(downcast_unchecked)]
#![feature(non_null_convenience)]
#![feature(ptr_metadata)]
#![feature(unsize)]

#![allow(warnings)]
//#![deny(missing_docs)]

//! Crate docs

use crate::dyn_vec::*;
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
use std::sync::mpsc::*;
use task_pool::*;
use wasm_sync::Mutex;

/// Defines a dynamic vector type for efficient allocation of variable-sized, hetegenous objects.
mod dyn_vec;

/// Defines a way to create unique IDs.
mod unique_id;

/// Denotes a general class of formats, which all share similar data.
/// Formats of the same kind may be derived from one another.
pub trait Kind: 'static + Send + Sync {
    /// A structure which holds properties common to all formats of this kind.
    type FormatDescriptor: Send + Sync;

    /// A structure which describes the parts of a format that have changed.
    type UsageDescriptor: Send + Sync;
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
    fn update(&self, data: &mut Self::Format, parent: &F, usages: &[<F::Kind as Kind>::UsageDescriptor]);
}

/// A type-erased instance of [`View`] that is used to specify
/// data usage in a [`CommandDescriptor`].
pub trait ViewUsage: 'static + Send + Sync + ViewUsageInner {}

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

    /// Schedules a command to execute on format data.
    pub fn schedule(&mut self, descriptor: CommandDescriptor<impl Send + Sync + FnOnce(CommandContext)>) {
        let command = self.command_list.push(Some(descriptor.command));
        let first_view_entry = self.push_views(descriptor.views);
        let next_command = self.command_list.push(CommandEntry {
            command,
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

}

impl CommandContext {
    /// Immutably gets the data referenced by the view.
    pub fn get<M: Mutability, F: Format>(&self, view: &View<M, F>) -> ViewRef<Const, F> {
        todo!()
    }

    /// Mutably gets the data referenced by the view, and records the usage for updating derived formats.
    /// This function will panic if `view` refers to a derived format.
    pub fn get_mut<F: Format>(&self, view: &View<Mut, F>, usage: <F::Kind as Kind>::UsageDescriptor) -> ViewRef<Mut, F> {
        todo!()
    }
}

impl std::fmt::Debug for CommandContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandContext").finish()
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
    pub fn view<M: Mutability, F: Format<Kind = K>>(&self) -> View<M, F> {
        View {
            inner: self.clone(),
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
    pub fn new(descriptor: ContextDescriptor) -> Self {
        let (object_update_sender, object_updates) = channel();

        let change_notifier = ChangeNotifier::default();
        let inner = Mutex::new(ContextInner {
            context_id: unique_id(),
            label: descriptor.label,
            objects: Slab::new(),
            object_update_sender,
            object_updates
        });

        let holder = Arc::new(ContextHolder {
            change_notifier,
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

    /// Immutably gets the data referenced by the view.
    pub fn get<M: Mutability, F: Format>(&self, view: &View<M, F>) -> ViewRef<Const, F> {
        todo!()
    }

    /// Mutably gets the data referenced by the view, and records the usage for updating derived formats.
    /// This function will panic if `view` refers to a derived format.
    pub fn get_mut<F: Format>(&self, view: &View<Mut, F>, usage: <F::Kind as Kind>::UsageDescriptor) -> ViewRef<Mut, F> {
        todo!()
    }

    /// Schedules the provided command buffer for execution.
    pub fn submit(&self, buffer: CommandBuffer) {
        todo!()
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
        todo!()
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

/// References a specified format underlying a [`Data`] instance.
pub struct View<M: Mutability, F: Format> {
    /// The inner representation of this object.
    inner: Data<F::Kind>,
    /// A marker for generic bounds.
    marker: PhantomData<fn() -> (M, F)>
}

impl<M: Mutability, F: Format> View<M, F> {
    /// Gets the data to which this view refers.
    pub fn data(&self) -> &Data<F::Kind> {
        &self.inner
    }
}

impl<M: Mutability, F: Format> Clone for View<M, F> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            marker: PhantomData
        }
    }
}

impl<M: Mutability, F: Format> std::fmt::Debug for View<M, F> where <F::Kind as Kind>::FormatDescriptor: std::fmt::Debug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("View")
            .field(&type_name::<M>())
            .field(&type_name::<F>())
            .field(&self.inner)
            .finish()
    }
}

impl<M: Mutability, F: Format> ViewUsage for View<M, F> {}

impl<M: Mutability, F: Format> ViewUsageInner for View<M, F> {
    fn add_to_list(&self, command_list: &mut DynVec) -> DynEntry<dyn ViewHolder> {
        command_list.push(self.clone())
    }
}

/// A guard which allows access to the data of a format.
pub struct ViewRef<'a, M: Mutability, F: Format> {
    /// The inner reference to the data.
    reference: M::Ref<'a, F>
}

impl<'a, M: Mutability, F: Format> Deref for ViewRef<'a, M, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        &self.reference
    }
}

impl<'a, F: Format> DerefMut for ViewRef<'a, Mut, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.reference
    }
}

trait DerivedFormatUpdater: 'static + Send + Sync {
    unsafe fn allocate(&self, descriptor: *const ()) -> Box<UnsafeCell<dyn Any + Send + Sync>>;
    fn format_type_id(&self) -> TypeId;
}

trait ExecutableCommand: 'static + Send + Sync {
    unsafe fn execute(&mut self, ctx: CommandContext);
}

trait ViewHolder: 'static + Send + Sync {

}

impl<F: 'static + Send + Sync + FnOnce(CommandContext)> ExecutableCommand for Option<F> {
    unsafe fn execute(&mut self, ctx: CommandContext) {
        take(self).unwrap_unchecked()(ctx);
    }
}

impl<M: Mutability, F: Format> ViewHolder for View<M, F> {
    
}

struct CommandEntry {
    pub command: DynEntry<dyn ExecutableCommand>,
    pub first_view_entry: Option<DynEntry<ViewEntry>>,
    pub label: Option<&'static str>,
    pub next_instance: Option<DynEntry<CommandEntry>>,
}

struct ContextHolder {
    change_notifier: ChangeNotifier,
    inner: Mutex<ContextInner>
}

struct ContextInner {
    context_id: u64,
    label: Option<&'static str>,
    objects: Slab<DataHolder>,
    object_update_sender: Sender<ObjectUpdate>,
    object_updates: std::sync::mpsc::Receiver<ObjectUpdate>
}

impl ContextInner {
    /// Creates a new object from the provided descriptor. Objects are also created for each derived format,
    /// and will automatically update whenever this object is changed.
    pub fn allocate<F: Format>(&mut self, descriptor: AllocationDescriptor<F>) -> Data<F::Kind> {
        unsafe {
            let mut derived_formats = Vec::with_capacity(descriptor.derived_formats.len());
    
            for derived in descriptor.derived_formats {
                let derived_object = derived.inner.allocate(&descriptor.descriptor as *const _ as *const _);
                let id = self.objects.insert(DataHolder {
                    derive_state: FormatDeriveState::Derived { updater: derived.inner.clone() },
                    value: Box::into_pin(derived_object)
                }) as u32;
                derived_formats.push((derived.inner.format_type_id(), id));
            }
    
            let object = F::allocate(&descriptor.descriptor);
            let id = self.objects.insert(DataHolder {
                derive_state: FormatDeriveState::Base { derived_formats },
                value: Box::pin(UnsafeCell::new(object))
            }) as u32;
    
            let inner = Arc::new(DataInner {
                context_id: self.context_id,
                descriptor: descriptor.descriptor,
                id,
                label: descriptor.label,
                object_updater: self.object_update_sender.clone()
            });

            Data { inner }
        }
    }
}

struct DataHolder {
    pub derive_state: FormatDeriveState,
    pub value: Pin<Box<UnsafeCell<dyn Any + Send + Sync>>>,
}

struct DataInner<K: Kind> {
    pub context_id: u64,
    pub descriptor: K::FormatDescriptor,
    pub id: u32,
    pub label: Option<&'static str>,
    pub object_updater: Sender<ObjectUpdate>
}

impl<K: Kind> Drop for DataInner<K> {
    fn drop(&mut self) {
        let _ = self.object_updater.send(ObjectUpdate::DropData { id: self.id });
    }
}

enum FormatDeriveState {
    Base {
        derived_formats: Vec<(TypeId, u32)>
    },
    Derived {
        updater: Arc<dyn DerivedFormatUpdater>
    }
}

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
        TypeId::of::<F>()
    }
}

/// Hides implementation details from other crates.
mod private {
    use super::*;

    pub trait ViewUsageInner {
        fn add_to_list(&self, command_list: &mut DynVec) -> DynEntry<dyn ViewHolder>;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    pub struct Image3dDescriptor {
        pub depth: u32
    }

    pub struct Image3d;

    impl Kind for Image3d {
        type FormatDescriptor = Image3dDescriptor;
        type UsageDescriptor = u32;
    }

    pub struct FlatArray3d;

    impl Format for FlatArray3d {
        type Kind = Image3d;

        fn allocate(descriptor: &<Self::Kind as Kind>::FormatDescriptor) -> Self {
            println!("Allocate array 3d with {descriptor:?}");
            Self
        }
    }

    pub struct IntersectionBuffer;

    impl IntersectionBuffer {
        pub fn test_it(&self) {
            println!("Hit test.");
        }
    }

    impl Format for IntersectionBuffer {
        type Kind = Image3d;

        fn allocate(descriptor: &<Self::Kind as Kind>::FormatDescriptor) -> Self {
            println!("Allocate intersection buffer with {descriptor:?}");
            Self
        }
    }

    pub struct IntersectionBufferFromFlatArray3d;

    impl DerivedDescriptor<FlatArray3d> for IntersectionBufferFromFlatArray3d {
        type Format = IntersectionBuffer;

        fn update(&self, data: &mut Self::Format, parent: &FlatArray3d, usages: &[<<FlatArray3d as Format>::Kind as Kind>::UsageDescriptor]) {
            println!("Update intersection buffer for usages {usages:?}");
        }
    }

    #[test]
    fn test_api() {
        let ctx = DataFrostContext::new(ContextDescriptor { label: Some("my context") });
        let data = ctx.allocate::<FlatArray3d>(AllocationDescriptor {
            descriptor: Image3dDescriptor { depth: 29 },
            label: Some("my data"),
            derived_formats: &[&Derived::new(IntersectionBufferFromFlatArray3d)]
        });
        
        let mut command_buffer = CommandBuffer::new(CommandBufferDescriptor { label: Some("my command buffer") });
        let view = data.view::<Const, IntersectionBuffer>();

        command_buffer.schedule(CommandDescriptor {
            label: Some("Test command"),
            command: |ctx| {
                println!("Execute.");
            },
            views: &[&view]
        });

        ctx.submit(command_buffer);

        let intersection_buffer = ctx.get(&view);
        intersection_buffer.test_it();
    }
}