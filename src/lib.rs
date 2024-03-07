#![allow(warnings)]
#![deny(missing_docs)]

//! Crate docs

pub use mutability_marker::*;
use private::*;
use std::marker::*;
use std::ops::*;
use std::sync::*;
use task_pool::*;

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
pub trait DerivedDescriptor<F: Format> {
    /// The format that should be created for this descriptor.
    type Format: Format<Kind = F::Kind>;

    /// Updates the given data based upon the portions of the parent that have changed.
    fn update(&self, data: &mut Self::Format, parent: &F, usages: &[<F::Kind as Kind>::UsageDescriptor]);
}

/// A type-erased instance of [`View`] that is used to specify
/// data usage in a [`CommandDescriptor`].
pub trait ViewEntry: 'static + Send + Sync + Sealed {
    // todo: stuff to get resource ID
}

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

}

impl CommandBuffer {
    /// Creates a new command buffer with the provided descriptor.
    pub fn new(descriptor: CommandBufferDescriptor) -> Self {
        todo!()
    }

    /// Schedules a command to execute on format data.
    pub fn schedule(&self, descriptor: CommandDescriptor<impl Send + Sync + FnOnce(CommandContext)>) {
        todo!()
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
        todo!()
    }
}

/// Describes a command to execute.
pub struct CommandDescriptor<'a, C: 'static + Send + Sync + FnOnce(CommandContext)> {
    /// The label associated with this command.
    pub label: Option<&'static str>,
    /// The command to execute asynchronously.
    pub command: C,
    /// A list of views that the command will access via the [`CommandContext`].
    pub views: &'a [&'a dyn ViewEntry]
}

impl<'a, C: 'static + Send + Sync + FnOnce(CommandContext)> std::fmt::Debug for CommandDescriptor<'a, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

/// Determines how a [`DataFrostContext`] should behave.
#[derive(Copy, Clone, Debug, Default)]
pub struct ContextDescriptor {
    /// An optional label to associate with the object.
    pub label: Option<&'static str>
}

impl<F: Format> std::fmt::Debug for Derived<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

/// References an object of a specific [`Kind`]. The object is backed
/// by a single primary format and some number of derived formats.
pub struct Data<K: Kind> {
    marker: PhantomData<K>
}

impl<K: Kind> Data<K> {
    /// Creates a view of this data of the given format and mutability.
    /// The format must either be the primary format or one of the derived
    /// formats specified during object creation. If this is a mutable view,
    /// then the format must be the primary format. Using views with invalid
    /// mutability or formats will lead to panics when [`DataFrostContext::submit`]
    /// is called.
    pub fn view<M: Mutability, F: Format>(&self) -> View<M, F> {
        todo!("Where to put the check for getting data?")
    }
}

impl<K: Kind> Clone for Data<K> {
    fn clone(&self) -> Self {
        todo!()
    }
}

impl<K: Kind> std::fmt::Debug for Data<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

/// Manages a collection of formatted data, coordinates the execution of commands
/// against the data, and automatically updates derived formats using the results
/// of those commands.
#[derive(Clone)]
pub struct DataFrostContext {
    
}

impl DataFrostContext {
    /// Allocates a new, empty context.
    pub fn new(descriptor: ContextDescriptor) -> Self {
        todo!()
    }

    /// Creates a new object from the provided descriptor. Objects are also created for each derived format,
    /// and will automatically update whenever this object is changed.
    pub fn allocate<F: Format>(&self, descriptor: AllocationDescriptor<F>) -> Data<F::Kind> {
        todo!()
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
}

impl std::fmt::Debug for DataFrostContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl WorkProvider for DataFrostContext {
    fn change_notifier(&self) -> &ChangeNotifier {
        todo!()
    }

    fn next_task(&self) -> Option<Box<dyn '_ + WorkUnit>> {
        todo!()
    }
}

/// Marks a derived format that should be accessible and kept
/// automatically up-to-date when the parent object changes.
pub struct Derived<F: Format> {
    inner: Arc<()>,
    marker: PhantomData<F>
}

impl<F: Format> Derived<F> {
    /// Specifies how a derived object should be created to track an object of type `F`.
    pub fn new<D: DerivedDescriptor<F>>(descriptor: D) -> Self {
        todo!()
    }
}

impl<F: Format> Clone for Derived<F> {
    fn clone(&self) -> Self {
        todo!()
    }
}

/// References a specified format underlying a [`Data`] instance.
pub struct View<M: Mutability, F: Format> {
    inner: Data<F::Kind>,
    marker: PhantomData<&'static (M, F)>
}

impl<M: Mutability, F: Format> View<M, F> {
    /// Gets the data to which this view refers.
    pub fn data(&self) -> &Data<F::Kind> {
        &self.inner
    }
}

impl<M: Mutability, F: Format> Clone for View<M, F> {
    fn clone(&self) -> Self {
        todo!()
    }
}

impl<M: Mutability, F: Format> std::fmt::Debug for View<M, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl<M: Mutability, F: Format> Sealed for View<M, F> {}
impl<M: Mutability, F: Format> ViewEntry for View<M, F> {}

/// A guard which allows access to the data of a format.
pub struct ViewRef<'a, M: Mutability, F: Format> {
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

/// Hides implementation details from other crates.
mod private {
    /// Ensures that external crates cannot implement derived traits. 
    pub trait Sealed {}
}

#[cfg(test)]
mod tests {
    use super::*;

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
            Self
        }
    }

    pub struct IntersectionBufferFromFlatArray3d;

    impl DerivedDescriptor<FlatArray3d> for IntersectionBufferFromFlatArray3d {
        type Format = IntersectionBuffer;

        fn update(&self, data: &mut Self::Format, parent: &FlatArray3d, usages: &[<<FlatArray3d as Format>::Kind as Kind>::UsageDescriptor]) {
            todo!()
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