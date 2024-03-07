#![allow(warnings)]

pub use mutability_marker::*;
use private::*;
use std::marker::*;
use std::ops::*;
use std::process::CommandArgs;
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
}

/// A format which can act as a maintained "copy" of a parent
/// by synchronizing its contents with those of the parent.
pub trait DerivedFormat<F: Format>: Format<Kind = F::Kind> {
    /// Creates a new instance of this format to track the given parent.
    fn create(descriptor: &<F::Kind as Kind>::FormatDescriptor, parent: &F) -> Self;

    /// Updates this format based upon the portions of the parent that have changed.
    fn update(&mut self, parent: &F, usages: &[<Self::Kind as Kind>::UsageDescriptor]);
}

/// Allows for instantiating a format.
pub trait FormatDescriptor: 'static + Send + Sync + Sized {
    /// The format of data that should be created.
    type Format: Format;

    /// Creates the specified instance of this format.
    fn create(self) -> Self::Format;

    /// Returns a structure holding properties common to the kind of this format.
    fn descriptor(&self) -> <<Self::Format as Format>::Kind as Kind>::FormatDescriptor;

    /// Gets the label that should be associated with this object.
    fn label(&self) -> Option<&'static str>;
}

/// A type-erased instance of [`View`] that is used to specify
/// handle usage in a [`CommandDescriptor`].
pub trait ViewEntry: 'static + Send + Sync + Sealed {
    // todo: stuff to get resource ID
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

#[derive(Clone)]
pub struct FrostContext {
    
}

impl FrostContext {
    pub fn new() -> Self {
        todo!()
    }

    /// Creates a new object from the provided descriptor. Objects are also created for each derived format,
    /// and will automatically update whenever this object is changed.
    pub fn create<D: FormatDescriptor>(&self, descriptor: D, derived_formats: &[Derived<D::Format>]) -> Handle<<D::Format as Format>::Kind> {
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

    /// Schedules a command to execute on format data.
    pub fn schedule(&self, descriptor: CommandDescriptor<impl Send + Sync + FnOnce(CommandContext)>) {
        todo!()
    }
}

impl std::fmt::Debug for FrostContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl WorkProvider for FrostContext {
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
    holder: PhantomData<&'static F>
}

impl<F: Format> Derived<F> {
    /// Specifies that a derived `T` should be created to track an object of type `F`.
    pub fn of<T: DerivedFormat<F>>() -> Self {
        todo!()
    }
}

impl<F: Format> Copy for Derived<F> {}

impl<F: Format> Clone for Derived<F> {
    fn clone(&self) -> Self {
        todo!()
    }
}

impl<F: Format> std::fmt::Debug for Derived<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

/// References an object of a specific [`Kind`]. The object is backed
/// by a single primary format and some number of derived formats.
pub struct Handle<K: Kind> {
    marker: PhantomData<K>
}

impl<K: Kind> Handle<K> {
    /// Creates a view of this data of the given format and mutability.
    /// The format must either be the primary format or one of the derived
    /// formats specified during object creation. If this is a mutable view,
    /// then the format must be the primary format. Using views with invalid
    /// mutability or formats will lead to panics when [`FormatContext::schedule`]
    /// is called.
    pub fn view<M: Mutability, F: Format>(&self) -> View<M, F> {
        todo!("Where to put the check for getting data?")
    }
}

impl<K: Kind> Clone for Handle<K> {
    fn clone(&self) -> Self {
        todo!()
    }
}

impl<K: Kind> std::fmt::Debug for Handle<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

/// References a specified format underlying a [`Handle`].
pub struct View<M: Mutability, F: Format> {
    inner: Handle<F::Kind>,
    marker: PhantomData<&'static (M, F)>
}

impl<M: Mutability, F: Format> View<M, F> {
    /// Gets the handle to which this view refers.
    pub fn handle(&self) -> &Handle<F::Kind> {
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
    }

    pub struct IntersectionBuffer;

    impl IntersectionBuffer {
        pub fn test_it(&self) {
            println!("Hit test.");
        }
    }

    impl Format for IntersectionBuffer {
        type Kind = Image3d;
    }

    impl DerivedFormat<FlatArray3d> for IntersectionBuffer {
        fn create(descriptor: &Image3dDescriptor, parent: &FlatArray3d) -> Self {
            Self
        }

        fn update(&mut self, parent: &FlatArray3d, usages: &[<Self::Kind as Kind>::UsageDescriptor]) {
            println!("Update ib");
        }
    }

    pub struct FlatArray3dDescriptor {

    }

    impl FormatDescriptor for FlatArray3dDescriptor {
        type Format = FlatArray3d;

        fn create(self) -> Self::Format {
            FlatArray3d
        }

        fn descriptor(&self) -> <<Self::Format as Format>::Kind as Kind>::FormatDescriptor {
            Image3dDescriptor {
                depth: 29
            }
        }

        fn label(&self) -> Option<&'static str> {
            None
        }
    }

    #[test]
    fn test_api() {
        let ctx = FrostContext::new();
        let handle = ctx.create(FlatArray3dDescriptor {}, &[Derived::of::<IntersectionBuffer>()]);
        let view = handle.view::<Const, IntersectionBuffer>();

        ctx.schedule(CommandDescriptor {
            label: Some("Test command"),
            command: |ctx| {
                println!("Execute.");
            },
            views: &[&view]
        });

        let intersection_buffer = ctx.get(&view);
        intersection_buffer.test_it();
    }
}