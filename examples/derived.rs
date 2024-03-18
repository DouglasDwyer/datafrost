use datafrost::*;
use std::ops::*;

/// First, we define a general "kind" of data that our program will use.
/// In this case, let's imagine that we want to efficiently deal with
/// arrays of numbers.
pub struct NumberArray;

/// Defines the layout of an array of numbers.
pub struct NumberArrayDescriptor {
    /// The length of the array.
    pub len: usize,
}

impl Kind for NumberArray {
    type FormatDescriptor = NumberArrayDescriptor;
    type UsageDescriptor = Range<usize>;
}

/// Next, we define the primary data format that we would like
/// to use and modify - an array of specifically `u32`s.
pub struct PrimaryArray(Vec<u32>);

impl Format for PrimaryArray {
    type Kind = NumberArray;

    fn allocate(descriptor: &NumberArrayDescriptor) -> Self {
        Self(vec![0; descriptor.len])
    }
}

/// Now, let's imagine that we want to efficiently maintain an
/// acceleration structure containing all of the numbers in
/// the array, but doubled. So, we define the format.
pub struct DoubledArray(Vec<u32>);

impl Format for DoubledArray {
    type Kind = NumberArray;

    fn allocate(descriptor: &NumberArrayDescriptor) -> Self {
        Self(vec![0; descriptor.len])
    }
}

/// Our goal is for `datafrost` to automatically update the doubled
/// array whenever the primary array changes. Thus, we implement
/// a way for it do so.
pub struct DoublePrimaryArray;

impl DerivedDescriptor<PrimaryArray> for DoublePrimaryArray {
    type Format = DoubledArray;

    fn update(&self, data: &mut DoubledArray, parent: &PrimaryArray, usages: &[&Range<usize>]) {
        // Loop over all ranges of the array that have changed, and
        // for each value in the range, recompute the data.
        for range in usages.iter().copied() {
            for i in range.clone() {
                data.0[i] = 2 * parent.0[i];
            }
        }
    }
}

/// Once our data formats are defined, we can create and use them with
/// a `DataFrostContext`. We can schedule commands to asynchronously
/// execute against the context and they will execute in parallel.
fn main() {
    // Create a new context.
    let ctx = DataFrostContext::new(ContextDescriptor {
        label: Some("my context"),
    });

    // Allocate a new primary array object, which has a doubled
    // array as a derived format.
    let data = ctx.allocate::<PrimaryArray>(AllocationDescriptor {
        descriptor: NumberArrayDescriptor { len: 7 },
        label: Some("my data"),
        derived_formats: &[Derived::new(DoublePrimaryArray)],
    });

    // Create a command buffer to record operations to execute
    // on our data.
    let mut command_buffer = CommandBuffer::new(CommandBufferDescriptor {
        label: Some("my command buffer"),
    });

    // Schedule a command to fill the primary number array with some data.
    let view = data.view::<PrimaryArray>();
    let view_clone = view.clone();
    command_buffer.schedule(CommandDescriptor {
        label: Some("fill array"),
        views: &[&view.as_mut(4..6)],
        command: move |ctx| ctx.get_mut(&view_clone).0[4..6].fill(33),
    });

    // Schedule a command to map the contents of the derived acceleration structure
    // so that we may view them synchronously.
    let derived = command_buffer.map(&data.view::<DoubledArray>().as_const());

    // Submit the buffer for processing.
    ctx.submit(command_buffer);

    // The doubled acceleration structure automatically contains the
    // correct, up-to-date data!
    assert_eq!(&[0, 0, 0, 0, 66, 66, 0], &ctx.get(&derived).0[..]);
}
