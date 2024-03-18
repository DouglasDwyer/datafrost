# datafrost

[![Crates.io](https://img.shields.io/crates/v/datafrost.svg)](https://crates.io/crates/datafrost)
[![Docs.rs](https://docs.rs/datafrost/badge.svg)](https://docs.rs/datafrost)
![Nightly](https://img.shields.io/badge/nightly-required-red)

#### Data format and acceleration structure management

`datafrost` is a data-oriented resource management and scheduling library. It implements a graphics API-inspired interface that allows one to cleanly and efficiently:

- Create primary data objects, and define "derived" datatypes whose contents are generated from the primary format.
- Track how a primary object changes and automatically update the affected parts of the derived formats.
- Map the contents of data objects and read them on the main thread.
- Schedule commands to asynchronously and concurrently read or modify data objects.
  - `datafrost` guarantees optimal scheduling by building a directed acyclic graph to represent pending operations.
  - Multiple commands which reference different data, or immutably reference the same data, will execute in parallel.
  - Commands which mutably access the same data run in sequence, without the possibility of data races.

## Usage

The following is an abridged example of how to use `datafrost`. The full code may be found in the
[examples folder](/examples/derived.rs). To begin, we define the data formats that our code will use:

```rust
use datafrost::*;
use std::ops::*;

/// First, we define a general "kind" of data that our program will use.
/// In this case, let's imagine that we want to efficiently deal with
/// arrays of numbers.
pub struct NumberArray;

/// Defines the layout of an array of numbers.
pub struct NumberArrayDescriptor {
    /// The length of the array.
    pub len: usize
}

impl Kind for NumberArray { .. }

/// Next, we define the primary data format that we would like
/// to use and modify - an array of specifically `u32`s.
pub struct PrimaryArray(Vec<u32>);

impl Format for PrimaryArray { .. }

/// Now, let's imagine that we want to efficiently maintain an
/// acceleration structure containing all of the numbers in
/// the array, but doubled. So, we define the format.
pub struct DoubledArray(Vec<u32>);

impl Format for DoubledArray { .. }

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
```

Now that our data and its derived formats are defined, we can create instances of
it and schedule commands to act upon the data:

```rust
// Create a new context.
let ctx = DataFrostContext::new(ContextDescriptor {
    label: Some("my context")
});

// Allocate a new primary array object, which has a doubled
// array as a derived format.
let data = ctx.allocate::<PrimaryArray>(AllocationDescriptor {
    descriptor: NumberArrayDescriptor { len: 7 },
    label: Some("my data"),
    derived_formats: &[&Derived::new(DoublePrimaryArray)]
});

// Create a command buffer to record operations to execute
// on our data.
let mut command_buffer = CommandBuffer::new(CommandBufferDescriptor { label: Some("my command buffer") });

// Schedule a command to fill the primary number array with some data.
let view = data.view::<PrimaryArray>();
let view_clone = view.clone();
command_buffer.schedule(CommandDescriptor {
    label: Some("fill array"),
    views: &[&view.as_mut(4..6)],
    command: move |ctx| ctx.get_mut(&view_clone).0[4..6].fill(33)
});

// Schedule a command to map the contents of the derived acceleration structure
// so that we may view them synchronously.
let derived = command_buffer.map(&data.view::<DoubledArray>().as_const());

// Submit the buffer for processing.
ctx.submit(command_buffer);

// The doubled acceleration structure automatically contains the
// correct, up-to-date data!
assert_eq!(&[0, 0, 0, 0, 66, 66, 0], &ctx.get(&derived).0[..]);
```