//! tokio-bus provides a lock-free, bounded, single-produce, multi-consumer,
//! broadcast channel usable as a `Sink` and `Stream` with tokio.
//!
//! The bus implementation itself is the wonderful [bus crate](https://crates.io/crates/bus),
//! this crate provides a layer on top to allow using the bus with tokio.
//!
//! # Example
//!
//! ```
//! use tokio;
//! use tokio_bus::Bus;
//! use futures::future::{Future, lazy, ok};
//! use futures::stream::{Stream, iter_ok};
//! use futures::sink::Sink;
//!
//! let mut bus = Bus::new(64);
//! let rx1 = bus.add_rx();
//! let rx2 = bus.add_rx();
//!
//! let send_values = bus
//!     .send_all(iter_ok::<_, ()>(vec![1, 2, 3, 4, 5, 6]));
//!
//! let sum_values = rx1
//!     .fold(0i32, |acc, x| { ok(acc + x) });
//!
//! let div_values = rx2
//!     .fold(1f64, |acc, x| { ok(x as f64 / acc) });
//!
//! let runtime = tokio::runtime::Runtime::new().unwrap();
//! runtime.block_on_all(lazy(move || {
//!     tokio::spawn(send_values
//!         .map(|_| {})
//!         .map_err(|_| { panic!(); })
//!     );
//!     assert_eq!(sum_values.wait(), Ok(21));
//!     assert_eq!(div_values.wait(), Ok(3.2));
//!     ok::<(), ()>(())
//! })).unwrap();
//!
//! ```
use std::sync::mpsc::{RecvError, TryRecvError};
use std::sync::{Arc, Weak};

use tokio::prelude::task::AtomicTask;

use futures::prelude::*;
use futures::AsyncSink;

use bus;

/// A bus which buffers messages for all of its readers
/// to eventually read. Allows the dynamic addition and
/// removal of readers.
pub struct Bus<T: Clone + Sync> {
    inner: bus::Bus<T>,
    /// Tasks indicating reads are likely to now succeed
    read_tasks: Vec<Weak<AtomicTask>>,
    /// Task indicating writes are likely to now succeed
    write_task: Arc<AtomicTask>,
}

impl<T: Clone + Sync> Bus<T> {
    /// Create a new `Bus` that will buffer at most `len` messages.
    ///
    /// Note that until all readers have read a given message
    /// (or the reader has been dropped) it is kept in the
    /// buffer and counts against the buffer size. 
    pub fn new(len: usize) -> Self {
        let inner = bus::Bus::new(len);
        let read_tasks = Vec::new();
        let write_task = Arc::new(AtomicTask::new());
        Bus {
            inner,
            read_tasks,
            write_task,
        }
    }

    /// Create a new `BusReader` instance which can be used to
    /// read messages from the `Bus` that were sent *after* the
    /// creation of this `BusReader`.
    pub fn add_rx(&mut self) -> BusReader<T> {
        let inner = self.inner.add_rx();
        let read_task = Arc::new(AtomicTask::new());
        self.read_tasks
            .push(Arc::downgrade(&Arc::clone(&read_task)));
        BusReader::new(inner, read_task, Arc::clone(&self.write_task))
    }

    /// Attempt to broadcast a message synchronously, failing and
    /// returning the item set to be broadcast if the broadcast
    /// cannot be completed without blocking.
    ///
    /// It can be inconvient in some cases to have to deal with
    /// the `Sink` trait as it necessarily needs to take and pass
    /// back ownership. The `Bus` does not need any blocking I/O
    /// except when the buffer is full, which in some systems is
    /// an easy situation to avoid.
    ///
    /// This method will allow for synchronous sending while still
    /// allowing asynchronous readers to be woken up to read.
    pub fn try_broadcast(&mut self, val: T) -> Result<(), T> {
        if let Err(val) = self.inner.try_broadcast(val) {
            Err(val)
        } else {
            self.notify_readers();
            Ok(())
        }
    }

    /// Wake all tasks for our readers, and silently drop any
    /// read task handles for which the reader has been dropped.
    ///
    /// We do some manual index nonsense because it seems to be
    /// the easiest way to be able to remove elements that can't
    /// be upgraded without allocating a new Vec
    fn notify_readers(&mut self) {
        let mut i = 0;
        while i < self.read_tasks.len() {
            if let Some(task) = self.read_tasks[i].upgrade() {
                task.notify();
                i += 1;
            } else {
                self.read_tasks.remove(i);
            }
        }
    }
}

impl<T: Clone + Sync> Sink for Bus<T> {
    type SinkItem = T;
    type SinkError = ();

    /// Either successfully buffer the item on the internal
    /// Bus' buffer, or indicate the Sink is full.
    fn start_send(
        &mut self,
        item: Self::SinkItem,
    ) -> futures::StartSend<Self::SinkItem, Self::SinkError> {
        self.write_task.register();
        let result = match self.inner.try_broadcast(item) {
            Ok(_) => {
                self.notify_readers();
                for weak_task in self.read_tasks.iter() {
                    if let Some(task) = weak_task.upgrade() {
                        task.notify();
                    }
                }
                Ok(AsyncSink::Ready)
            }
            Err(item) => Ok(AsyncSink::NotReady(item)),
        };

        result
    }

    /// This sink uses the inner Bus' buffer and therefore a
    /// success with `start_send` has already completed the send
    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::Ready(()))
    }
}

impl<T: Clone + Sync> Drop for Bus<T> {
    /// Make readers aware of the Bus dropping in
    /// order to let them terminate their `Stream`.
    fn drop(&mut self) {
        self.notify_readers()
    }
}

/// The `BusReader` should not be manually crated,
/// but rather crated by calling `add_rx()` on a `Bus`.
///
/// A `Bus` and `BusReader` are both safe to drop at any time,
/// any messages not read by the `BusReader` will not be lost
/// if the `Bus` is dropped first, and if the `BusReader` is
/// dropped first it will not block the `Bus`'s buffer.
pub struct BusReader<T: Clone + Sync> {
    read_task: Arc<AtomicTask>,
    write_task: Arc<AtomicTask>,
    inner: bus::BusReader<T>,
}

impl<T: Clone + Sync> BusReader<T> {
    /// Create a new `BusReader` from a `bus::BusReader`, a
    /// handle to a task to register for writes to the `Bus`
    /// and a handle to a task to notify when a read has
    /// potentially caused buffer space in the `Bus` to
    /// become available.
    pub fn new(
        inner: bus::BusReader<T>,
        read_task: Arc<AtomicTask>,
        write_task: Arc<AtomicTask>,
    ) -> Self {
        BusReader {
            inner,
            read_task,
            write_task,
        }
    }
}

impl<T: Clone + Sync> Stream for BusReader<T> {
    type Item = T;
    type Error = RecvError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.read_task.register();
        match self.inner.try_recv() {
            Ok(value) => {
                self.write_task.notify();
                Ok(Async::Ready(Some(value)))
            }
            Err(err) => match err {
                TryRecvError::Disconnected => Ok(Async::Ready(None)),
                TryRecvError::Empty => Ok(Async::NotReady),
            },
        }
    }
}

impl<T: Clone + Sync> Drop for BusReader<T> {
    /// This reader dropping may allow some
    /// buffer space to become free, so a
    /// write may succeed afterward.
    fn drop(&mut self) {
        self.write_task.notify();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future;
    use futures::stream::iter_ok;

    #[test]
    /// test the underlying bus has been properly
    /// represented with the `poll` returning the
    /// underlying availability of the items.
    fn basic_receiving() {
        let mut bus: Bus<()> = Bus::new(16);

        let mut receiver1 = bus.add_rx().peekable();
        let mut receiver2 = bus.add_rx().peekable();

        let mut bus = bus.wait();
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on_all(future::lazy(move || {
            // receivers should be empty out of the gate
            assert_eq!(receiver1.peek(), Ok(Async::NotReady));
            assert_eq!(receiver2.peek(), Ok(Async::NotReady));

            // receivers should both receive one message once
            bus.send(()).unwrap();
            bus.flush().unwrap();
            assert_eq!(receiver1.peek(), Ok(Async::Ready(Some(&()))));
            assert_eq!(receiver2.peek(), Ok(Async::Ready(Some(&()))));
            let mut receiver1 = receiver1.skip(1).peekable();
            let mut receiver2 = receiver2.skip(1).peekable();
            assert_eq!(receiver1.peek(), Ok(Async::NotReady));
            assert_eq!(receiver2.peek(), Ok(Async::NotReady));

            // receivers should be able to receive after the
            // bus drops, and get the recv error afterward
            bus.send(()).unwrap();
            bus.flush().unwrap();
            ::std::mem::drop(bus);
            assert_eq!(receiver1.peek(), Ok(Async::Ready(Some(&()))));
            assert_eq!(receiver2.peek(), Ok(Async::Ready(Some(&()))));
            let mut receiver1 = receiver1.skip(1).peekable();
            let mut receiver2 = receiver2.skip(1).peekable();
            assert_eq!(receiver1.peek(), Ok(Async::Ready(None)));
            assert_eq!(receiver2.peek(), Ok(Async::Ready(None)));

            future::ok::<(), ()>(())
        }))
        .unwrap();
    }

    #[test]
    /// Test that receivers dropping does not interrupt the bus
    fn receiver_dropping() {
        let mut bus: Bus<()> = Bus::new(1);

        let receiver1 = bus.add_rx().peekable();
        let receiver2 = bus.add_rx().peekable();

        let mut bus = bus.wait();
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on_all(future::lazy(move || {
            // broadcast a message, receive it on receiver1 and drop
            // receiver2 which *should* allow the single-message buffer
            // to advance.
            //
            // NOTE: the peek call *is* required, as the `skip()` call
            //       will only lazily skip the next element, therefore
            //       it does not actually advance the internal reader's
            //       state until the `peek()` call is made.
            bus.send(()).unwrap();
            bus.flush().unwrap();
            let mut receiver1 = receiver1.skip(1).peekable();
            assert_eq!(receiver1.peek(), Ok(Async::NotReady));
            ::std::mem::drop(receiver2);
            bus.send(()).unwrap();
            bus.flush().unwrap();

            // drop the remaining receiver which should result in
            // in all broadcasts being effectively a noop and
            // therefore should not cause the buffer to block
            ::std::mem::drop(receiver1);
            bus.send(()).unwrap();
            bus.flush().unwrap();
            bus.send(()).unwrap();
            bus.flush().unwrap();

            future::ok::<(), ()>(())
        }))
        .unwrap();
    }

    #[test]
    /// Test task wakeups occur as expected - ie: that a receive can wake the
    /// bus waiting for buffer space to send, and the bus sending can wake the
    /// receivers waiting for more values to read.
    fn task_wakeup() {
        let mut bus: Bus<i32> = Bus::new(1);

        let receiver1 = bus.add_rx().peekable();
        let receiver2 = bus.add_rx().peekable();
        let mut rt = tokio::runtime::Runtime::new().unwrap();

        // With a single-item buffer and multiple items to
        // move through to complete all futures it's impossible
        // for the futures to complete unless the receivers
        // correctly wake up the bus to check for buffer space,
        // and the bus correctly wakes up the receivers to
        // consume from the buffer and make more space availabile
        rt.spawn(receiver1.take(6).collect().map(|_| {}).map_err(|_| {}));
        rt.spawn(receiver2.take(6).collect().map(|_| {}).map_err(|_| {}));
        rt.block_on_all(bus.send_all(iter_ok::<_, ()>(vec![10, 20, 30, 40, 50, 60])))
            .unwrap();
    }
}
