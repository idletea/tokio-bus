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

pub struct Bus<T: Clone + Sync> {
    inner: bus::Bus<T>,
    /// Tasks indicating reads are likely to now succeed
    read_tasks: Vec<Weak<AtomicTask>>,
    /// Task indicating writes are likely to now succeed
    write_task: Arc<AtomicTask>,
}

impl<T: Clone + Sync> Bus<T> {
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

    pub fn add_rx(&mut self) -> BusReader<T> {
        let inner = self.inner.add_rx();
        let read_task = Arc::new(AtomicTask::new());
        self.read_tasks
            .push(Arc::downgrade(&Arc::clone(&read_task)));
        BusReader::new(inner, read_task, Arc::clone(&self.write_task))
    }
}

impl<T: Clone + Sync> Sink for Bus<T> {
    type SinkItem = T;
    type SinkError = ();

    /// Either successfully buffer the item on the internal
    /// Bus' buffer, or indicate the Sink is full
    fn start_send(
        &mut self,
        item: Self::SinkItem,
    ) -> futures::StartSend<Self::SinkItem, Self::SinkError> {
        self.write_task.register();
        let result = match self.inner.try_broadcast(item) {
            Ok(_) => {
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

pub struct BusReader<T: Clone + Sync> {
    read_task: Arc<AtomicTask>,
    write_task: Arc<AtomicTask>,
    inner: bus::BusReader<T>,
}

impl<T: Clone + Sync> BusReader<T> {
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
