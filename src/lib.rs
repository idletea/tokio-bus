use std::sync::mpsc::{RecvError, TryRecvError};
use std::sync::{Arc, Weak};

use tokio::prelude::task::AtomicTask;

use futures::prelude::*;

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

    pub fn broadcast(&mut self, val: T) {
        self.inner.broadcast(val);
        for weak_task in self.read_tasks.iter() {
            if let Some(task) = weak_task.upgrade() {
                task.notify();
            }
        }
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
        match self.inner.try_recv() {
            Ok(value) => {
                self.write_task.notify();
                Ok(Async::Ready(Some(value)))
            }
            Err(err) => match err {
                TryRecvError::Disconnected => Ok(Async::Ready(None)),
                TryRecvError::Empty => {
                    self.read_task.register();
                    Ok(Async::NotReady)
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future;

    #[test]
    /// test the underlying bus has been properly
    /// represented with the `poll` returning the
    /// underlying availability of the items.
    fn basic_receiving() {
        let mut bus: Bus<()> = Bus::new(16);

        let mut receiver1 = bus.add_rx().peekable();
        let mut receiver2 = bus.add_rx().peekable();
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on_all(future::lazy(move || {
            // receivers should be empty out of the gate
            assert_eq!(receiver1.peek(), Ok(Async::NotReady));
            assert_eq!(receiver2.peek(), Ok(Async::NotReady));

            // receivers should both receive one message once
            bus.broadcast(());
            assert_eq!(receiver1.peek(), Ok(Async::Ready(Some(&()))));
            assert_eq!(receiver2.peek(), Ok(Async::Ready(Some(&()))));
            let mut receiver1 = receiver1.skip(1).peekable();
            let mut receiver2 = receiver2.skip(1).peekable();
            assert_eq!(receiver1.peek(), Ok(Async::NotReady));
            assert_eq!(receiver2.peek(), Ok(Async::NotReady));

            // receivers should be able to receive after the
            // bus drops, and get the recv error afterward
            bus.broadcast(());
            ::std::mem::drop(bus);
            assert_eq!(receiver1.peek(), Ok(Async::Ready(Some(&()))));
            assert_eq!(receiver2.peek(), Ok(Async::Ready(Some(&()))));
            let mut receiver1 = receiver1.skip(1).peekable();
            let mut receiver2 = receiver2.skip(1).peekable();
            assert_eq!(receiver1.peek(), Err(RecvError {}));
            assert_eq!(receiver2.peek(), Err(RecvError {}));

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
            bus.broadcast(());
            let mut receiver1 = receiver1.skip(1).peekable();
            assert_eq!(receiver1.peek(), Ok(Async::NotReady));
            ::std::mem::drop(receiver2);
            bus.broadcast(());

            future::ok::<(), ()>(())
        }))
        .unwrap();
    }

    // #[test]
    // /// Test task wakeups occur as expected - ie: that a receive can wake the
    // /// bus waiting for buffer space to send, and the bus sending can wake the
    // /// receivers waiting for more values to read.
    // fn task_wakeup() {
    //     let mut bus: Bus<()> = Bus::new(1);

    //     let receiver1 = bus.add_rx().peekable();
    //     let receiver2 = bus.add_rx().peekable();
    //     let rt = tokio::runtime::Runtime::new().unwrap();

    //     // With a single-item buffer and multiple items to
    //     // move through to complete all futures it's impossible
    //     // for the futures to complete unless the receivers
    //     // correctly wake up the bus to check for buffer space,
    //     // and the bus correctly wakes up the receivers to
    //     // consume from the buffer and make more space availabile
    //     rt.spawn(receiver1.take(4).collect());
    //     rt.block_on_all(
    //         future::ok::<(), ()>(())
    //     }));
    //     .unwrap();
    // }
}
