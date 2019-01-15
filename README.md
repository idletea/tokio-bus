# tokio-bus

[![crates.io badge](https://img.shields.io/crates/v/tokio-bus.svg)](https://crates.io/crates/tokio-bus) [![docs.rs badge](https://docs.rs/tokio-bus/badge.svg)](https://docs.rs/tokio-bus) [![travis-ci.org badge](https://travis-ci.org/oefd/tokio-bus.svg?branch=master)](https://travis-ci.org/oefd/tokio-bus)


Integration to let you use [bus](https://crates.io/crates/bus) with [tokio](https://crates.io/crates/tokio).

# Example

```rust
use tokio;
use tokio_bus::Bus;
use futures::future::{Future, lazy, ok};
use futures::stream::{Stream, iter_ok};
use futures::sink::Sink;

let mut bus = Bus::new(64);
let rx1 = bus.add_rx();
let rx2 = bus.add_rx();

let send_values = bus
    .send_all(iter_ok::<_, ()>(vec![1, 2, 3, 4, 5, 6]));

let sum_values = rx1
    .fold(0i32, |acc, x| { ok(acc + x) });

let div_values = rx2
    .fold(1f64, |acc, x| { ok(x as f64 / acc) });

let runtime = tokio::runtime::Runtime::new().unwrap();
runtime.block_on_all(lazy(move || {
    tokio::spawn(send_values
        .map(|_| {})
        .map_err(|_| { panic!(); })
    );
    assert_eq!(sum_values.wait(), Ok(21));
    assert_eq!(div_values.wait(), Ok(3.2));
    ok::<(), ()>(())
})).unwrap();

```
