# Upstre

Upstre is a Rust library for tracking the latest value from a stream in read-heavy workloads.

It keeps a shared, atomically updatable value and recreates the stream after errors or completion.

## Usage

```rust
use std::{convert::Infallible, future::ready};

use futures_util::stream::{once, Stream};
use upstre::UpstreBuilder;

async fn make_stream() -> Result<impl Stream<Item = Result<i32, Infallible>>, Infallible> {
    Ok(once(ready(Ok(42))))
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let upstre = UpstreBuilder::default().build(make_stream).await.unwrap();
    let value = upstre.value();
    println!("Latest value: {:?}", value);
}
```

## Notes

- Requires a Tokio runtime.
- Cloned `Upstre` handles share the same latest value.
- The background task is aborted automatically when all handles are dropped.

## License
Licensed under Apache-2.0.
