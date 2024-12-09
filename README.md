# Upstre

Upstre is a Rust library for tracking stream value in read-heavy tasks.

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

## License
Apache 2.0 license.