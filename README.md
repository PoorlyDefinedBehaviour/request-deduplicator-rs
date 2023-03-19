# About

Deduplicate tasks based on a key.

# Add to your dependencies

```toml
[dependencies]
request_deduplicator = { git = "https://github.com/poorlydefinedbehaviour/request-deduplicator-rs", tag = "v0.0.1" }
```

# Example

```rust
const NUM_TASKS: usize = 10_000;

let deduplicator = RequestDeduplicator::new();

let request_executions = AtomicUsize::new(0);

let tasks = (0..NUM_TASKS).into_iter().map(|_| {
    deduplicator.dedup("key", async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = request_executions.fetch_add(1, Ordering::SeqCst);
    })
});

for result in futures::future::join_all(tasks).await {
    result.expect("deduplicator task returned error");
}

// Only one request should have been executed. The other requests using the same key that
// arrived while the first request was executing should wait for the result of the first request.
assert_eq!(1, request_executions.load(Ordering::SeqCst),);
```