use anyhow::{Context, Result};
use crossbeam_skiplist::SkipMap;
use tracing::error;

use std::{future::Future, sync::Arc};
use tokio::sync::broadcast;

const BROADCAST_CHANNEL_CAPACITY: usize = 1;

#[derive(Debug, Clone)]
pub struct RequestDeduplicator<I, K, V>
where
    K: Ord,
{
    requests_in_flight: Arc<SkipMap<K, Task<I, V>>>,
}

#[derive(Debug)]
struct Task<I, V> {
    id: I,
    receiver: broadcast::Receiver<V>,
}

impl<I, K, V> Default for RequestDeduplicator<I, K, V>
where
    K: Ord + Send + 'static,
    V: Clone + Send + 'static,
    I: PartialEq + Send + Clone + 'static,
{
    fn default() -> Self {
        RequestDeduplicator::new()
    }
}

impl<I, K, V> RequestDeduplicator<I, K, V>
where
    K: Ord + Send + 'static,
    V: Clone + Send + 'static,
    I: PartialEq + Send + Clone + 'static,
{
    pub fn new() -> Self {
        Self {
            requests_in_flight: Arc::new(SkipMap::new()),
        }
    }

    pub async fn dedup<F>(&self, key: K, id: I, future: F) -> Result<V>
    where
        F: Future<Output = V>,
        I: std::fmt::Debug,
        K: std::fmt::Debug,
    {
        let (sender, receiver) = broadcast::channel(BROADCAST_CHANNEL_CAPACITY);

        let entry = self.requests_in_flight.get_or_insert(
            key,
            Task {
                id: id.clone(),
                receiver,
            },
        );

        // If the entry was inserted by this task it means that
        // this task is the task that will poll the future.
        if entry.value().id == id {
            let result = future.await;

            // Yield so other tasks have a chance of resubscribing to the receiver.
            tokio::task::yield_now().await;

            let _ = entry.remove();

            if let Err(err) = sender.send(result.clone()) {
                error!(error = err.to_string(), "unable to broadcast task result");
            }

            return Ok(result);
        }
        // Some other task will make the request so this task should just await for the result.
        let mut receiver = entry.value().receiver.resubscribe();
        let result = match receiver
            .recv()
            .await
            .context("trying to receive result from channel")
        {
            Err(_) => {
                // The channel was closed, execute the future to get the result.
                future.await
            }
            Ok(v) => v,
        };
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn basic() {
        const NUM_TASKS: usize = 10_000;

        let deduplicator = Arc::new(RequestDeduplicator::new());

        let request_executions = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::with_capacity(NUM_TASKS);

        for id in 0..NUM_TASKS {
            let deduplicator = deduplicator.clone();
            let request_executions = Arc::clone(&request_executions);

            handles.push(tokio::spawn(async move {
                let _ = deduplicator
                    .dedup("key", id, async move {
                        let _ = request_executions.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    })
                    .await;
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // Only one request should have been executed. The other requests using the same key that
        // arrived while the first request was executing should wait for the result of the first request.
        assert_eq!(1, request_executions.load(Ordering::SeqCst),);

        assert!(deduplicator.requests_in_flight.is_empty());
    }
}
