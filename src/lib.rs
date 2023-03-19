use anyhow::{anyhow, Result};
use crossbeam_skiplist::SkipMap;

use std::{future::Future, sync::Arc};
use tokio::sync::broadcast;

const BROADCAST_CHANNEL_CAPACITY: usize = 1;

#[derive(Debug, Clone)]
pub struct RequestDeduplicator<K, V>
where
    K: Ord + Send + 'static,
    V: Clone + Send + 'static,
{
    requests_in_flight: Arc<SkipMap<K, broadcast::Receiver<V>>>,
}

impl<K, V> RequestDeduplicator<K, V>
where
    K: Ord + Send + 'static,
    V: Clone + Send + 'static,
{
    pub fn new() -> Self {
        Self {
            requests_in_flight: Arc::new(SkipMap::new()),
        }
    }

    pub async fn dedup<F>(&self, key: K, future: F) -> Result<V>
    where
        F: Future<Output = V>,
    {
        match self.requests_in_flight.get(&key) {
            None => {
                let (sender, receiver) = broadcast::channel(BROADCAST_CHANNEL_CAPACITY);

                let entry = self.requests_in_flight.insert(key, receiver);

                let result = future.await;

                let _ = entry.remove();

                if let Err(err) = sender.send(result.clone()) {
                    return Err(anyhow!(
                        "erro broadcasting result to listeners. err={}",
                        err.to_string()
                    ));
                }
                Ok(result)
            }
            Some(result_rx) => {
                let value = result_rx
                .value()
                .resubscribe()
                .recv()
                .await
                .expect("bug: the sender will never be closed before sending a value, a value should always be available at some point in the future");
                Ok(value)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use super::*;

    #[tokio::test]
    async fn todo() {
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
    }
}
