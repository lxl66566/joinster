mod signal;

use std::{future::Future, sync::mpsc, thread::scope};

use pollster::FutureExt;
use signal::Signal;

struct Pool<T> {
    signal: Signal,
    tx: mpsc::Sender<T>,
    rx: mpsc::Receiver<T>,
}

impl<T: Send> Pool<T> {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            signal: Signal::new(num_cpus::get().saturating_sub(1).max(1)),
            tx,
            rx,
        }
    }

    fn spawn<F>(self, futures: F) -> Vec<T>
    where
        F: IntoIterator,
        F::Item: Future<Output = T> + Send,
    {
        let mut nums = 0;
        scope(|s| {
            for f in futures {
                nums += 1;
                self.signal.wait();
                s.spawn(|| {
                    let result = f.block_on();
                    self.tx.send(result).unwrap();
                    self.signal.notify_one();
                });
            }
        });

        let mut results = Vec::with_capacity(nums);
        for _ in 0..nums {
            results.push(self.rx.recv().unwrap());
        }
        results
    }
}

pub trait JoinAllExt<T> {
    fn join_all(self) -> Vec<T>;
}

impl<T, F, C> JoinAllExt<T> for C
where
    T: Send,
    F: Future<Output = T> + Send,
    C: IntoIterator<Item = F>,
{
    /// Join all futures in the list, waiting for all of them to complete.
    /// Returns the Unordered Vec of results.
    fn join_all(self) -> Vec<T> {
        let pool = Pool::new();
        pool.spawn(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let futs = (1..10).map(|x| async move { x });

        let results = futs.join_all();
        dbg!(&results);
    }
}
