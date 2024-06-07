#![doc = include_str!("../README.md")]
#![cfg_attr(test, feature(test))]
#![feature(auto_traits)]

mod signal;

use std::{cmp::Reverse, collections::BinaryHeap, future::Future, sync::mpsc, thread::scope};

use pollster::FutureExt;
use signal::Signal;

/// Marker trait for ordered and unordered spawning.
trait Ordering: Sized {}
struct Ordered;
impl Ordering for Ordered {}
struct Unordered;
impl Ordering for Unordered {}

struct Pool<T, O: Ordering> {
    signal: Signal,
    tx: mpsc::Sender<T>,
    rx: mpsc::Receiver<T>,
    total: usize,
    _order: std::marker::PhantomData<O>,
}

impl<T: Send, O: Ordering> Pool<T, O> {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            signal: Signal::new(num_cpus::get().saturating_sub(1).max(1)),
            tx,
            rx,
            total: 0,
            _order: std::marker::PhantomData,
        }
    }
}

trait JoinsterSpawn<T> {
    type Item;
    fn spawn_fn<F>(self, fns: F) -> Vec<Self::Item>
    where
        F: IntoIterator,
        F::Item: FnOnce() -> Self::Item + Send,
        T: Send;

    fn spawn<F>(self, futures: F) -> Vec<Self::Item>
    where
        Self: Sized,
        F: IntoIterator,
        F::Item: Future<Output = Self::Item> + Send,
        T: Send,
    {
        self.spawn_fn(futures.into_iter().map(|f| || f.block_on()))
    }
}

impl<T: Send> JoinsterSpawn<T> for Pool<T, Unordered> {
    type Item = T;
    fn spawn_fn<F>(mut self, fns: F) -> Vec<Self::Item>
    where
        F: IntoIterator,
        F::Item: FnOnce() -> T + Send,
        T: Send,
    {
        scope(|s| {
            for f in fns {
                self.total += 1;
                self.signal.wait();
                s.spawn(|| {
                    let result = f();
                    self.tx.send(result).unwrap();
                    self.signal.notify_one();
                });
            }
        });

        let mut results = Vec::with_capacity(self.total);
        for _ in 0..self.total {
            results.push(self.rx.recv().unwrap());
        }
        results
    }
}

struct MyTuple<T>(T, usize);

impl<T> Ord for MyTuple<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.1.cmp(&other.1)
    }
}

impl<T> PartialOrd for MyTuple<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> PartialEq for MyTuple<T> {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1
    }
}

impl<T> Eq for MyTuple<T> {}

impl<T> JoinsterSpawn<T> for Pool<MyTuple<T>, Ordered>
where
    T: Send,
{
    type Item = T;
    fn spawn_fn<F>(mut self, fns: F) -> Vec<Self::Item>
    where
        F: IntoIterator,
        F::Item: FnOnce() -> T + Send,
        T: Send,
    {
        scope(|s| {
            for f in fns {
                self.total += 1;
                let signal_clone = self.signal.clone();
                let tx = self.tx.clone();
                self.signal.wait();
                s.spawn(move || {
                    let result = f();
                    tx.send(MyTuple(result, self.total)).unwrap();
                    signal_clone.notify_one();
                });
            }
        });

        let mut heap = BinaryHeap::new();

        for _ in 0..self.total {
            heap.push(Reverse(self.rx.recv().unwrap()));
        }
        let mut results = Vec::with_capacity(self.total);
        while let Some(Reverse(MyTuple(result, _))) = heap.pop() {
            results.push(result);
        }
        results
    }
}

pub trait JoinAllExt<T> {
    /// Join all futures in the list, waiting for all of them to complete.
    /// Returns the Unordered Vec of results.
    fn join_unordered(self) -> Vec<T>;

    /// Join all futures in the list, waiting for all of them to complete.
    /// Returns the Ordered Vec of results.
    fn join_ordered(self) -> Vec<T>;
}

impl<T, F, C> JoinAllExt<T> for C
where
    T: Send,
    F: Future<Output = T> + Send,
    C: IntoIterator<Item = F>,
{
    fn join_unordered(self) -> Vec<T> {
        let pool = Pool::<_, Unordered>::new();
        pool.spawn(self)
    }

    fn join_ordered(self) -> Vec<T> {
        let pool = Pool::<_, Ordered>::new();
        pool.spawn(self)
    }
}

// impl<T, F, C> JoinAllExt<T> for C
// where
//     T: Send,
//     F: FnOnce() -> T + Send,
//     C: IntoIterator<Item = F>,
// {
//     fn join_unordered(self) -> Vec<T> {
//         let pool = Pool::<_, Unordered>::new();
//         pool.spawn_fn(self)
//     }

//     fn join_ordered(self) -> Vec<T> {
//         let pool = Pool::<_, Ordered>::new();
//         pool.spawn_fn(self)
//     }
// }

#[cfg(test)]
mod tests {

    extern crate test;
    use test::Bencher;

    use super::*;

    #[test]
    fn test_unordered() {
        let futs = (1..10).map(|x| async move { x });
        let results = futs.join_unordered();
        for i in 1..10 {
            assert!(results.contains(&i));
        }
    }

    #[test]
    fn test_ordered() {
        let futs = (1..10).map(|x| async move { x });
        let results = futs.join_ordered();
        assert_eq!(results, (1..10).collect::<Vec<_>>());
    }

    #[bench]
    fn bench_basic(b: &mut Bencher) {}
}
