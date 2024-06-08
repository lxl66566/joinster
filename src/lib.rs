#![doc = include_str!("../README.md")]
#![cfg_attr(test, feature(test))]

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

/// A thread pool for spawning futures/fns.
struct Pool<T, O: Ordering> {
    /// A classic Signal(Semaphore) to control the number of threads running at
    /// the same time. The number will be set to the number of CPUs - 1.
    signal: Signal,
    /// The Sender given to each spawned thread, to return the result.
    tx: mpsc::Sender<T>,
    /// The Receiver to collect the result of each spawned thread.
    rx: mpsc::Receiver<T>,
    /// The total number of spawned tasks.
    total: usize,
    _ordering: std::marker::PhantomData<O>,
}

impl<T: Send, O: Ordering> Pool<T, O> {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            // Set the number of threads to the number of CPUs - 1.
            signal: Signal::new(num_cpus::get().saturating_sub(1).max(1)),
            tx,
            rx,
            total: 0,
            _ordering: std::marker::PhantomData,
        }
    }
}

trait JoinsterSpawn<T> {
    /// Type of the return value of each spawned tasks.
    type Item;
    /// Collect the results of all spawned threads from the receiver.
    fn collect(&self) -> Vec<Self::Item>;
    /// Spawn all fns in the list, waiting for all of them to complete, and
    /// return the results.
    fn spawn_fn<F>(self, fns: F) -> Vec<Self::Item>
    where
        F: IntoIterator,
        F::Item: FnOnce() -> Self::Item + Send,
        T: Send;

    /// Spawn all futures in the list. The futures will be wrapped to blocking
    /// fns, and then spawned by [`Pool::spawn_fn`].
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
    fn collect(&self) -> Vec<Self::Item> {
        let mut results = Vec::with_capacity(self.total);
        for _ in 0..self.total {
            results.push(self.rx.recv().unwrap());
        }
        results
    }
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
        self.collect()
    }
}

/// A tuple wrapper implements [`Ord`] and [`PartialOrd`] for tuple sorting.
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
    // TODO: bench for BH sorting and quick sorting.
    fn collect(&self) -> Vec<Self::Item> {
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
        self.collect()
    }
}

pub trait JoinFutureExt<T> {
    /// Join all futures in the list, waiting for all of them to complete.
    /// Returns the Unordered Vec of results.
    fn join_unordered(self) -> Vec<T>;

    /// Join all futures in the list, waiting for all of them to complete.
    /// Returns the Ordered Vec of results. Note that the execution order of
    /// closures is still random.
    fn join_ordered(self) -> Vec<T>;
}

impl<T, F, C> JoinFutureExt<T> for C
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

/// I can't implement [`JoinFutureExt`] for both `F: Future<Output = T> + Send`
/// and `F: FnOnce() -> T + Send` because of Rust's restrictions: there may be
/// an object implements both trait, so there may be a potential conflict. For
/// that, I could just make another counterpart. If you have any better idea,
/// please make a PR.
pub trait JoinFnExt<T> {
    /// Join all futures in the list, waiting for all of them to complete.
    /// Returns the Unordered Vec of results.
    fn join_unordered(self) -> Vec<T>;

    /// Join all futures in the list, waiting for all of them to complete.
    /// Returns the Ordered Vec of results. Note that the execution order of
    /// closures is still random.
    fn join_ordered(self) -> Vec<T>;
}

impl<T, F, C> JoinFnExt<T> for C
where
    T: Send,
    F: FnOnce() -> T + Send,
    C: IntoIterator<Item = F>,
{
    fn join_unordered(self) -> Vec<T> {
        let pool = Pool::<_, Unordered>::new();
        pool.spawn_fn(self)
    }

    fn join_ordered(self) -> Vec<T> {
        let pool = Pool::<_, Ordered>::new();
        pool.spawn_fn(self)
    }
}

#[cfg(test)]
mod tests {

    extern crate test;
    use test::Bencher;

    use super::*;

    const BENCH_ITER: std::ops::Range<u32> = 2..400;

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

    /// CPU bound task for benching.
    fn generate_primes(n: u32) -> Vec<u32> {
        assert!(n > 1);
        let mut primes = Vec::with_capacity((n as f32 / (n as f32).ln()) as usize);
        for possible_prime in 2..=n {
            let mut is_prime = true;
            for num in 2..=((possible_prime as f32).sqrt() as u32 + 1) {
                if possible_prime % num == 0 {
                    is_prime = false;
                    break;
                }
            }
            if is_prime {
                primes.push(possible_prime);
            }
        }
        primes
    }

    #[bench]
    fn bench_singlethread(b: &mut Bencher) {
        b.iter(|| {
            let ret = BENCH_ITER.map(generate_primes).collect::<Vec<_>>();
            test::black_box(ret);
        });
    }

    #[bench]
    fn bench_joinster_unordered(b: &mut Bencher) {
        b.iter(|| {
            let ret = BENCH_ITER
                .map(|x| move || generate_primes(x))
                .join_unordered();
            test::black_box(ret);
        });
    }

    #[bench]
    fn bench_joinster_ordered(b: &mut Bencher) {
        b.iter(|| {
            let futs = BENCH_ITER.map(|x| move || generate_primes(x));
            let ret = futs.join_ordered();
            test::black_box(ret);
        });
    }

    #[bench]
    fn bench_rayon(b: &mut Bencher) {
        use rayon::iter::{IntoParallelIterator, ParallelIterator};
        b.iter(|| {
            let ret = BENCH_ITER
                .into_par_iter()
                .map(generate_primes)
                .collect::<Vec<_>>();
            test::black_box(ret);
        });
    }

    #[bench]
    fn bench_tokio(b: &mut Bencher) {
        use tokio::runtime::Builder;
        let rt = Builder::new_multi_thread()
            .enable_all()
            .max_blocking_threads(num_cpus::get().saturating_sub(1).max(1))
            .build()
            .unwrap();
        b.iter(|| {
            let futs = BENCH_ITER.map(|x| rt.spawn_blocking(move || generate_primes(x)));
            let mut ret = Vec::with_capacity(futs.len());
            rt.block_on(async {
                for fut in futs {
                    ret.push(fut.await.unwrap());
                }
            });
            test::black_box(ret);
        });
    }
}
