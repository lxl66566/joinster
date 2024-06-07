use std::sync::{Arc, Condvar, Mutex};

#[derive(Clone, Debug)]
pub struct Signal {
    value: Arc<(Mutex<usize>, Condvar)>,
}

impl Signal {
    pub fn new(val: usize) -> Self {
        Signal {
            value: Arc::new((Mutex::new(val), Condvar::new())),
        }
    }

    pub fn wait(&self) {
        let (lock, cvar) = &*self.value;
        let mut value = lock.lock().unwrap();
        while *value == 0 {
            value = cvar.wait(value).unwrap();
        }
    }

    pub fn notify_one(&self) {
        let (lock, cvar) = &*self.value;
        let mut value = lock.lock().unwrap();
        *value += 1;
        cvar.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn test_signal() {
        let signal = Signal::new(0);
        let signal_clone = signal.clone();
        let recorder = Box::leak(Box::new(Arc::new(Mutex::new(0))));
        let recorder_clone = recorder.clone();

        let handle = thread::spawn(move || {
            signal_clone.wait();
            *recorder.lock().unwrap() += 1;
            assert_eq!(*signal_clone.value.0.lock().unwrap(), 1);
        });
        thread::sleep(std::time::Duration::from_millis(10));
        assert_eq!(*recorder_clone.lock().unwrap(), 0);
        signal.notify_one();
        handle.join().unwrap();
        assert_eq!(*recorder_clone.lock().unwrap(), 1);
    }
}
