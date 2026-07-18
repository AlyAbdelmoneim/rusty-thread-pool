use crate::worker::Worker;
use std::panic::{RefUnwindSafe, UnwindSafe, catch_unwind};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread;

type Job = Box<dyn FnOnce() + Send + 'static>;

fn thread_loop(i: u32, recv_mutex_clone: Arc<Mutex<Receiver<Job>>>) {
    loop {
        // The reason for this block is to drop the mutex guard immediately after
        // reading the closure
        let receieved_job = {
            let receiver = recv_mutex_clone.lock().unwrap();
            receiver.recv()
        };

        match receieved_job {
            Ok(job) => {
                match catch_unwind(std::panic::AssertUnwindSafe(job)) {
                    Ok(()) => {}
                    Err(..) => println!("Thread resurrected after panic"),
                }
                println!("Thread {} Finished", i);
            }
            Err(..) => {
                return;
            }
        }
    }
}

#[allow(unused)]
pub struct AbdoThreadPool {
    workers: Vec<Worker>,
    sender: Option<Sender<Job>>,
}

impl AbdoThreadPool {
    pub fn new(workers_num: u32) -> Self {
        let (sender, receiver): (Sender<Job>, Receiver<Job>) = channel();

        let recv_mutex = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::new();

        for i in 0..workers_num {
            let recv_mutex_clone = Arc::clone(&recv_mutex);
            let builder = thread::Builder::new().name(format!("{}", i));
            let handle = builder
                .spawn(move || {
                    thread_loop(i, recv_mutex_clone);
                })
                .unwrap();

            workers.push(Worker::new(i, handle));
        }
        Self {
            workers,
            sender: Some(sender),
        }
    }

    pub fn execute<F, T>(&self, closure: F) -> async_oneshot_channel::Receiver<T>
    where
        F: FnOnce() -> T + Send + 'static + UnwindSafe,
        T: Send + 'static + RefUnwindSafe,
    {
        let (oneshot_sender, oneshot_recv) = async_oneshot_channel::oneshot::<T>();

        let wrapped_job = move || {
            let result = closure();
            let _ = oneshot_sender.send(result);
        };

        let _ = self
            .sender
            .as_ref()
            .expect("Thread Pool Killed")
            .send(Box::new(wrapped_job));
        oneshot_recv
    }
}

impl Drop for AbdoThreadPool {
    fn drop(&mut self) {
        self.sender.take();
        for worker in &mut self.workers {
            let handle = worker.handle.take().unwrap();
            let _ = handle.join();
        }
    }
}
