use crate::worker::Worker;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread;

type Job = Box<dyn FnOnce() + Send + 'static>;

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
                    loop {
                        // The reason for this block is to drop the mutex guard immediately after
                        // reading the closure
                        let closure = {
                            let receiver = recv_mutex_clone.lock().unwrap();
                            receiver.recv()
                        };

                        match closure {
                            Ok(job) => {
                                job();
                                println!("Thread {} finished", i);
                            }
                            _ => break,
                        }
                    }
                })
                .unwrap();
            workers.push(Worker::new(i, handle));
        }
        Self {
            workers,
            sender: Some(sender),
        }
    }

    pub fn execute<F>(&self, closure: F)
    where
        F: FnOnce() + Send + 'static,
    {
        // closure -> box(closure) -> send to threads
        let _ = self.sender.clone().take().unwrap().send(Box::new(closure));
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
