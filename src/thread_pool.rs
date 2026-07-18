use crate::worker::Worker;
use std::panic::UnwindSafe;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread;

type JobFn<T> = Box<dyn FnOnce() -> T + Send + 'static + UnwindSafe>;
pub struct Job<T> {
    pub job: JobFn<T>,
    pub oneshot_sender: async_oneshot_channel::Sender<T>,
}

impl<T> Job<T> {
    pub fn new(job: JobFn<T>, oneshot_sender: async_oneshot_channel::Sender<T>) -> Self {
        Self {
            job,
            oneshot_sender,
        }
    }
}

fn thread_loop<T>(i: u32, recv_mutex_clone: Arc<Mutex<Receiver<Job<T>>>>) {
    loop {
        // The reason for this block is to drop the mutex guard immediately after
        // reading the closure
        let closure = {
            let receiver = recv_mutex_clone.lock().unwrap();
            receiver.recv()
        };

        match closure {
            Ok(job) => {
                let sender = job.oneshot_sender;
                let job_fn = job.job;
                let result = job_fn();
                let _ = sender.send(result);
                println!("Thread {} Finished", i);
            }
            Err(..) => {
                return;
            }
        }
    }
}

#[allow(unused)]
pub struct AbdoThreadPool<T> {
    workers: Vec<Worker>,
    sender: Option<Sender<Job<T>>>,
}

impl<T> AbdoThreadPool<T>
where
    T: 'static + Sync + Send,
{
    pub fn new(workers_num: u32) -> Self {
        let (sender, receiver): (Sender<Job<T>>, Receiver<Job<T>>) = channel();

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

    pub async fn execute(&self, closure: JobFn<T>) -> T {
        // closure -> box(closure) -> send to thread
        // first: create one-shot channel
        let (oneshot_sender, oneshot_recv) = async_oneshot_channel::oneshot::<T>();

        // second : create Job
        let job = Job::new(closure, oneshot_sender);

        // send it in the mpsc
        let _ = self
            .sender
            .as_ref()
            .expect("Thread Pool Closed ")
            .send(job)
            .unwrap();

        oneshot_recv.recv().await.take().unwrap()
    }
}

impl<T> Drop for AbdoThreadPool<T> {
    fn drop(&mut self) {
        self.sender.take();
        for worker in &mut self.workers {
            let handle = worker.handle.take().unwrap();
            let _ = handle.join();
        }
    }
}
