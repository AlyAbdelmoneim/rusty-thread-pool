mod thread_pool;
mod worker;
use std::{thread, time::Duration};

use crate::thread_pool::AbdoThreadPool;
//use std::sync::Arc;
//use std::sync::Mutex;
//use std::sync::mpsc::channel;
//use threadpool::ThreadPool;
//use std::rc::Rc;

fn my_func(b: i32) {
    thread::sleep(Duration::from_secs(1));
    println!("started with {}", b);
}

fn poisonous_fn() {
    panic!("oops");
}

fn main() {
    let n_jobs = 8;

    let pool = AbdoThreadPool::new(8);

    for i in 0..n_jobs {
        if i % 2 == 0 {
            pool.execute(move || my_func(12));
        } else {
            pool.execute(move || poisonous_fn());
        }
    }
}
