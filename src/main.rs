mod thread_pool;
mod worker;
use crate::thread_pool::AbdoThreadPool;
//use std::sync::Arc;
//use std::sync::Mutex;
//use std::sync::mpsc::channel;
//use threadpool::ThreadPool;
//use std::rc::Rc;

fn my_func(b: i32) -> i32 {
    b
}

#[tokio::main]
async fn main() {
    let n_jobs = 8;

    let pool = AbdoThreadPool::new(8);

    let mut sum = 0;

    for _ in 0..n_jobs {
        let x = pool.execute(Box::new(|| my_func(8))).recv().await.unwrap();
        sum += x;
    }

    println!("Total sum : {}", sum);
}
