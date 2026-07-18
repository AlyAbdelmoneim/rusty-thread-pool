use std::thread::JoinHandle;

#[allow(unused)]
pub struct Worker {
    pub id: u32,
    pub handle: Option<JoinHandle<()>>,
}

impl Worker {
    pub fn new(id: u32, handle: JoinHandle<()>) -> Self {
        Self {
            id,
            handle: Some(handle),
        }
    }
}
