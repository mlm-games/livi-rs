#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
use core::ffi::c_void;
use std::mem::size_of;
use std::slice;
use std::sync::{Arc, Mutex};

use ringbuf::traits::{Consumer, Observer, Producer, Split};

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct LV2_Worker_Interface {
    pub work: Option<
        unsafe extern "C" fn(
            instance: *mut c_void,
            respond: Option<
                unsafe extern "C" fn(handle: *mut c_void, size: u32, data: *const c_void) -> i32,
            >,
            handle: *mut c_void,
            size: u32,
            data: *const c_void,
        ) -> i32,
    >,

    pub work_response:
        Option<unsafe extern "C" fn(instance: *mut c_void, size: u32, body: *const c_void) -> i32>,

    pub end_run: Option<unsafe extern "C" fn(instance: *mut c_void) -> i32>,
}

#[repr(C)]
#[derive(Debug)]
pub struct LV2_Worker_Schedule {
    pub handle: *mut c_void,
    pub schedule_work:
        Option<unsafe extern "C" fn(handle: *mut c_void, size: u32, data: *const c_void) -> i32>,
}

// Worker status codes
pub type LV2_Worker_Status = i32;
pub const LV2_WORKER_SUCCESS: LV2_Worker_Status = 0;
pub const LV2_WORKER_ERR_UNKNOWN: LV2_Worker_Status = 1;
pub const LV2_WORKER_ERR_NO_SPACE: LV2_Worker_Status = 2;

// Type aliases
pub type LV2_Handle = *mut c_void;
pub type LV2_Worker_Schedule_Handle = *mut c_void;
pub type LV2_Worker_Respond_Handle = *mut c_void;

// Extension URIs
pub const LV2_WORKER__interface: &str = "http://lv2plug.in/ns/ext/worker#interface";
pub const LV2_WORKER__schedule: &[u8] = b"http://lv2plug.in/ns/ext/worker#schedule\0";

pub(crate) type WorkerMessageSender = ringbuf::HeapProd<u8>;
pub(crate) type WorkerMessageReceiver = ringbuf::HeapCons<u8>;

const MAX_MESSAGE_SIZE: usize = 8192;
const N_MESSAGES: usize = 4;

type MessageBody = [u8; MAX_MESSAGE_SIZE];

#[derive(Debug)]
struct WorkerMessage {
    size: usize,
    body: MessageBody,
}

impl WorkerMessage {
    fn data(&mut self) -> *mut c_void {
        &mut self.body as *mut MessageBody as *mut c_void
    }
}

pub(crate) fn instantiate_queue() -> (WorkerMessageSender, WorkerMessageReceiver) {
    let (sender, receiver) = ringbuf::HeapRb::new(MAX_MESSAGE_SIZE * N_MESSAGES).split();
    (sender, receiver)
}

fn publish_message(
    sender: &mut WorkerMessageSender,
    size: usize,
    body: *mut u8,
) -> LV2_Worker_Status {
    if size > MAX_MESSAGE_SIZE {
        return LV2_WORKER_ERR_NO_SPACE;
    }
    let mut body = unsafe { slice::from_raw_parts(body, size) };
    let total_size = size_of::<usize>() + size;
    if sender.vacant_len() < total_size {
        return LV2_WORKER_ERR_NO_SPACE;
    }
    let size_as_bytes = size.to_be_bytes();
    sender.push_slice(&size_as_bytes);
    let result = sender.read_from(&mut body, Some(size));
    match result {
        Some(_) => LV2_WORKER_SUCCESS,
        None => LV2_WORKER_ERR_UNKNOWN,
    }
}

fn pop_message(receiver: &mut WorkerMessageReceiver) -> WorkerMessage {
    let mut size_as_bytes = [0; size_of::<usize>()];
    receiver.pop_slice(&mut size_as_bytes);
    let size = usize::from_be_bytes(size_as_bytes);
    let mut body: MessageBody = [0; MAX_MESSAGE_SIZE];
    let mut slice = &mut body[..];
    receiver.write_into(&mut slice, Some(size));

    WorkerMessage { size, body }
}

pub extern "C" fn schedule_work(
    handle: LV2_Worker_Schedule_Handle,
    size: u32,
    body: *const c_void,
) -> LV2_Worker_Status {
    let sender = unsafe { &mut *(handle as *mut WorkerMessageSender) };
    publish_message(sender, size as usize, body as *mut u8)
}

extern "C" fn worker_respond(
    handle: LV2_Worker_Respond_Handle,
    size: u32,
    body: *const c_void,
) -> LV2_Worker_Status {
    let sender = unsafe { &mut *(handle as *mut WorkerMessageSender) };
    publish_message(sender, size as usize, body as *mut u8)
}

/// A plugin instance delegates non-realtime-safe work to a Worker
pub struct Worker {
    plugin_is_alive: Arc<Mutex<bool>>,
    interface: LV2_Worker_Interface,
    instance_handle: LV2_Handle,
    receiver: WorkerMessageReceiver,
    sender: WorkerMessageSender,
}

unsafe impl Send for Worker {}
unsafe impl Sync for Worker {}

impl Worker {
    pub(crate) fn new(
        plugin_is_alive: Arc<Mutex<bool>>,
        interface: LV2_Worker_Interface,
        instance_handle: LV2_Handle,
        receiver: WorkerMessageReceiver,
        sender: WorkerMessageSender,
    ) -> Self {
        Worker {
            plugin_is_alive,
            interface,
            instance_handle,
            receiver,
            sender,
        }
    }

    pub fn do_work(&mut self) {
        let plugin_is_alive = self.plugin_is_alive.lock().unwrap();
        while *plugin_is_alive && self.receiver.occupied_len() > size_of::<usize>() {
            let mut message = pop_message(&mut self.receiver);
            if let Some(work_function) = self.interface.work {
                let sender = &mut self.sender as *mut WorkerMessageSender as *mut c_void;
                unsafe {
                    work_function(
                        self.instance_handle,
                        Some(worker_respond),
                        sender,
                        message.size as u32,
                        message.data(),
                    );
                }
            }
        }
    }

    pub fn should_keep_working(&self) -> bool {
        *self.plugin_is_alive.lock().unwrap()
    }
}

impl std::fmt::Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker")
            .field("plugin_is_alive", &self.plugin_is_alive)
            .field("interface", &self.interface)
            .field("instance_handle", &self.instance_handle)
            .field("receiver", &"__internal__")
            .field("sender", &"__internal__")
            .finish()
    }
}

pub(crate) fn maybe_get_worker_interface(
    plugin: &lilv::plugin::Plugin,
    common_uris: &crate::CommonUris,
    instance: &mut lilv::instance::ActiveInstance,
) -> Option<LV2_Worker_Interface> {
    if !plugin.has_feature(&common_uris.worker_schedule_feature_uri) {
        return None;
    }

    Some(*unsafe {
        instance
            .instance()
            .extension_data::<LV2_Worker_Interface>(LV2_WORKER__interface)?
            .as_ref()
    })
}

pub(crate) fn handle_work_responses(
    worker_interface: &mut LV2_Worker_Interface,
    receiver: &mut WorkerMessageReceiver,
    handle: LV2_Handle,
) {
    while receiver.occupied_len() > size_of::<usize>() {
        let mut message = pop_message(receiver);
        if let Some(work_response_function) = worker_interface.work_response {
            unsafe { work_response_function(handle, message.size as u32, message.data()) };
        }
    }
}

pub(crate) fn end_run(worker_interface: &mut LV2_Worker_Interface, handle: LV2_Handle) {
    if let Some(end_function) = worker_interface.end_run {
        unsafe { end_function(handle) };
    }
}

/// Use a WorkerManager to own and run Workers
#[derive(Default, Debug)]
pub struct WorkerManager {
    new_workers: Mutex<Vec<Worker>>,
    running_workers: Mutex<Vec<Worker>>,
}

impl WorkerManager {
    pub fn run_workers(&self) {
        let mut workers = self.running_workers.lock().unwrap();
        workers.extend(self.new_workers.lock().unwrap().drain(..));
        workers.iter_mut().for_each(|worker| worker.do_work());
        workers.retain(|worker| worker.should_keep_working());
    }

    pub fn workers_count(&self) -> usize {
        self.running_workers.lock().unwrap().len() + self.new_workers.lock().unwrap().len()
    }

    pub(crate) fn add_worker(&self, worker: Worker) {
        self.new_workers.lock().unwrap().push(worker);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str;

    #[test]
    fn test_send() {
        let (mut sender, mut receiver) = instantiate_queue();
        let sentence_to_transfer = String::from("This is a message for you");
        let mut data = sentence_to_transfer.clone().into_bytes();
        publish_message(&mut sender, data.len(), data.as_mut_ptr());
        let message = pop_message(&mut receiver);
        let body = &message.body[..message.size];
        let message_body = str::from_utf8(body).unwrap();
        assert_eq!(sentence_to_transfer, message_body);
    }
}
