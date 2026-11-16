//! Bounded out-of-band fixture control for a genuine original F2 backend call.
//! This thread only releases a barrier; it never applies or retries a batch.
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
    sync::{Arc, Condvar, Mutex},
    thread::JoinHandle,
};
use storage::{
    ConsensusBackend, ConsensusFjallStore, Key, Mutation, StorageError, Value, WriteBatch,
};

#[derive(Default)]
struct State {
    index: Option<u64>,
    entered: bool,
    released: bool,
}
#[derive(Default)]
pub(super) struct Gate {
    state: Mutex<State>,
    changed: Condvar,
}
pub(super) struct Server {
    thread: JoinHandle<()>,
    pub endpoint: SocketAddr,
}
impl Server {
    pub fn bind() -> (Self, Arc<Gate>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let gate = Arc::new(Gate::default());
        let control = gate.clone();
        let thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            loop {
                let mut line = Vec::new();
                loop {
                    let mut byte = [0];
                    stream.read_exact(&mut byte).unwrap();
                    if byte[0] == b'\n' {
                        break;
                    }
                    assert!(line.len() < 32);
                    line.push(byte[0]);
                }
                let line = std::str::from_utf8(&line).unwrap();
                let reply = if let Some(index) = line.strip_prefix("arm ") {
                    let mut state = control.state.lock().unwrap();
                    assert!(state.index.is_none());
                    state.index = Some(index.parse().unwrap());
                    state.entered = false;
                    state.released = false;
                    "armed\n"
                } else {
                    match line {
                        "wait" => {
                            let mut state = control.state.lock().unwrap();
                            while !state.entered {
                                state = control.changed.wait(state).unwrap();
                            }
                            "held\n"
                        }
                        "release" => {
                            let mut state = control.state.lock().unwrap();
                            assert!(state.entered);
                            state.released = true;
                            control.changed.notify_all();
                            "released\n"
                        }
                        "stop" => {
                            stream.write_all(b"stopped\n").unwrap();
                            break;
                        }
                        _ => panic!("unknown physical gate command"),
                    }
                };
                stream.write_all(reply.as_bytes()).unwrap();
                stream.flush().unwrap();
            }
        });
        (Self { thread, endpoint }, gate)
    }
    pub fn join(self) {
        self.thread.join().unwrap();
    }
}
pub(super) struct Backend {
    pub inner: ConsensusFjallStore,
    pub gate: Arc<Gate>,
}
impl ConsensusBackend for Backend {
    fn get(&self, k: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(k)
    }
    fn scan_from(&self, p: &[u8], s: &[u8], n: usize) -> Result<Vec<(Key, Value)>, StorageError> {
        self.inner.scan_from(p, s, n)
    }
    fn apply(&mut self, b: WriteBatch) -> Result<(), StorageError> {
        {
            let mut state = self.gate.state.lock().unwrap();
            if let Some(index) = state.index
                && b.mutations().iter().any(|mutation| match mutation {
                    Mutation::Put { key, .. } => {
                        key.first() == Some(&0xf2)
                            && key.len() > 9
                            && key[key.len() - 9] == 2
                            && key.ends_with(&index.to_be_bytes())
                    }
                    Mutation::Delete { .. } => false,
                })
            {
                state.index = None;
                state.entered = true;
                self.gate.changed.notify_all();
                while !state.released {
                    state = self.gate.changed.wait(state).unwrap();
                }
            }
        }
        self.inner.apply(b)
    }
}
