use std::collections::HashMap;
use std::sync::Arc;
use std::time;
use time::Instant;
use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

use crate::serializer::RespData;

pub type StreamType = Vec<(String, Vec<(String, String)>)>;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    String(String),
    // a stream consists of multiple entires which in turn consist of multiple key-value pairs
    Stream(StreamType),
}

pub enum StreamEntryResult {
    ErrorMessage(String),
    EntryId(String),
}

pub struct StoredValue {
    pub value: Value,
    pub expiry: Option<Instant>,
}

pub struct TransactionData {
    pub commands: Vec<(String, RespData, Vec<u8>)>, // stores SET <serialized command> <raw bytes>
    pub in_transaction: bool,
}

impl TransactionData {
    pub fn new() -> TransactionData {
        TransactionData {
            commands: Vec::new(),
            in_transaction: false,
        }
    }
}

pub struct ServerInfo {
    pub role: String,
    pub master_replid: String,
    pub master_repl_offset: usize,
    // After the master-replica handshake is complete, a replica should only send responses to REPLCONF GETACK commands.
    // All other propagated commands (like PING, SET etc.) should be read and processed, but a response should not be sent back to the master.
    pub handshake_with_master_complete: bool,
    pub num_command_bytes_processed_as_replica: usize,
    // For counting purposes we treat this as the number of replicas that have finished the handshake
    pub replica_addresses: Vec<String>,
    pub rdb_file_dir: Option<String>,
    pub rdb_file_name: Option<String>,
    pub rdb_file_key_value_mapping: Option<HashMap<String, String>>,
}

pub struct ServerMessageChannels {
    pub sender: Arc<Sender<Vec<u8>>>,
    pub wait_tx: Arc<Mutex<mpsc::Sender<u64>>>,
    pub wait_rc: Arc<Mutex<mpsc::Receiver<u64>>>,
}
