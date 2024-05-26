use anyhow::Result;
use serializer::{parse_resp_data, RespData};
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::env;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::time;
use time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::Sender;
use tokio::sync::{broadcast, Mutex};
mod serializer;

struct StoredValue {
    value: String,
    expiry: Option<Instant>,
}

struct ServerInfo {
    role: String,
    master_replid: String,
    master_repl_offset: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let mut port_to_use = 6379;
    let mut replica_info: Option<String> = None;

    let port_flag_position = args.iter().position(|s| s == "--port");
    if let Some(pos) = port_flag_position {
        port_to_use = args[pos + 1].parse().unwrap();
    }
    let replica_flag_position = args.iter().position(|s| s == "--replicaof");
    if let Some(pos) = replica_flag_position {
        replica_info = Some(args[pos + 1].clone());
    }

    // generate 40 character long random string for the master_replid
    let master_replid: String = get_random_string(40);
    let master_repl_offset = 0;

    let sock = format!("127.0.0.1:{}", port_to_use);

    // use an Arc<Mutex<HashMap>> to store the key-value pairs in memory across threads
    let storage: Arc<Mutex<HashMap<String, StoredValue>>> = Arc::new(Mutex::new(HashMap::new()));

    // use a broadcast channel to send messages to all connected replicas
    // each replica will have its own broadcast receiver and is maintained on a dedicated green
    // thread
    let (tx, _rc) = broadcast::channel::<Vec<u8>>(16);

    let role = match replica_info {
        Some(_) => "slave",
        None => "master",
    };
    if role == "slave" {
        let replica_info = replica_info.unwrap();
        let replica_info_split: Vec<&str> = replica_info.split(' ').collect();
        let master_ip = replica_info_split[0];
        let master_port = replica_info_split[1];
        perform_replica_master_handshake(master_ip, master_port, port_to_use).await?;
    }
    let server_info = Arc::new(Mutex::new(ServerInfo {
        role: role.to_string(),
        master_replid,
        master_repl_offset,
    }));
    let listener = TcpListener::bind(sock).await?;
    let sender_arc = Arc::new(tx);

    loop {
        let (stream, _) = listener.accept().await?;
        let storage = storage.clone();
        let server_info = server_info.clone();
        let sender = sender_arc.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, storage, server_info, sender).await {
                eprintln!("Error handling connection: {}", e);
            }
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    storage: Arc<Mutex<HashMap<String, StoredValue>>>,
    server_info: Arc<Mutex<ServerInfo>>,
    sender: Arc<Sender<Vec<u8>>>,
) -> Result<()> {
    // Primer on sockets: https://docs.python.org/3/howto/sockets.html
    let mut buf = [0; 512];
    let mut accumulated_data = Vec::new();

    loop {
        match stream.read(&mut buf).await {
            Ok(0) => {
                // 0 bytes read so connection is closed
                return Ok(());
            }
            Ok(_n) => {
                accumulated_data.extend_from_slice(&buf);
                let parse_result = parse_resp_data(&buf);
                if parse_result.is_err() {
                    continue;
                }
                let (resp, bytes_read) = parse_result.unwrap();
                accumulated_data = accumulated_data[bytes_read..].to_vec();
                let commands_list = resp.serialize_to_list_of_strings(true);
                match commands_list[0].as_str() {
                    "echo" => {
                        let echo_resp = RespData::unpack_array(&resp);
                        let serialized_resp = echo_resp[1].serialize_to_redis_protocol();
                        stream.write_all(serialized_resp.as_bytes()).await?;
                    }
                    "ping" => {
                        stream.write_all("+PONG\r\n".as_bytes()).await?;
                    }
                    "set" => {
                        handle_set_command(
                            &mut stream,
                            storage.clone(),
                            sender.clone(),
                            &buf,
                            bytes_read,
                            resp,
                        )
                        .await?;
                    }
                    "get" => {
                        handle_get_command(&mut stream, storage.clone(), &resp).await?;
                    }
                    "info" => {
                        // ex: redis-cli INFO replication
                        // still need to actually parse the argument but this works for now
                        let server_info = server_info.lock().await;
                        let role = &server_info.role;

                        let replication_info = format!(
                            "role:{}\nmaster_replid:{}\nmaster_repl_offset:{}",
                            role, server_info.master_replid, server_info.master_repl_offset
                        );
                        let info_resp_response = RespData::BulkString(replication_info);
                        stream
                            .write_all(info_resp_response.serialize_to_redis_protocol().as_bytes())
                            .await?;
                    }
                    "replconf" => {
                        let replconf_resp = RespData::unpack_array(&resp);
                        let _subcommand =
                            replconf_resp[1].serialize_to_list_of_strings(false)[0].clone();
                        let ok_response = RespData::SimpleString("OK".to_string());
                        stream
                            .write_all(ok_response.serialize_to_redis_protocol().as_bytes())
                            .await?;
                    }
                    "psync" => {
                        handle_psync_command(
                            &mut stream,
                            server_info.clone(),
                            sender.clone(),
                            &resp,
                        )
                        .await?;
                    }
                    _ => {
                        stream
                            .write_all("-ERR unknown command\r\n".as_bytes())
                            .await?;
                    }
                }
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }
}

async fn perform_replica_master_handshake(
    master_ip: &str,
    master_port: &str,
    port_to_use: usize,
) -> Result<()> {
    // connect to the master using the provided IP and
    let mut stream = TcpStream::connect(format!("{}:{}", master_ip, master_port)).await?;

    // send the initial PING command to the master
    let ping_redis_command = RespData::Array(vec![RespData::SimpleString("PING".to_string())]);
    stream
        .write_all(ping_redis_command.serialize_to_redis_protocol().as_bytes())
        .await?;
    stream.flush().await?;
    // wait for the PONG response from the master
    let n = stream.read(&mut [0; 512]).await?;
    if n == 0 {
        return Err(anyhow::anyhow!("Connection closed on master"));
    }
    // send REPLCONF listening-port <port> command to the master as an array of bulk strings
    let listening_port_command = RespData::Array(vec![
        RespData::BulkString("REPLCONF".to_string()),
        RespData::BulkString("listening-port".to_string()),
        RespData::BulkString(port_to_use.to_string()),
    ]);
    stream
        .write_all(
            listening_port_command
                .serialize_to_redis_protocol()
                .as_bytes(),
        )
        .await?;
    // flush writes across the network individually
    stream.flush().await?;
    // wait for the reply from the master
    let n = stream.read(&mut [0; 512]).await?;
    if n == 0 {
        return Err(anyhow::anyhow!("Connection closed on master"));
    }

    // send REPLCONF capa psync2 command to the master as an array of bulk strings
    let capa_command = RespData::Array(vec![
        RespData::BulkString("REPLCONF".to_string()),
        RespData::BulkString("capa".to_string()),
        RespData::BulkString("psync2".to_string()),
    ]);
    stream
        .write_all(capa_command.serialize_to_redis_protocol().as_bytes())
        .await?;
    stream.flush().await?;
    // wait for the final reply from the master
    let n = stream.read(&mut [0; 512]).await?;
    if n == 0 {
        return Err(anyhow::anyhow!("Connection closed on master"));
    }

    // lastly, send the PSYNC command to the master
    let psync_command = RespData::Array(vec![
        RespData::BulkString("PSYNC".to_string()),
        RespData::BulkString("?".to_string()),
        RespData::BulkString("-1".to_string()),
    ]);
    stream
        .write_all(psync_command.serialize_to_redis_protocol().as_bytes())
        .await?;
    stream.flush().await?;
    let n = stream.read(&mut [0; 512]).await?;
    if n == 0 {
        return Err(anyhow::anyhow!("Connection closed on master"));
    }
    Ok(())
}

async fn handle_get_command(
    stream: &mut TcpStream,
    storage: Arc<Mutex<HashMap<String, StoredValue>>>,
    resp: &RespData,
) -> Result<()> {
    let get_resp = RespData::unpack_array(resp);
    let key_str = get_resp[1].serialize_to_list_of_strings(false)[0].clone();
    let held_storage = storage.lock().await;
    match held_storage.get(&key_str) {
        Some(stored_value)
            if stored_value.expiry.is_none() || stored_value.expiry.unwrap() > Instant::now() =>
        {
            let string_value = stored_value.value.clone();
            let resp_response = RespData::SimpleString(string_value);
            stream
                .write_all(resp_response.serialize_to_redis_protocol().as_bytes())
                .await?;
        }
        // Key has either expired or never existed in the map.
        _ => {
            println!("Key not found");
            stream.write_all("$-1\r\n".as_bytes()).await?;
        }
    }
    Ok(())
}

async fn handle_set_command(
    stream: &mut TcpStream,
    storage: Arc<Mutex<HashMap<String, StoredValue>>>,
    sender: Arc<Sender<Vec<u8>>>,
    buf: &[u8],
    bytes_read: usize,
    resp: RespData,
) -> Result<()> {
    let set_resp = RespData::unpack_array(&resp);
    // the second element in the array is the key
    let key_str = set_resp[1].serialize_to_list_of_strings(false)[0].clone();
    // the third element in the array is the value
    let value_str = set_resp[2].serialize_to_list_of_strings(false)[0].clone();
    // px argument is being provided
    if set_resp.len() > 3 {
        let arg = set_resp[3].serialize_to_list_of_strings(true)[0].clone();
        if arg == "px" {
            let expiry_milliseconds: usize = set_resp[4].serialize_to_list_of_strings(false)[0]
                .clone()
                .parse()?;
            let expiry = Instant::now() + Duration::from_millis(expiry_milliseconds as u64);
            let mut held_storage = storage.lock().await;
            held_storage.insert(
                key_str,
                StoredValue {
                    value: value_str,
                    expiry: Some(expiry),
                },
            );
        } else {
            return Err(anyhow::anyhow!("Invalid argument"));
        }
    } else {
        let mut held_storage = storage.lock().await;
        held_storage.insert(
            key_str,
            StoredValue {
                value: value_str,
                expiry: None,
            },
        );
    }
    // return OK as a simple string
    stream.write_all("+OK\r\n".as_bytes()).await?;
    // send the same bytes that we read for this command to all of the
    // connected replicas. Since the replicas run the same code they will
    // parse the command the same way so we don't need to re-serialize the
    // payload for them.
    //
    // We can't directly send the contents of our own buffer because it will contain a bunch of
    // null byte padding at the end. Instead, we need to send the exact bytes
    // that we actually read to construct the message
    let buf_up_to_bytes_read = &buf[..bytes_read];
    sender.send(buf_up_to_bytes_read.to_vec())?;
    Ok(())
}

async fn handle_psync_command(
    stream: &mut TcpStream,
    server_info: Arc<Mutex<ServerInfo>>,
    sender: Arc<Sender<Vec<u8>>>,
    resp: &RespData,
) -> Result<()> {
    let psync_resp = RespData::unpack_array(resp);
    let _replication_id = psync_resp[1].serialize_to_list_of_strings(false)[0].clone();
    let _offset = psync_resp[2].serialize_to_list_of_strings(false)[0].clone();
    // Respond with +FULLRESYNC <replid> <offset>
    let server_info = server_info.lock().await;
    let full_resync_response = RespData::SimpleString(format!(
        "+FULLRESYNC {} {}",
        server_info.master_replid, server_info.master_repl_offset
    ));
    stream
        .write_all(
            full_resync_response
                .serialize_to_redis_protocol()
                .as_bytes(),
        )
        .await?;
    stream.flush().await?;
    // lastly, send an empty RDB file back to the replica
    let hardcoded_empty_rdb_file_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
    let binary_empty_rdb = decode_hex_string(hardcoded_empty_rdb_file_hex)?;
    let len = binary_empty_rdb.len();
    stream.write_all(format!("${}\r\n", len).as_bytes()).await?;
    stream.write_all(&binary_empty_rdb).await?;
    // create a new receiver for the replica command channel
    let mut replica_rx = sender.subscribe();
    loop {
        let replica_command = replica_rx.recv().await?;
        // whenever a message is received from the broadcast channel, simply forward it to the replica
        stream.write_all(&replica_command).await?;
        // make sure messages are sent to the replica immediately
        stream.flush().await?;
    }
}

fn get_random_string(len: usize) -> String {
    let mut random_string = String::new();
    for _ in 0..len {
        let random_value = RandomState::new().build_hasher().finish() as usize;
        random_string.push_str(&random_value.to_string());
    }
    random_string
}

fn decode_hex_string(hex: &str) -> Result<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return Err(anyhow::anyhow!("Invalid hex string length"));
    }

    let mut binary_data = Vec::new();

    for i in (0..hex.len()).step_by(2) {
        let byte_str = &hex[i..i + 2];
        let byte = u8::from_str_radix(byte_str, 16)?;
        binary_data.push(byte);
    }

    Ok(binary_data)
}
