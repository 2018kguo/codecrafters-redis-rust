use anyhow::Result;
use serializer::{parse_resp_data, RespData};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time;
use time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::Sender;
use tokio::sync::{broadcast, Mutex};
mod serializer;
mod utils;

struct StoredValue {
    value: String,
    expiry: Option<Instant>,
}

struct ServerInfo {
    role: String,
    master_replid: String,
    master_repl_offset: usize,
    // After the master-replica handshake is complete, a replica should only send responses to REPLCONF GETACK commands.
    // All other propagated commands (like PING, SET etc.) should be read and processed, but a response should not be sent back to the master.
    handshake_with_master_complete: bool,
    num_command_bytes_processed_as_replica: usize,
    // For counting purposes we treat this as the number of replicas that have finished the handshake
    num_connected_replicas: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let mut port_to_use: usize = 6379;
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
    let master_replid: String = utils::get_random_string(40);
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

    let server_info = Arc::new(Mutex::new(ServerInfo {
        role: role.to_string(),
        master_replid,
        master_repl_offset,
        handshake_with_master_complete: false,
        num_command_bytes_processed_as_replica: 0,
        num_connected_replicas: 0,
    }));

    let listener = TcpListener::bind(sock).await?;
    let sender_arc = Arc::new(tx);
    let sender_arc_listener_clone = Arc::clone(&sender_arc);
    let storage_listener_clone = Arc::clone(&storage);
    let server_info_listener_clone = Arc::clone(&server_info);

    // !! In terms of control flow, we need the listener loop to run ASAP and we then optionally
    // !! perform the replica handshake. If this isn't respected then you run into situations where
    // !! one end of the connection isn't ready to accept incoming connections in time.
    // !! I haven't figured out a way to avoid this race condition other than doing this if/else
    // !! where if we're a master than we immediatelly enter the listener loop in a blocking manner
    // !! without waiting for tokio to spawn and execute loop in a green thread.
    if role == "slave" {
        let listener_task = tokio::spawn(async move {
            println!("Listening");
            loop {
                println!("Waiting for connection");
                let (stream, _) = listener.accept().await.unwrap();
                println!("Connection accepted");
                let storage = storage_listener_clone.clone();
                let server_info = server_info_listener_clone.clone();
                let sender = sender_arc_listener_clone.clone();
                handle_connection(stream, storage, server_info, sender)
                    .await
                    .unwrap();
            }
        });
        println!("Starting replica handshake");
        let replica_info = replica_info.unwrap();
        let replica_info_split: Vec<&str> = replica_info.split(' ').collect();
        let master_ip = replica_info_split[0];
        let master_port = replica_info_split[1];
        if let Err(e) = perform_replica_master_handshake(
            master_ip,
            master_port,
            port_to_use,
            storage.clone(),
            server_info.clone(),
            sender_arc.clone(),
        )
        .await
        {
            eprintln!("Error performing replica master handshake: {}", e);
        }

        // Wait for the listener task to complete (this will run indefinitely unless interrupted)
        listener_task.await?;
    } else {
        loop {
            let (stream, _) = listener.accept().await?;
            let storage: Arc<Mutex<HashMap<String, StoredValue>>> = storage.clone();
            let server_info = server_info.clone();
            let sender = sender_arc.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, storage, server_info, sender).await {
                    eprintln!("Error handling connection: {}", e);
                }
            });
        }
    }
    Ok(())
}

async fn handle_connection(
    mut stream: TcpStream,
    storage: Arc<Mutex<HashMap<String, StoredValue>>>,
    server_info: Arc<Mutex<ServerInfo>>,
    sender: Arc<Sender<Vec<u8>>>,
) -> Result<()> {
    // Primer on sockets: https://docs.python.org/3/howto/sockets.html
    let mut buf = [0; 512];
    let mut local_buffer = Vec::new();

    loop {
        match stream.read(&mut buf).await {
            Ok(0) => {
                println!("Connection closed");
                // 0 bytes read so connection is closed
                return Ok(());
            }
            Ok(n) => {
                local_buffer.extend_from_slice(&buf[..n]);
                process_local_buffer(
                    &mut stream,
                    &mut local_buffer,
                    storage.clone(),
                    server_info.clone(),
                    sender.clone(),
                )
                .await?;
            }
            Err(e) => {
                println!("Error reading from socket: {}", e);
                return Err(e.into());
            }
        }
    }
}

async fn read_and_handle_single_command_from_local_buffer(
    stream: &mut TcpStream,
    storage: Arc<Mutex<HashMap<String, StoredValue>>>,
    server_info: Arc<Mutex<ServerInfo>>,
    sender: Arc<Sender<Vec<u8>>>,
    local_buffer: &mut Vec<u8>,
) -> Result<()> {
    let (
        role,
        master_replid,
        master_repl_offset,
        handshake_with_master_complete,
        num_command_bytes_processed_as_replica,
    ) = {
        let server_info = server_info.lock().await;
        (
            server_info.role.clone(),
            server_info.master_replid.clone(),
            server_info.master_repl_offset,
            server_info.handshake_with_master_complete,
            server_info.num_command_bytes_processed_as_replica,
        )
    };
    let is_replica = role == "slave";
    let (resp, bytes_read) = parse_resp_data(local_buffer)?;

    // pop the bytes that we've already read for this message from our local buffer
    let message_bytes = local_buffer[..bytes_read].to_vec();
    *local_buffer = local_buffer[bytes_read..].to_vec();

    let commands_list = resp.serialize_to_list_of_strings(true);
    match commands_list[0].as_str() {
        "echo" => {
            let echo_resp = RespData::unpack_array(&resp);
            let serialized_resp = echo_resp[1].serialize_to_redis_protocol();
            stream.write_all(serialized_resp.as_bytes()).await?;
        }
        "ping" => {
            // increment the number of command bytes processed as a replica
            if is_replica {
                let mut server_info = server_info.lock().await;
                server_info.num_command_bytes_processed_as_replica += bytes_read;
            }
            if !handshake_with_master_complete {
                stream.write_all("+PONG\r\n".as_bytes()).await?;
            }
        }
        "set" => {
            handle_set_command(
                stream,
                storage.clone(),
                sender.clone(),
                &message_bytes,
                resp,
                is_replica,
                handshake_with_master_complete,
                server_info.clone(),
            )
            .await?;
        }
        "get" => {
            handle_get_command(stream, storage.clone(), &resp).await?;
        }
        "info" => {
            // ex: redis-cli INFO replication
            // still need to actually parse the argument but this works for now
            let server_info = server_info.lock().await;
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
            let _subcommand = replconf_resp[1].serialize_to_list_of_strings(false)[0].clone();
            if _subcommand.to_lowercase() == "getack" {
                // i.ie REPLCONF GETACK *
                if !is_replica {
                    return Err(anyhow::anyhow!("GETACK command not expected for masters"));
                }
                let num_command_bytes_str = num_command_bytes_processed_as_replica.to_string();
                // for now just respond with REPLCONF ACK 0 as a RESP Array of bulk strings
                let ack_response = RespData::Array(vec![
                    RespData::BulkString("REPLCONF".to_string()),
                    RespData::BulkString("ACK".to_string()),
                    RespData::BulkString(num_command_bytes_str),
                ]);
                // increment the number of command bytes processed as a replica
                {
                    let mut server_info = server_info.lock().await;
                    server_info.num_command_bytes_processed_as_replica += bytes_read;
                }
                stream
                    .write_all(ack_response.serialize_to_redis_protocol().as_bytes())
                    .await?;
            } else {
                let ok_response = RespData::SimpleString("OK".to_string());
                stream
                    .write_all(ok_response.serialize_to_redis_protocol().as_bytes())
                    .await?;
            }
        }
        "psync" => {
            if is_replica {
                return Err(anyhow::anyhow!("PSYNC command not expected for replicas"));
            }
            // This spawns a loop where we will keep reading from the replica command receiver
            // and forwarding the commands to the replica that we're currently replying to.
            // We need to make sure we aren't holding any locks before entering this loop
            // because that will limit us to only being able to handle 1 replica at a time
            // since the other replicas will be forever blocked on acquiring the lock.
            handle_psync_command(
                stream,
                sender.clone(),
                &resp,
                master_replid,
                master_repl_offset,
                server_info.clone(),
            )
            .await?;
        }
        "wait" => {
            // Just respond with 0 (as a RESP Integer) for now. In the test there are no connected replicas
            let num_connected_replicas = {
                let server_info = server_info.lock().await;
                server_info.num_connected_replicas
            };
            let integer_resp = RespData::Integer(num_connected_replicas as isize);
            stream
                .write_all(integer_resp.serialize_to_redis_protocol().as_bytes())
                .await?;
        }
        _ => {
            println!("Unknown command");
            // slaves receive the FULLRESYNC command from the master
            // as well as the RDB file which we don't want to respond to with an error
            // so just ignore unknown commands for slaves for now
            if role == "master" {
                stream
                    .write_all("-ERR unknown command\r\n".as_bytes())
                    .await?;
            }
        }
    }
    Ok(())
}

async fn process_local_buffer(
    stream: &mut tokio::net::TcpStream,
    local_buffer: &mut Vec<u8>,
    storage: Arc<Mutex<HashMap<String, StoredValue>>>,
    server_info: Arc<Mutex<ServerInfo>>,
    sender: Arc<Sender<Vec<u8>>>,
) -> Result<()> {
    // Multiple commands can get fetched in a single read from the TCP stream, so we need to make sure
    // we handle all of the messages inside our local buffer because otherwise we'll only process 1 command
    // per read and the rest will be left in the buffer.
    loop {
        // if local_buffer is purely null bytes, end the loop
        if local_buffer.iter().all(|&x| x == 0) {
            local_buffer.clear();
            return Ok(());
        }
        read_and_handle_single_command_from_local_buffer(
            stream,
            storage.clone(),
            server_info.clone(),
            sender.clone(),
            local_buffer,
        )
        .await?;
    }
}

async fn perform_replica_master_handshake(
    master_ip: &str,
    master_port: &str,
    port_to_use: usize,
    storage: Arc<Mutex<HashMap<String, StoredValue>>>,
    server_info: Arc<Mutex<ServerInfo>>,
    sender: Arc<Sender<Vec<u8>>>,
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
    println!("Finished handshake");
    stream
        .write_all(psync_command.serialize_to_redis_protocol().as_bytes())
        .await?;
    stream.flush().await?;
    // set the handshake_with_master_complete flag to true
    {
        let mut server_info = server_info.lock().await;
        server_info.handshake_with_master_complete = true;
    }

    // We need to handle commands sent back on the same outgoing TCP connection from the stream to the master.
    // This traffic isn't directed to the main listener loop, so we need to handle it here.
    // We expect to receive a FULLRESYNC command from the master followed by an RDB file, and then potentially
    // more commands.
    handle_connection(stream, storage, server_info, sender).await?;
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
    message_bytes: &[u8],
    resp: RespData,
    is_replica: bool,
    handshake_with_master_complete: bool,
    server_info: Arc<Mutex<ServerInfo>>,
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
    // increment the number of command bytes processed as a replica
    if is_replica {
        let mut server_info = server_info.lock().await;
        server_info.num_command_bytes_processed_as_replica += message_bytes.len();
    }
    // return OK as a simple string
    if !handshake_with_master_complete {
        stream.write_all("+OK\r\n".as_bytes()).await?;
    }
    // send the same bytes that we read for this command to all of the
    // connected replicas. Since the replicas run the same code they will
    // parse the command the same way so we don't need to re-serialize the
    // payload for them.
    //
    // We can't directly send the contents of our own buffer because it will contain a bunch of
    // null byte padding at the end. Instead, we need to send the exact bytes
    // that we actually read to construct the message
    if !is_replica {
        sender.send(message_bytes.to_vec())?;
    }
    Ok(())
}

async fn handle_psync_command(
    stream: &mut TcpStream,
    sender: Arc<Sender<Vec<u8>>>,
    resp: &RespData,
    master_replid: String,
    master_repl_offset: usize,
    server_info: Arc<Mutex<ServerInfo>>,
) -> Result<()> {
    let psync_resp = RespData::unpack_array(resp);
    let _replication_id = psync_resp[1].serialize_to_list_of_strings(false)[0].clone();
    let _offset = psync_resp[2].serialize_to_list_of_strings(false)[0].clone();

    let full_resync_response = RespData::SimpleString(format!(
        "+FULLRESYNC {} {}",
        master_replid, master_repl_offset
    ));
    stream
        .write_all(
            full_resync_response
                .serialize_to_redis_protocol()
                .as_bytes(),
        )
        .await?;
    stream.flush().await?;

    // increment the number of connected replicas
    {
        let mut server_info = server_info.lock().await;
        server_info.num_connected_replicas += 1;
    }
    // lastly, send an empty RDB file back to the replica
    let hardcoded_empty_rdb_file_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
    let binary_empty_rdb = utils::decode_hex_string(hardcoded_empty_rdb_file_hex)?;
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
