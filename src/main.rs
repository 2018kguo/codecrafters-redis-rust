use anyhow::Result;
use serializer::{parse_resp_data, RespData};
use std::collections::HashMap;
use std::sync::Arc;
use std::time;
use std::{env, io};
use time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::Sender;
use tokio::sync::{broadcast, Mutex};
use tokio::time::sleep;
mod serializer;
mod utils;
use tokio::sync::mpsc;

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
    replica_addresses: Vec<String>,
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
    let (wait_tx, wait_rc) = mpsc::channel::<u64>(64);

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
        replica_addresses: Vec::new(),
    }));

    let listener = TcpListener::bind(sock).await?;
    let sender_arc = Arc::new(tx);
    let sender_arc_listener_clone = Arc::clone(&sender_arc);
    let storage_listener_clone = Arc::clone(&storage);
    let server_info_listener_clone = Arc::clone(&server_info);
    let wait_tx_arc = Arc::new(Mutex::new(wait_tx));
    let wait_rc_arc = Arc::new(Mutex::new(wait_rc));
    let wait_tx_arc_clone = Arc::clone(&wait_tx_arc);
    let wait_rc_arc_clone = Arc::clone(&wait_rc_arc);

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
                let wait_tx = wait_tx_arc_clone.clone();
                let wait_rc = wait_rc_arc_clone.clone();
                handle_connection(stream, storage, server_info, sender, wait_tx, wait_rc)
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
            wait_tx_arc.clone(),
            wait_rc_arc.clone(),
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
            let wait_tx = wait_tx_arc.clone();
            let wait_rc = wait_rc_arc.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    handle_connection(stream, storage, server_info, sender, wait_tx, wait_rc).await
                {
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
    wait_tx: Arc<Mutex<mpsc::Sender<u64>>>,
    wait_rc: Arc<Mutex<mpsc::Receiver<u64>>>,
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
                    wait_tx.clone(),
                    wait_rc.clone(),
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
    wait_tx: Arc<Mutex<mpsc::Sender<u64>>>,
    wait_rc: Arc<Mutex<mpsc::Receiver<u64>>>,
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
                wait_tx.clone(),
            )
            .await?;
        }
        "wait" => {
            if is_replica {
                return Err(anyhow::anyhow!("WAIT command not expected for replicas"));
            }

            let args = RespData::unpack_array(&resp);
            let num_replicas_arg = args[1].serialize_to_list_of_strings(false)[0]
                .clone()
                .parse::<usize>()?;
            let wait_timeout_arg = args[2].serialize_to_list_of_strings(false)[0]
                .clone()
                .parse::<usize>()?;
            let timeout_instant = Instant::now() + Duration::from_millis(wait_timeout_arg as u64);

            let replica_addresses = {
                let server_info = server_info.lock().await;
                server_info.replica_addresses.clone()
            };

            let mut wait_rc_locked = wait_rc.lock().await;
            // Discard all messages currently in the broadcast channel
            while let Ok(_) = wait_rc_locked.try_recv() {
                continue;
            }

            let mut getack_bytes_to_add_to_master_offset = 0;
            let num_synced_replicas = if master_repl_offset == 0 {
                replica_addresses.len()
            } else {
                // send GETACK to all replicas and wait for them to acknowledge
                // or wait for a timeout:
                let mut synced_counter = 0;
                let getack_command = RespData::Array(vec![
                    RespData::BulkString("REPLCONF".to_string()),
                    RespData::BulkString("GETACK".to_string()),
                    RespData::BulkString("*".to_string()),
                ]);
                let bytes = getack_command
                    .serialize_to_redis_protocol()
                    .as_bytes()
                    .to_vec();
                let mut is_initial_iteration = true;
                while Instant::now() < timeout_instant {
                    // TODO: after sending the messages on this broadcast channel,
                    // listen to their responses and read their offsets
                    if synced_counter >= num_replicas_arg {
                        break;
                    }
                    if !is_initial_iteration {
                        // send the GETACK command to all replicas
                        sender.send(bytes.clone())?;
                    }
                    is_initial_iteration = false;
                    //                    for r_address in &replica_addresses {
                    //                        println!("connecting to replica");
                    //                        println!("replica address: {}", r_address);
                    //                        let mut replica_stream = TcpStream::connect(r_address).await?;
                    //                        println!("successfully connected to replica");
                    //                        let offset =
                    //                            send_getack_to_replica_and_read_offset(&mut replica_stream, &bytes)
                    //                                .await?;
                    //                        if offset >= master_repl_offset {
                    //                            synced_counter += 1;
                    //                        }
                    //                        if synced_counter >= num_replicas_arg {
                    //                            break;
                    //                        }
                    //                    }
                    //
                    getack_bytes_to_add_to_master_offset += bytes.len();
                    //println!("Before the sleep");
                    sleep(Duration::from_millis(50)).await;
                    //println!("After the sleep");
                    while let Ok(offset) = wait_rc_locked.try_recv() {
                        println!(
                            "Received offset from replica: {}, master repl offset is {}",
                            offset, master_repl_offset
                        );
                        if offset >= (master_repl_offset as u64) {
                            synced_counter += 1;
                        }
                        if synced_counter >= num_replicas_arg {
                            break;
                        }
                    }
                }
                synced_counter
            };
            println!("Number of synced replicas: {}", num_synced_replicas);
            let integer_resp = RespData::Integer(num_synced_replicas as isize);
            stream
                .write_all(integer_resp.serialize_to_redis_protocol().as_bytes())
                .await?;
            // increment the master_repl_offset by the number of bytes in the GETACK command
            //{
            //    let mut server_info = server_info.lock().await;
            //    server_info.master_repl_offset += getack_bytes_to_add_to_master_offset;
            //}
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

//async fn send_getack_to_replica_and_read_offset(
//    stream: &mut TcpStream,
//    bytes: &[u8],
//) -> Result<usize> {
//    stream.write_all(bytes).await?;
//    let mut buf = [0; 512];
//    let n = stream.read(&mut buf).await?;
//    let (resp, _) = parse_resp_data(&buf[..n])?;
//    let offset_resp = RespData::unpack_array(&resp);
//    let offset = offset_resp[2].serialize_to_list_of_strings(false)[0]
//        .clone()
//        .parse()?;
//    Ok(offset)
//}
async fn process_local_buffer(
    stream: &mut tokio::net::TcpStream,
    local_buffer: &mut Vec<u8>,
    storage: Arc<Mutex<HashMap<String, StoredValue>>>,
    server_info: Arc<Mutex<ServerInfo>>,
    sender: Arc<Sender<Vec<u8>>>,
    wait_tx: Arc<Mutex<mpsc::Sender<u64>>>,
    wait_rc: Arc<Mutex<mpsc::Receiver<u64>>>,
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
            wait_tx.clone(),
            wait_rc.clone(),
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
    wait_tx: Arc<Mutex<mpsc::Sender<u64>>>,
    wait_rc: Arc<Mutex<mpsc::Receiver<u64>>>,
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
    handle_connection(stream, storage, server_info, sender, wait_tx, wait_rc).await?;
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
        let mut server_info = server_info.lock().await;
        // increment master_replication_offset by the number of bytes in the message_bytes
        // I think there technically is supposed to be a system where the master pings replicas
        // to make sure they're alive and I suspect we're also supposed to increment the offset
        // in the case, but I don't think its necessary to implement.
        server_info.master_repl_offset += message_bytes.len();
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
    wait_tx: Arc<Mutex<mpsc::Sender<u64>>>,
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
        // get the address of the replica
        let addr = stream.peer_addr().unwrap().to_string();
        println!("Replica address: {}", addr);
        server_info.replica_addresses.push(addr);
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
        // check if its a getack command that's being propagated to replicas
        let (replica_command_resp, _num_bytes) = parse_resp_data(&replica_command)?;
        let replica_command_list = replica_command_resp.serialize_to_list_of_strings(true);
        let is_wait_command = replica_command_list.len() == 3
            && replica_command_list[0] == "replconf"
            && replica_command_list[1] == "getack";

        // When reading the results back from GETACK responses we clear the read buffer beforehand
        if is_wait_command {
            //println!("is wait command");
            //println!("Clearing read buffer from TCP stream");
            clear_read_buffer_from_tcp_stream(stream);
            //println!("Finished clearing read buffer from TCP stream");
        }
        // whenever a message is received from the broadcast channel, simply forward it to the replica
        stream.write_all(&replica_command).await?;
        // make sure messages are sent to the replica immediately
        stream.flush().await?;
        // for wait commands, read
        if is_wait_command {
            let mut bytes_read_vec = Vec::<u8>::new();
            let mut buf = [0; 512];
            //println!("reading reply for wait command");
            // this is dumb but basically going to sleep for a bit to give the replica time to respond
            sleep(Duration::from_millis(50)).await;
            loop {
                match stream.try_read(&mut buf) {
                    Ok(0) => {
                        //println!("Exited wait read loop gracefully");
                        break;
                    }
                    Ok(num_bytes) => {
                        // read the bytes into bytes_read_vec
                        bytes_read_vec.extend_from_slice(&buf[..num_bytes]);
                        break;
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        //println!("Would block");
                        break;
                    }
                    Err(e) => {
                        //eprintln!("Error reading from socket for wait command: {}", e);
                        break;
                    }
                }
            }
            //println!("finished reading reply for wait command");
            //println!("bytes read for wait command: {:?}", bytes_read_vec);
            // didn't receive a GETACK response from the replica so just continue
            if bytes_read_vec.is_empty() {
                println!("No bytes read for wait command");
                continue;
            }
            let (resp, _) = parse_resp_data(&bytes_read_vec)?;
            let offset_resp = RespData::unpack_array(&resp);
            // we expect to get REPLCONF ACK <offset> as a response
            if !(offset_resp.len() == 3
                && offset_resp[0] == RespData::BulkString("REPLCONF".to_string())
                && offset_resp[1] == RespData::BulkString("ACK".to_string()))
            {
                anyhow::bail!("Unexpected response from replica");
            }
            let offset = offset_resp[2].serialize_to_list_of_strings(false)[0]
                .clone()
                .parse::<usize>()?;
            // send the replica's offset back upstreawm to the thread thats handling the WAIT
            // command via the mpsc channel
            let u64_offset = offset.try_into().unwrap();
            let wait_tx_locked = wait_tx.lock().await;
            wait_tx_locked.send(u64_offset).await?;
            println!("Forwarded offset to WAIT command: {}", offset);
        }
        println!(
            "Forwarded command to replica: {:?}, is wait command {:?}",
            replica_command, is_wait_command
        );
    }
}

fn clear_read_buffer_from_tcp_stream(stream: &mut TcpStream) {
    let mut buf = [0; 512];
    loop {
        match stream.try_read(&mut buf) {
            Ok(0) => {
                break;
            }
            Ok(_) => {
                continue;
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                println!("Would block");
                break;
            }
            Err(e) => {
                eprintln!("Error reading from socket: {}", e);
                break;
            }
        }
    }
}
