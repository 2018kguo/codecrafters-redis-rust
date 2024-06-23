use anyhow::Result;
use commands::*;
use rdb_file::parse_rdb_file_at_path;
use serializer::{parse_resp_data, RespData};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use structs::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::{broadcast, Mutex};

mod commands;
mod rdb_file;
mod serializer;
mod structs;
mod utils;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let mut port_to_use: usize = 6379;
    let mut replica_info: Option<String> = None;
    let mut rdb_file_dir: Option<String> = None;
    let mut rdb_file_name: Option<String> = None;

    let port_flag_position = args.iter().position(|s| s == "--port");
    if let Some(pos) = port_flag_position {
        port_to_use = args[pos + 1].parse().unwrap();
    }
    let replica_flag_position = args.iter().position(|s| s == "--replicaof");
    if let Some(pos) = replica_flag_position {
        replica_info = Some(args[pos + 1].clone());
    }
    let dir_flag_position = args.iter().position(|s| s == "--dir");
    if let Some(pos) = dir_flag_position {
        rdb_file_dir = Some(args[pos + 1].clone());
    }
    let rdb_file_name_flag_position = args.iter().position(|s| s == "--dbfilename");
    if let Some(pos) = rdb_file_name_flag_position {
        rdb_file_name = Some(args[pos + 1].clone());
    }

    // generate 40 character long random string for the master_replid
    let master_replid: String = utils::get_random_string(40);
    let master_repl_offset = 0;

    let sock = format!("127.0.0.1:{}", port_to_use);

    // use an Arc<Mutex<HashMap>> to store the key-value pairs in memory across threads
    let storage: Arc<Mutex<HashMap<String, StoredValue>>> = Arc::new(Mutex::new(HashMap::new()));

    let mut rdb_file_key_value_mapping = None;
    if rdb_file_dir.is_some() && rdb_file_name.is_some() {
        let file_path = format!(
            "{}/{}",
            &rdb_file_dir.clone().unwrap(),
            &rdb_file_name.clone().unwrap()
        );
        let rdb_file_result = parse_rdb_file_at_path(&file_path);
        if let Err(e) = rdb_file_result {
            eprintln!("Error parsing RDB file: {}", e);
        } else {
            let mapping = rdb_file_result.unwrap().key_value_mapping;
            for (key, value_plus_expiry) in &mapping {
                let value = &value_plus_expiry.0;
                let expiry = value_plus_expiry.1;
                println!("Key: {}, Value: {}, expiry: {:?}", key, value, expiry);
                let stored_value = StoredValue {
                    value: Value::String(value.to_string()),
                    expiry,
                };
                storage.lock().await.insert(key.to_string(), stored_value);
            }
            // only take the string values from the mapping and ignore the expiry values
            rdb_file_key_value_mapping = Some(
                mapping
                    .iter()
                    .map(|(k, v)| (k.clone(), v.0.clone()))
                    .collect(),
            );
        }
    }
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
        rdb_file_dir,
        rdb_file_name,
        rdb_file_key_value_mapping,
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
                let mut message_channels = ServerMessageChannels {
                    sender: sender.clone(),
                    wait_tx: wait_tx.clone(),
                    wait_rc: wait_rc.clone(),
                };
                handle_connection(stream, storage, server_info, &mut message_channels)
                    .await
                    .unwrap();
            }
        });
        println!("Starting replica handshake");
        let replica_info = replica_info.unwrap();
        let replica_info_split: Vec<&str> = replica_info.split(' ').collect();
        let master_ip = replica_info_split[0];
        let master_port = replica_info_split[1];
        let mut message_channels = ServerMessageChannels {
            sender: sender_arc.clone(),
            wait_tx: wait_tx_arc.clone(),
            wait_rc: wait_rc_arc.clone(),
        };
        if let Err(e) = perform_replica_master_handshake(
            master_ip,
            master_port,
            port_to_use,
            storage.clone(),
            server_info.clone(),
            &mut message_channels,
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
                let mut message_channels = ServerMessageChannels {
                    sender: sender.clone(),
                    wait_tx: wait_tx.clone(),
                    wait_rc: wait_rc.clone(),
                };
                if let Err(e) =
                    handle_connection(stream, storage, server_info, &mut message_channels).await
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
    message_channels: &mut ServerMessageChannels,
) -> Result<()> {
    // Primer on sockets: https://docs.python.org/3/howto/sockets.html
    let mut buf = [0; 512];
    let mut local_buffer = Vec::new();
    // we shouldn't need to use any synchronization primitives here because thi
    // data only belongs to the current connection which is handled by a single green thread
    let mut transaction_data = TransactionData::new();
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
                    message_channels,
                    &mut transaction_data,
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
    local_buffer: &mut Vec<u8>,
    message_channels: &mut ServerMessageChannels,
    transaction_data: &mut TransactionData,
) -> Result<()> {
    let (
        role,
        master_replid,
        master_repl_offset,
        handshake_with_master_complete,
        num_command_bytes_processed_as_replica,
        rdb_file_dir,
        rdb_file_name,
        mapping,
    ) = {
        let server_info = server_info.lock().await;
        (
            server_info.role.clone(),
            server_info.master_replid.clone(),
            server_info.master_repl_offset,
            server_info.handshake_with_master_complete,
            server_info.num_command_bytes_processed_as_replica,
            server_info.rdb_file_dir.clone(),
            server_info.rdb_file_name.clone(),
            server_info.rdb_file_key_value_mapping.clone(),
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
            let resp_data_response = handle_set_command(
                storage.clone(),
                message_channels.sender.clone(),
                &message_bytes,
                resp,
                is_replica,
                handshake_with_master_complete,
                server_info.clone(),
                transaction_data,
            )
            .await?;
            if let Some(resp_response) = resp_data_response {
                let resp_bytes = resp_response.serialize_to_redis_protocol();
                stream.write_all(resp_bytes.as_bytes()).await?;
            }
        }
        "get" => {
            let resp_data_response =
                handle_get_command(storage.clone(), &resp, transaction_data).await?;
            if let Some(resp_response) = resp_data_response {
                let resp_bytes = resp_response.serialize_to_redis_protocol();
                stream.write_all(resp_bytes.as_bytes()).await?;
            }
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
                message_channels.sender.clone(),
                &resp,
                master_replid,
                master_repl_offset,
                server_info.clone(),
                message_channels.wait_tx.clone(),
            )
            .await?;
        }
        "wait" => {
            handle_wait_command(
                stream,
                &resp,
                server_info.clone(),
                message_channels,
                master_repl_offset,
                is_replica,
            )
            .await?;
        }
        "config" => {
            let config_resp = resp.serialize_to_list_of_strings(true);
            let subcommand = &config_resp[1];
            if subcommand != "get" {
                return Err(anyhow::anyhow!("Only the CONFIG GET command is supported"));
            }
            let get_arg = &config_resp[2];
            match get_arg.as_str() {
                "dir" => {
                    // respond with a RESP array of dir and the value
                    let dir_resp = RespData::Array(vec![
                        RespData::BulkString("dir".to_string()),
                        RespData::BulkString(rdb_file_dir.unwrap()),
                    ]);
                    stream
                        .write_all(dir_resp.serialize_to_redis_protocol().as_bytes())
                        .await?;
                }
                "dbfilename" => {
                    // respond with a RESP array of dbfilename and the value
                    let dbfilename_resp = RespData::Array(vec![
                        RespData::BulkString("dbfilename".to_string()),
                        RespData::BulkString(rdb_file_name.unwrap()),
                    ]);
                    stream
                        .write_all(dbfilename_resp.serialize_to_redis_protocol().as_bytes())
                        .await?;
                }
                _ => {
                    return Err(anyhow::anyhow!("Invalid argument for CONFIG GET"));
                }
            }
        }
        "keys" => {
            println!("dir: {:?}", rdb_file_dir);
            println!("name: {:?}", rdb_file_name);
            match mapping {
                Some(key_value_mapping) => {
                    let keys = key_value_mapping.keys();
                    println!("keys: {:?}", keys);
                    // response with a RESP array of all the keys encoded as bulk strings
                    let keys_resp = keys
                        .map(|key| RespData::BulkString(key.clone()))
                        .collect::<Vec<RespData>>();
                    let array_resp = RespData::Array(keys_resp);
                    stream
                        .write_all(array_resp.serialize_to_redis_protocol().as_bytes())
                        .await?;
                }
                None => {
                    // respond with empty array
                    let empty_array_resp = RespData::Array(Vec::new());
                    stream
                        .write_all(empty_array_resp.serialize_to_redis_protocol().as_bytes())
                        .await?;
                }
            }
        }
        "type" => {
            handle_type_command(stream, storage.clone(), &resp).await?;
        }
        "xadd" => handle_xadd_command(stream, storage.clone(), &resp).await?,
        "xrange" => {
            handle_xrange_command(stream, storage.clone(), &resp).await?;
        }
        "xread" => {
            handle_xread_command(stream, storage.clone(), &resp).await?;
        }
        "incr" => {
            let resp_data_response = handle_incr_command(
                storage.clone(),
                resp,
                &message_bytes,
                is_replica,
                transaction_data,
                server_info.clone(),
            )
            .await?;
            if let Some(resp_response) = resp_data_response {
                let resp_bytes = resp_response.serialize_to_redis_protocol();
                stream.write_all(resp_bytes.as_bytes()).await?;
            }
        }
        "multi" => {
            handle_multi_command(stream, transaction_data).await?;
        }
        "exec" => {
            handle_exec_command(
                stream,
                storage.clone(),
                message_channels.sender.clone(),
                server_info.clone(),
                transaction_data,
                is_replica,
                handshake_with_master_complete,
            )
            .await?;
        }
        _ => {
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
    message_channels: &mut ServerMessageChannels,
    transaction_data: &mut TransactionData,
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
            local_buffer,
            message_channels,
            transaction_data,
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
    message_channels: &mut ServerMessageChannels,
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
    handle_connection(stream, storage, server_info, message_channels).await?;
    Ok(())
}
